#!/usr/bin/env python3
"""Interactive Budget Dashboard - Streamlit app for personal finance tracking."""

import sqlite3
import hashlib
import csv
import io
import math
import re
import time
from datetime import datetime, timedelta, date
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
from icalendar import Calendar as iCalendar

DB_PATH = Path(__file__).parent / "budget.db"

from db import (
    get_conn, init_db, load_transactions, load_budgets,
    save_budget, load_achievements, unlock_achievement,
    get_setting, set_setting,
    load_goals, save_goal, update_goal_progress, delete_goal,
)

try:
    from deals import (
        search_offers as deals_search, get_cheapest as deals_cheapest,
        test_connection as deals_test, smart_search, record_search, record_prices,
        get_price_history, get_grocery_budget_status, get_frequent_searches,
        generate_smart_list, analyze_grocery_habits, set_staple, find_price_drops,
        add_to_shopping_list, remove_shopping_item, clear_all_shopping_data,
    )
    DEALS_AVAILABLE = True
except ImportError:
    DEALS_AVAILABLE = False

# ──────────────────────────── CSV import ────────────────────────────

def parse_danish_number(s):
    s = s.strip().strip('"')
    s = s.replace(".", "").replace(",", ".")
    return float(s)


def import_csv_data(file_bytes, conn):
    """Import CSV from bytes (uploaded file). Returns (new, skip)."""
    for enc in ["utf-8", "latin-1", "cp1252"]:
        try:
            text = file_bytes.decode(enc)
            if "Dato" in text and "Kategori" in text:
                break
        except UnicodeDecodeError:
            continue
    else:
        return 0, 0

    reader = csv.reader(io.StringIO(text), delimiter=";")
    header = next(reader)
    new_count = skip_count = 0
    seen_counts = {}
    for row in reader:
        if len(row) < 6:
            continue
        try:
            date_str = row[0].strip().strip('"')
            date = datetime.strptime(date_str, "%d.%m.%Y").strftime("%Y-%m-%d")
            category = row[1].strip().strip('"')
            subcategory = row[2].strip().strip('"')
            description = row[3].strip().strip('"')
            amount = parse_danish_number(row[4])
            balance = parse_danish_number(row[5])
            base_key = f"{date}|{amount}|{description}"
            seen_counts[base_key] = seen_counts.get(base_key, 0) + 1
            h = hashlib.sha256(f"{date}|{amount}|{description}#{seen_counts[base_key]}".encode()).hexdigest()
            conn.execute(
                "INSERT INTO transactions (date, category, subcategory, description, amount, balance, import_hash) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (date, category, subcategory, description, amount, balance, h),
            )
            new_count += 1
        except Exception:
            skip_count += 1
    conn.commit()
    return new_count, skip_count


# ──────────────────────────── Auto-Categorize ────────────────────────────

CATEGORY_RULES = [
    # (regex_pattern, target_category, target_subcategory)
    # Housing
    (r"P\.?O\.?\s*Pedersen|Kollegiet", "Bolig", "Husleje"),
    (r"Norlys", "Bolig", "Forsyning"),
    (r"SILVAN|Bauhaus|Harald Nyborg", "Bolig", "Vedligeholdelse"),
    # Transport
    (r"Rejsekort|DSB|MOB\.PAY\*DSB", "Transport", "Offentlig transport"),
    # Groceries (handles MobilePay prefix via desc_stripped matching)
    (r"Coop|REMA|F[Ø@]TEX|Føtex|Netto|Lidl|ALDI|Fakta|Meny|Irma|COOP365|Bilka|Spar\b", "Dagligvarer", "Supermarked"),
    (r"McDonalds|Burger King|Max Burgers|Sunset Boulevard", "Dagligvarer", "Fast food"),
    # Entertainment & dining
    (r"Wolt|Just.?Eat|Hungry", "Fornøjelser og fritid", "Takeaway"),
    (r"Kaffestuen|Starbucks|Espresso|Cafe|Café|Kaffeb.nnen", "Fornøjelser og fritid", "Café"),
    (r"Durumbar|Kebap|Shawarma|Pizza|Pakhus", "Fornøjelser og fritid", "Restaurant"),
    (r"WINTER|Bar |Bodega|Pub\b|S-Huset", "Fornøjelser og fritid", "Bar"),
    # Subscriptions & digital services
    (r"TIDAL|Spotify|Netflix|Disney|HBO|YouTube|Viaplay", "Øvrige udgifter", "Streaming"),
    (r"Microsoft|OPENAI|CLAUDE|ANTHROPIC|Google\s*\*?Google\s*One|Google\s*Play|"
     r"PADDLE\.NET|GOODNOTES|LMST.*Drift|BUDGETMATE", "Øvrige udgifter", "Abonnement"),
    # Health & personal care
    (r"Tandl.ge|Godt Smil", "Tøj, sko og personlig pleje", "Tandlæge"),
    (r"Carlsen|Barbershop|Frisør|frisør", "Tøj, sko og personlig pleje", "Frisør"),
    (r"Matas|Apotek|Normal\b|N.rrebro Apotek|Medictinedic", "Tøj, sko og personlig pleje", "Personlig pleje"),
    # Shopping
    (r"temashop|Brinck Elektronik|ESSENCEVAULT|Klarna|PRINT YOUR SPEAKERS", "Øvrige udgifter", "Shopping"),
    # Insurance & fees
    (r"forsikri|Tryg\b", "Personforsikringer", "Forsikring"),
    (r"Kortgebyr|Gebyrer", "Øvrige udgifter", "Gebyrer"),
    (r"^Rente$", "Anden gæld", "Renter"),
    # Donations
    (r"Kr.ftens Bek.mpelse|Red Barnet|UNICEF", "Øvrige udgifter", "Donation"),
    # Savings transfers (negative = moving to savings)
    (r"Til Opsparing|Fra Opsparing", "Opsparing", "Overførsel"),
    # Person-to-person MobilePay (catch-all for names — must be LAST)
    (r"^MobilePay\s+[A-ZÆØÅ][a-zæøå]+\s+[A-ZÆØÅ]", "Øvrige udgifter", "MobilePay overførsel"),
]


def auto_categorize(conn, dry_run=False):
    """Re-categorize 'Ukategoriseret' transactions using CATEGORY_RULES.

    Strips the 'MobilePay ' prefix before matching so rules work for both
    direct card purchases and MobilePay-wrapped versions.

    If dry_run=True, returns list of proposed changes without applying.
    Otherwise applies changes and returns count.
    """
    rows = conn.execute(
        "SELECT id, description FROM transactions WHERE category = 'Ukategoriseret'"
    ).fetchall()

    changes = []
    for row_id, desc in rows:
        # Try matching both the raw description and with MobilePay prefix stripped
        desc_stripped = re.sub(r"^MobilePay\s+", "", desc)
        matched = False
        for pattern, cat, subcat in CATEGORY_RULES:
            if re.search(pattern, desc, re.IGNORECASE) or re.search(pattern, desc_stripped, re.IGNORECASE):
                changes.append((row_id, desc, cat, subcat))
                matched = True
                break

    if dry_run:
        return changes

    for row_id, _desc, cat, subcat in changes:
        conn.execute(
            "UPDATE transactions SET category = ?, subcategory = ? WHERE id = ?",
            (cat, subcat, row_id),
        )
    if changes:
        conn.commit()
        load_transactions.clear()

    # Gas station detection (no car — never fuel)
    gas_rows = conn.execute(
        "SELECT id, description, amount FROM transactions "
        "WHERE amount < 0"
    ).fetchall()
    gas_count = 0
    for row_id, desc, amt in gas_rows:
        if re.search(r"Q8|Circle K|Shell|7-Eleven|Narvesen|kiosk|OK Plus",
                      desc, re.IGNORECASE):
            if abs(amt) >= 65:
                conn.execute(
                    "UPDATE transactions SET category = 'Snus', subcategory = 'Snus + kiosk' WHERE id = ?",
                    (row_id,),
                )
                gas_count += 1
            else:
                conn.execute(
                    "UPDATE transactions SET category = 'Dagligvarer', subcategory = 'Kiosk' WHERE id = ?",
                    (row_id,),
                )
                gas_count += 1
    if gas_count:
        conn.commit()
        load_transactions.clear()

    return len(changes) + snus_count


# ──────────────────────────── Gamification ────────────────────────────

LEVELS = {
    1: "Broke Student",
    2: "Budget Beginner",
    3: "Penny Pincher",
    4: "Savings Starter",
    5: "Cash Conscious",
    6: "Money Manager",
    7: "Budget Boss",
    8: "Savings Superstar",
    9: "Finance Guru",
    10: "Budget Master",
}

XP_PER_LEVEL = 500  # XP needed to advance one level


def compute_xp(df, budgets_df):
    """Compute XP from under-budget performance.

    Only counts months from February 2026 onward (when budget was set).
    XP is earned prospectively, not retroactively on historical data.
    """
    if df.empty or budgets_df.empty:
        return 0

    budget_map = dict(zip(budgets_df["category"], budgets_df["monthly_limit"]))
    if not budget_map or all(v == 0 for v in budget_map.values()):
        return 0

    # Only count from the month the budget was activated (Feb 2026)
    BUDGET_START = "2026-02"
    expenses = df[(df["amount"] < 0) & (df["month"] >= BUDGET_START)].copy()
    if expenses.empty:
        return 0

    xp = 0
    total_monthly_budget = sum(v for v in budget_map.values() if v > 0)
    daily_budget = total_monthly_budget / 30.0
    weekly_budget = daily_budget * 7

    # Monthly XP: +200 per month where ALL categories are under budget
    for month, grp in expenses.groupby("month"):
        month_spend = grp.groupby("category")["amount"].sum().abs()
        all_under = all(month_spend.get(cat, 0) <= limit for cat, limit in budget_map.items() if limit > 0)
        if all_under:
            xp += 200

    # Weekly XP: +50 per week under total weekly budget
    expenses_indexed = expenses.set_index("date")
    for week_start, grp in expenses_indexed.resample("W"):
        week_spend = abs(grp["amount"].sum())
        if week_spend > 0 and week_spend < weekly_budget:
            xp += 50

    # Daily XP: +10 per day under daily budget
    daily_spend = expenses.groupby(expenses["date"].dt.date)["amount"].sum().abs()
    for day, spend in daily_spend.items():
        if spend < daily_budget:
            xp += 10

    return xp


def get_level(xp):
    level = min(10, max(1, xp // XP_PER_LEVEL + 1))
    return int(level)


ACHIEVEMENT_DEFS = {
    "Coffee Quitter": {
        "icon": "\u2615",
        "desc": "0 KAFFESTUEN purchases in a week",
    },
    "Meal Prep Master": {
        "icon": "\U0001f373",
        "desc": "Fast food < 200 DKK in a month",
    },
    "First Surplus": {
        "icon": "\U0001f4b0",
        "desc": "First month with positive net",
    },
    "Streak x3": {
        "icon": "\U0001f525",
        "desc": "3 days consecutively under daily budget",
    },
    "Streak x7": {
        "icon": "\U0001f525",
        "desc": "7 days consecutively under daily budget",
    },
    "Streak x30": {
        "icon": "\U0001f525",
        "desc": "30 days consecutively under daily budget",
    },
    "Smart Shopper": {
        "icon": "\U0001f3ea",
        "desc": "Avg grocery bill < 100 DKK",
    },
    "Trend Breaker": {
        "icon": "\U0001f4c9",
        "desc": "Spending down 3 months in a row",
    },
    "On Target": {
        "icon": "\U0001f3af",
        "desc": "Hit all category budgets in a month",
    },
    "Snus Free Week": {
        "icon": "\U0001f6ad",
        "desc": "Zero detected snus purchases in a week",
    },
    "Snus Halved": {
        "icon": "\U0001f4aa",
        "desc": "Cut detected snus purchases by 50% vs first month",
    },
}


def check_achievements(df, budgets_df, conn):
    """Check and unlock any newly earned achievements."""
    if df.empty:
        return
    existing = set(load_achievements()["name"].tolist()) if not load_achievements().empty else set()
    budget_map = dict(zip(budgets_df["category"], budgets_df["monthly_limit"])) if not budgets_df.empty else {}

    # Coffee Quitter: any week with 0 KAFFESTUEN
    if "Coffee Quitter" not in existing:
        kaffe = df[df["description"].str.contains("KAFFESTUEN", case=False, na=False)]
        expenses = df[df["amount"] < 0].copy()
        if not expenses.empty:
            weeks_with_kaffe = set()
            for _, row in kaffe.iterrows():
                weeks_with_kaffe.add(row["date"].isocalendar()[1])
            all_weeks = set()
            for _, row in expenses.iterrows():
                all_weeks.add(row["date"].isocalendar()[1])
            if all_weeks - weeks_with_kaffe:
                unlock_achievement(conn, "Coffee Quitter", ACHIEVEMENT_DEFS["Coffee Quitter"]["desc"])

    # Meal Prep Master: any month with fast food < 200 DKK
    if "Meal Prep Master" not in existing:
        ff = df[(df["subcategory"].str.contains("Fast food", case=False, na=False)) & (df["amount"] < 0)]
        if not ff.empty:
            monthly_ff = ff.groupby("month")["amount"].sum().abs()
            if (monthly_ff < 200).any():
                unlock_achievement(conn, "Meal Prep Master", ACHIEVEMENT_DEFS["Meal Prep Master"]["desc"])
        else:
            # No fast food at all = qualifies
            if len(df["month"].unique()) > 0:
                unlock_achievement(conn, "Meal Prep Master", ACHIEVEMENT_DEFS["Meal Prep Master"]["desc"])

    # First Surplus: any month with positive net
    if "First Surplus" not in existing:
        monthly_net = df.groupby("month")["amount"].sum()
        if (monthly_net > 0).any():
            unlock_achievement(conn, "First Surplus", ACHIEVEMENT_DEFS["First Surplus"]["desc"])

    # Smart Shopper: avg grocery bill < 100
    if "Smart Shopper" not in existing:
        groceries = df[(df["subcategory"].str.contains("Supermarked", case=False, na=False)) & (df["amount"] < 0)]
        if not groceries.empty and groceries["amount"].abs().mean() < 100:
            unlock_achievement(conn, "Smart Shopper", ACHIEVEMENT_DEFS["Smart Shopper"]["desc"])

    # Trend Breaker: 3 consecutive months of decreasing spend
    if "Trend Breaker" not in existing:
        monthly_spend = df[df["amount"] < 0].groupby("month")["amount"].sum().abs().sort_index()
        if len(monthly_spend) >= 3:
            vals = monthly_spend.values
            for i in range(len(vals) - 2):
                if vals[i] > vals[i + 1] > vals[i + 2]:
                    unlock_achievement(conn, "Trend Breaker", ACHIEVEMENT_DEFS["Trend Breaker"]["desc"])
                    break

    # Streak achievements + On Target need budgets
    if budget_map:
        daily_budget = sum(budget_map.values()) / 30.0
        daily_spend = df[df["amount"] < 0].groupby(df["date"].dt.date)["amount"].sum().abs().sort_index()
        streak = 0
        max_streak = 0
        for spend in daily_spend.values:
            if spend < daily_budget:
                streak += 1
                max_streak = max(max_streak, streak)
            else:
                streak = 0
        if max_streak >= 3 and "Streak x3" not in existing:
            unlock_achievement(conn, "Streak x3", ACHIEVEMENT_DEFS["Streak x3"]["desc"])
        if max_streak >= 7 and "Streak x7" not in existing:
            unlock_achievement(conn, "Streak x7", ACHIEVEMENT_DEFS["Streak x7"]["desc"])
        if max_streak >= 30 and "Streak x30" not in existing:
            unlock_achievement(conn, "Streak x30", ACHIEVEMENT_DEFS["Streak x30"]["desc"])

        # On Target: all categories under budget for any month
        if "On Target" not in existing:
            for month, grp in df[df["amount"] < 0].groupby("month"):
                month_spend = grp.groupby("category")["amount"].sum().abs()
                all_under = True
                for cat, limit in budget_map.items():
                    if month_spend.get(cat, 0) > limit:
                        all_under = False
                        break
                if all_under:
                    unlock_achievement(conn, "On Target", ACHIEVEMENT_DEFS["On Target"]["desc"])
                    break


def load_css():
    css_path = Path(__file__).parent / "style.css"
    if css_path.exists():
        with open(css_path) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)


# ──────────────────────────── Google Calendar (ICS) ────────────────────────────

GCAL_CACHE_TTL = 900  # 15 minutes

def fetch_shifts(conn):
    """Fetch work shifts from a Google Calendar private ICS URL.

    Returns a DataFrame with columns:
        date, start, end, hours, normal_h, ot_h, gross, net, summary
    Returns empty DataFrame if URL not configured or fetch fails.
    """
    ics_url = get_setting(conn, "gcal_ics_url", "")
    keyword = get_setting(conn, "gcal_shift_keyword", "Working time")
    if not ics_url:
        return pd.DataFrame()

    # Check session_state cache
    cache_key = "gcal_shifts_cache"
    cache_ts_key = "gcal_shifts_ts"
    if (cache_key in st.session_state
            and cache_ts_key in st.session_state
            and time.time() - st.session_state[cache_ts_key] < GCAL_CACHE_TTL):
        return st.session_state[cache_key]

    try:
        resp = requests.get(ics_url, timeout=15)
        resp.raise_for_status()
    except Exception:
        # Return cached data if available, otherwise empty
        return st.session_state.get(cache_key, pd.DataFrame())

    # Parse ICS
    try:
        cal = iCalendar.from_ical(resp.content)
    except Exception:
        return st.session_state.get(cache_key, pd.DataFrame())

    # Read pay settings
    hourly_rate = float(get_setting(conn, "hourly_rate", "185"))
    shift_hours_setting = int(float(get_setting(conn, "shift_hours", "12")))
    ot_after = int(float(get_setting(conn, "ot_after", "8")))

    # Normalize keyword for matching (strip, collapse whitespace)
    keyword_norm = " ".join(keyword.split()).lower()

    rows = []
    for component in cal.walk():
        if component.name != "VEVENT":
            continue
        summary = str(component.get("summary", ""))
        summary_norm = " ".join(summary.split()).lower()
        if keyword_norm not in summary_norm:
            continue

        dtstart = component.get("dtstart")
        dtend = component.get("dtend")
        if dtstart is None:
            continue
        dt_start = dtstart.dt
        dt_end = dtend.dt if dtend else None

        # Handle all-day events (date instead of datetime)
        if isinstance(dt_start, date) and not isinstance(dt_start, datetime):
            # All-day event — assume default shift hours
            shift_date = dt_start
            start_time = None
            end_time = None
            hours = shift_hours_setting
        elif dt_end is not None:
            shift_date = dt_start.date() if isinstance(dt_start, datetime) else dt_start
            start_time = dt_start
            end_time = dt_end
            delta = dt_end - dt_start
            hours = delta.total_seconds() / 3600
        else:
            shift_date = dt_start.date() if isinstance(dt_start, datetime) else dt_start
            start_time = dt_start
            end_time = None
            hours = shift_hours_setting

        normal_h = min(hours, ot_after)
        ot_h = max(0, hours - ot_after)
        gross = normal_h * hourly_rate + ot_h * (hourly_rate * 1.5)
        am = gross * 0.08
        tax = (gross - am) * 0.37
        net = gross - am - tax

        rows.append({
            "date": shift_date,
            "start": start_time,
            "end": end_time,
            "hours": round(hours, 1),
            "normal_h": round(normal_h, 1),
            "ot_h": round(ot_h, 1),
            "gross": round(gross),
            "net": round(net),
            "summary": summary,
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)

    # Cache
    st.session_state[cache_key] = df
    st.session_state[cache_ts_key] = time.time()

    return df


# ── Streamlit App ──

def main():
    st.set_page_config(page_title="Finances", page_icon="💸", layout="wide")
    load_css()

    conn = get_conn()
    conn.execute("PRAGMA journal_mode=WAL") # Better concurrency
    init_db(conn)

    # ── Sidebar ──
    # Removed generic title, using clean nav below

    # ── Sidebar ──
    with st.sidebar:
        st.markdown("## Navigation")
        page = st.radio("Go to",
            ["Overview", "Analytics", "Income & Loan", "Snus Tracker", "Deals", "AI Advisor", "Achievements", "Settings"],
            label_visibility="collapsed"
        )
        
        st.markdown("---")
        
        # Bank Sync
        with st.expander("🏦 Danske Bank Sync"):
            if st.button("Sync Now", use_container_width=True):
                try:
                    with st.spinner("Syncing with Danske Bank..."):
                        from bank_sync import sync as bank_sync_run
                        new_count = bank_sync_run()
                    if new_count > 0:
                        recat = auto_categorize(conn)
                        msg = f"Synced {new_count} new transaction(s)"
                        if recat > 0:
                            msg += f", auto-categorized {recat}"
                        st.success(msg)
                        set_setting(conn, "last_bank_sync", datetime.now().isoformat())
                        st.rerun()
                    else:
                        st.info("Already up to date — no new transactions.")
                except RuntimeError as e:
                    st.error(str(e))
                except Exception as e:
                    st.error(f"Sync failed: {e}")

            last_sync = get_setting(conn, "last_bank_sync")
            if last_sync:
                try:
                    ts = datetime.fromisoformat(last_sync)
                    st.caption(f"Last sync: {ts.strftime('%d %b %Y %H:%M')}")
                except ValueError:
                    pass

            st.markdown("---")
            st.caption("Import bank CSV (replaces all data with bank's real balances)")
            csv_file = st.file_uploader("Upload Danske Bank CSV", type=["csv"], key="bank_csv_upload")
            if csv_file is not None:
                if st.button("Import CSV", type="primary", use_container_width=True):
                    try:
                        file_bytes = csv_file.getvalue()
                        # Clear existing transactions before importing
                        conn.execute("DELETE FROM transactions")
                        conn.commit()
                        new_count, skip_count = import_csv_data(file_bytes, conn)
                        if new_count > 0:
                            recat = auto_categorize(conn)
                            msg = f"Imported {new_count} transactions from CSV"
                            if skip_count > 0:
                                msg += f" ({skip_count} skipped)"
                            if recat > 0:
                                msg += f", auto-categorized {recat}"
                            st.success(msg)
                            load_transactions.clear()
                            st.rerun()
                        else:
                            st.warning("No transactions found in CSV.")
                    except Exception as e:
                        st.error(f"CSV import failed: {e}")

        # Quick-add manual transaction
        with st.expander("➕ Quick Add"):
            with st.form("quick_add_tx", clear_on_submit=True):
                qa_date = st.date_input("Date", value=date.today(), key="qa_date")
                qa_desc = st.text_input("Description", key="qa_desc", placeholder="e.g. Coffee at Kaffestuen")
                qa_amount = st.number_input("Amount (DKK)", step=10.0, key="qa_amount",
                                            help="Negative = expense, positive = income")
                qa_cat = st.text_input("Category", value="Ukategoriseret", key="qa_cat")
                qa_submit = st.form_submit_button("Add Transaction")
                if qa_submit and qa_desc and qa_amount != 0:
                    qa_date_str = qa_date.strftime("%Y-%m-%d")
                    qa_hash = hashlib.sha256(f"{qa_date_str}|{qa_amount}|{qa_desc}#manual".encode()).hexdigest()
                    try:
                        conn.execute(
                            "INSERT INTO transactions (date, category, subcategory, description, amount, balance, import_hash) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (qa_date_str, qa_cat, "", qa_desc, qa_amount, 0, qa_hash),
                        )
                        conn.commit()
                        load_transactions.clear()
                        st.success(f"Added: {qa_desc} ({qa_amount:+,.0f} DKK)")
                        st.rerun()
                    except sqlite3.IntegrityError:
                        st.warning("Transaction already exists (duplicate hash).")

        # Load data
        df = load_transactions()
        budgets_df = load_budgets()

        if df.empty:
            st.warning("No transactions found. Click 🏦 Danske Bank Sync in the sidebar, or use Quick Add.")
            return

        # Date filter
        st.markdown("### 📅 Date Filter")
        all_months = sorted(df["month"].unique())
        if not all_months:
            all_months = [datetime.now().strftime("%Y-%m")]

        month_range = st.select_slider(
            "Select range",
            options=all_months,
            value=(all_months[0], all_months[-1]),
            label_visibility="collapsed"
        )
        mask = (df["month"] >= month_range[0]) & (df["month"] <= month_range[1])
        df_filtered = df[mask].copy()

    # Load shared data needed for pages
    achievements_df = load_achievements()
    budget_map = dict(zip(budgets_df["category"], budgets_df["monthly_limit"])) if not budgets_df.empty else {}
    
    # Check achievements on load
    check_achievements(df, budgets_df, conn)

    # ── Page Routing ──
    if page == "Overview":
        render_overview(df_filtered, budgets_df, budget_map, conn)
    elif page == "Analytics":
        render_analytics(df_filtered, budgets_df, budget_map)
    elif page == "Income & Loan":
        render_income(df_filtered, conn)
    elif page == "Snus Tracker":
        render_snus_tracker(df_filtered, conn)
    elif page == "Deals":
        render_deals(conn, df_filtered, budgets_df)
    elif page == "AI Advisor":
        render_ai_insights(df_filtered, budgets_df, conn)
    elif page == "Achievements":
        render_achievements(df_filtered, achievements_df, budgets_df, budget_map, conn)
    elif page == "Settings":
        render_settings(df, budgets_df, conn)






SU_LOAN_MAX = 4000  # SU loan payments are below this threshold (max ~3,700 DKK + adjustments)


def classify_income(df):
    """Split income transactions into SU Grant, SU Loan, and Other.

    SU loan payments are always <= 3,700 DKK (government max).
    Anything labelled SU above that threshold is the grant.
    """
    income = df[df["amount"] > 0].copy()
    su_all = income[income["subcategory"].str.contains("SU", case=False, na=False)].copy()
    other = income[~income["subcategory"].str.contains("SU", case=False, na=False)].copy()

    su_grant = su_all[su_all["amount"] > SU_LOAN_MAX].copy()
    su_loan = su_all[su_all["amount"] <= SU_LOAN_MAX].copy()

    return su_grant, su_loan, other


# ──────────────────────────── Income & SU Loan ────────────────────────────

def render_income(df, conn):
    st.header("Income & SU Loan")

    if df.empty:
        return

    su_grant, su_loan, other_income = classify_income(df)
    n_months = max(df["month"].nunique(), 1)
    months_sorted = sorted(df["month"].unique())

    # Create Tabs
    tab_overview, tab_loan, tab_shifts, tab_tax = st.tabs(["💰 Income Overview", "🎓 SU Loan Planner", "🏨 Shift Planner", "🧾 Tax (Restskat)"])

    with tab_overview:
        # ── KPI row ──
        grant_total = su_grant["amount"].sum() if not su_grant.empty else 0
        loan_total = su_loan["amount"].sum() if not su_loan.empty else 0
        other_total = other_income["amount"].sum() if not other_income.empty else 0
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("SU Grant", f"{grant_total:,.0f} DKK", f"{grant_total/n_months:,.0f}/mo")
        c2.metric("SU Loan", f"{loan_total:,.0f} DKK", f"{loan_total/n_months:,.0f}/mo")
        c3.metric("Other Income", f"{other_total:,.0f} DKK", f"{other_total/n_months:,.0f}/mo")
        c4.metric("Total Income", f"{grant_total + loan_total + other_total:,.0f} DKK", 
                  f"{(grant_total + loan_total + other_total)/n_months:,.0f}/mo")

        st.markdown("---")

        # ── Monthly income breakdown stacked bar ──
        st.subheader("Monthly Breakdown")
        monthly_data = pd.DataFrame({"month": months_sorted})
        monthly_data["label"] = pd.to_datetime(monthly_data["month"]).dt.strftime("%b %y")

        grant_by_month = su_grant.groupby("month")["amount"].sum() if not su_grant.empty else pd.Series(dtype=float)
        loan_by_month = su_loan.groupby("month")["amount"].sum() if not su_loan.empty else pd.Series(dtype=float)
        other_by_month = other_income.groupby("month")["amount"].sum() if not other_income.empty else pd.Series(dtype=float)

        monthly_data["SU Grant"] = monthly_data["month"].map(grant_by_month).fillna(0)
        monthly_data["SU Loan"] = monthly_data["month"].map(loan_by_month).fillna(0)
        monthly_data["Other"] = monthly_data["month"].map(other_by_month).fillna(0)

        fig = go.Figure()
        fig.add_trace(go.Bar(x=monthly_data["label"], y=monthly_data["SU Grant"],
                             name="SU Grant", marker_color="#2ecc71",
                             text=monthly_data["SU Grant"].apply(lambda v: f"{v:,.0f}" if v > 0 else ""),
                             textposition="inside", textfont_size=11))
        fig.add_trace(go.Bar(x=monthly_data["label"], y=monthly_data["SU Loan"],
                             name="SU Loan", marker_color="#e67e22",
                             text=monthly_data["SU Loan"].apply(lambda v: f"{v:,.0f}" if v > 0 else ""),
                             textposition="inside", textfont_size=11))
        fig.add_trace(go.Bar(x=monthly_data["label"], y=monthly_data["Other"],
                             name="Other", marker_color="#3498db",
                             text=monthly_data["Other"].apply(lambda v: f"{v:,.0f}" if v > 0 else ""),
                             textposition="inside", textfont_size=11))
        fig.update_layout(barmode="stack", height=400,
                          yaxis_title="DKK", font=dict(size=13),
                          margin=dict(t=30, b=40, l=60, r=20),
                          legend=dict(orientation="h", y=1.08, x=0.5, xanchor="center"),
                          paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)

        # Other Income Sources
        if not other_income.empty:
            st.markdown("---")
            st.subheader("Other Income Sources")
            other_income_copy = other_income.copy()
            def classify_other(desc):
                desc_u = desc.upper()
                if "MOBILEPAY" in desc_u: return "MobilePay"
                elif "SKAT" in desc_u: return "Tax Refund"
                elif any(w in desc_u for w in ["FERIEPENGE", "FERIE"]): return "Holiday Pay"
                elif any(w in desc_u for w in ["RENTE"]): return "Interest"
                elif any(w in desc_u for w in ["LØN", "L\xd8N"]): return "Salary"
                else: return "Other"
            other_income_copy["source"] = other_income_copy["description"].apply(classify_other)
            source_totals = other_income_copy.groupby("source")["amount"].agg(["sum", "count"]).reset_index()
            source_totals.columns = ["Source", "Total (DKK)", "Transactions"]
            source_totals = source_totals.sort_values("Total (DKK)", ascending=True)

            fig4 = px.bar(source_totals, y="Source", x="Total (DKK)", orientation="h",
                          text="Total (DKK)", color="Total (DKK)", color_continuous_scale="Greens")
            fig4.update_traces(texttemplate="%{text:,.0f} DKK", textposition="outside", textfont_size=11)
            fig4.update_layout(height=max(250, len(source_totals) * 45),
                               margin=dict(t=10, b=20, l=20, r=80),
                               font=dict(size=13), coloraxis_showscale=False,
                               paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig4, use_container_width=True)

    with tab_loan:
        st.header("Projected Debt & Repayment")
        
        # ── Config ──
        with st.expander("⚙️ Loan Settings", expanded=True):
            STUDY_RATE_DEFAULT = 4.0
            DISKONTO = 1.60
            GRAD_RATE_DEFAULT = DISKONTO + 1.0

            col_l, col_r = st.columns(2)
            with col_l:
                st.subheader("Interest Rates")
                current_interest = float(get_setting(conn, "su_loan_interest", str(STUDY_RATE_DEFAULT)))
                interest_rate = st.number_input(
                    "Study Phase (% p.a.)",
                    0.0, 15.0, value=current_interest, step=0.1, key="loan_interest",
                    help=f"Default: {STUDY_RATE_DEFAULT}%"
                )
                if interest_rate != current_interest:
                    set_setting(conn, "su_loan_interest", interest_rate)

                grad_interest_default = float(get_setting(conn, "su_loan_grad_interest", str(GRAD_RATE_DEFAULT)))
                grad_interest = st.number_input(
                    "Post-Grad Phase (% p.a.)",
                    0.0, 15.0, value=grad_interest_default, step=0.1, key="grad_interest",
                    help=f"Default: {GRAD_RATE_DEFAULT}% (Diskonto + 1%)"
                )
                if grad_interest != grad_interest_default:
                    set_setting(conn, "su_loan_grad_interest", grad_interest)

            with col_r:
                st.subheader("Timeline & Repayment")
                semesters_left_default = int(float(get_setting(conn, "semesters_left", "3")))
                semesters_left = st.number_input(
                    "Semesters remaining", 0, 10, value=semesters_left_default, step=1, key="sem_left"
                )
                if semesters_left != semesters_left_default:
                    set_setting(conn, "semesters_left", semesters_left)
                
                monthly_loan_default = float(get_setting(conn, "su_loan_monthly", "3625"))
                monthly_loan_choice = st.slider(
                    "Planned monthly loan (DKK)", 0, 3700, value=int(monthly_loan_default), step=100, key="loan_monthly"
                )
                if monthly_loan_choice != int(monthly_loan_default):
                    set_setting(conn, "su_loan_monthly", monthly_loan_choice)
                
                repay_years_default = int(float(get_setting(conn, "repay_years", "15")))
                repayment_years = st.slider(
                    "Repayment period (years)", 7, 15, value=repay_years_default, key="repay_years"
                )
                if repayment_years != repay_years_default:
                    set_setting(conn, "repay_years", repayment_years)

        # ── Calculations ──
        # Existing actual loan
        actual_borrowed = loan_total
        # Recalculate accrued interest history?
        # ... (code for loan_df reused)
        study_monthly_rate_hist = interest_rate / 100 / 12
        cumulative = 0.0
        total_interest = 0.0
        for m in months_sorted:
            new_this_month = loan_by_month.get(m, 0)
            interest_this_month = cumulative * study_monthly_rate_hist
            total_interest += interest_this_month
            cumulative += interest_this_month + new_this_month
        
        # Projections
        months_left = semesters_left * 6
        study_monthly_rate = interest_rate / 100 / 12
        grad_monthly_rate = grad_interest / 100 / 12
        grace_period_months = 12

        # Projection logic ...
        projected_balance = cumulative
        projection_rows = [{"month": 0, "label": "Now", "balance": projected_balance, "phase": "Studying"}]
        for i in range(1, months_left + 1):
            interest_i = projected_balance * study_monthly_rate
            projected_balance += interest_i + monthly_loan_choice
            projection_rows.append({"month": i, "label": f"+{i}mo", "balance": projected_balance, "phase": "Studying"})
        
        debt_at_graduation = projected_balance
        
        for i in range(1, grace_period_months + 1):
            interest_i = projected_balance * grad_monthly_rate
            projected_balance += interest_i
            projection_rows.append({"month": months_left + i, "label": f"+{months_left + i}mo", "balance": projected_balance, "phase": "Grace period"})
        
        debt_after_grace = projected_balance
        repayment_months = repayment_years * 12
        
        # Annuity
        if grad_monthly_rate > 0 and debt_after_grace > 0:
            annuity_payment = debt_after_grace * grad_monthly_rate / (1 - (1 + grad_monthly_rate) ** -repayment_months)
        elif debt_after_grace > 0:
            annuity_payment = debt_after_grace / repayment_months
        else:
            annuity_payment = 0
            
        monthly_payment = annuity_payment
        
        bal = debt_after_grace
        repay_start = months_left + grace_period_months
        actual_repay_months = 0
        total_cost = 0
        
        for i in range(1, repayment_months + 1):
            interest_i = bal * grad_monthly_rate
            bal = bal + interest_i - monthly_payment
            actual_repay_months = i
            if bal <= 0:
                projection_rows.append({"month": repay_start + i, "label": f"+{repay_start + i}mo", "balance": 0, "phase": "Repaying"})
                bal = 0
                break
            if i % 6 == 0 or i == 1:
                projection_rows.append({"month": repay_start + i, "label": f"+{repay_start + i}mo", "balance": bal, "phase": "Repaying"})
        
        proj_df = pd.DataFrame(projection_rows)
        total_cost = monthly_payment * actual_repay_months
        interest_cost = total_cost - debt_after_grace if debt_after_grace > 0 else 0

        # KPI Cards
        st.markdown("### Projection Results")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Debt at Graduation", f"{debt_at_graduation:,.0f} DKK")
        c2.metric("Monthly Repayment", f"{monthly_payment:,.0f} DKK", f"for {repayment_years} years")
        c3.metric("Total Interest Cost", f"{interest_cost:,.0f} DKK")
        c4.metric("Total Repaid", f"{total_cost:,.0f} DKK")
        
        # Chart
        fig3 = go.Figure()
        for phase, color, fill in [
            ("Studying", "#e67e22", "rgba(230,126,34,0.15)"),
            ("Grace period", "#f39c12", "rgba(243,156,18,0.10)"),
            ("Repaying", "#2ecc71", "rgba(46,204,113,0.15)"),
        ]:
            phase_df = proj_df[proj_df["phase"] == phase]
            if phase_df.empty: continue
            # connect to prev
            idx = phase_df.index[0]
            if idx > 0:
                phase_df = pd.concat([proj_df.iloc[idx - 1: idx], phase_df])
            fig3.add_trace(go.Scatter(
                x=phase_df["month"], y=phase_df["balance"],
                name=phase, fill="tozeroy",
                line=dict(color=color, width=3), fillcolor=fill,
            ))
            
        fig3.update_layout(
            height=400, font=dict(size=13),
            xaxis_title="Months from now",
            yaxis_title="Loan balance (DKK)",
            margin=dict(t=30, b=40, l=60, r=20),
            legend=dict(orientation="h", y=1.08, x=0.5, xanchor="center"),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)"
        )
        st.plotly_chart(fig3, use_container_width=True)

        st.info("💡 **Tip:** Paying off interest during study/grace period prevents compound interest from spiraling.")
        
        # What If Table
        st.subheader("What If? - Monthly Loan Amount impact")
        st.caption("How much borrowing more/less affects your final bill")
        
        comparison_rows = []
        for loan_amt in [0, 1000, 2000, 3000, 3700]:
            # Simple calc for table
            bal_c = cumulative
            for _ in range(months_left): bal_c = bal_c * (1 + study_monthly_rate) + loan_amt
            grad_debt = bal_c
            for _ in range(grace_period_months): bal_c *= (1 + grad_monthly_rate)
            if grad_monthly_rate > 0 and bal_c > 0:
                pmt = bal_c * grad_monthly_rate / (1 - (1 + grad_monthly_rate) ** -repayment_months)
            elif bal_c > 0: pmt = bal_c / repayment_months
            else: pmt = 0
            total_paid = pmt * repayment_months
            comparison_rows.append({
                "Monthly Loan": f"{loan_amt:,} DKK",
                "Debt at Grad": f"{grad_debt:,.0f}",
                "Monthly Repayment": f"{pmt:,.0f}",
                "Total Repaid": f"{total_paid:,.0f}",
                "Interest Cost": f"{total_paid - grad_debt:,.0f}",
            })
        st.table(pd.DataFrame(comparison_rows))

    with tab_shifts:
        st.header("Hotel D'Angleterre Shift Planner")

        # ── Pay settings ──
        col_j1, col_j2, col_j3 = st.columns(3)
        with col_j1:
            hourly_rate = float(get_setting(conn, "hourly_rate", "185"))
            hourly_rate = st.number_input("Hourly rate", 0.0, value=hourly_rate, step=5.0, key="hourly_rate")
            set_setting(conn, "hourly_rate", hourly_rate)
        with col_j2:
            shift_hours = int(float(get_setting(conn, "shift_hours", "12")))
            shift_hours = st.number_input("Hours/Shift", 1, 24, value=shift_hours, step=1, key="shift_hours")
            set_setting(conn, "shift_hours", shift_hours)
        with col_j3:
            ot_after = int(float(get_setting(conn, "ot_after", "8")))
            ot_after = st.number_input("OT after (hours)", 1, 24, value=ot_after, step=1, key="ot_after")
            set_setting(conn, "ot_after", ot_after)

        overtime_rate = hourly_rate * 1.5
        normal_hours = min(shift_hours, ot_after)
        ot_hours = max(0, shift_hours - ot_after)
        gross_per_shift = normal_hours * hourly_rate + ot_hours * overtime_rate
        am = gross_per_shift * 0.08
        tax = (gross_per_shift - am) * 0.37
        net_per_shift = gross_per_shift - am - tax

        st.success(f"**Net per shift: ~{net_per_shift:,.0f} DKK** (est with 37% tax)")

        # ── Google Calendar Shifts ──
        shifts_df = fetch_shifts(conn)
        now = datetime.now()
        current_month_str = now.strftime("%Y-%m")

        if not shifts_df.empty:
            st.markdown("---")
            st.subheader("Shifts from Google Calendar")

            past_shifts = shifts_df[shifts_df["date"].dt.date < now.date()]
            month_shifts = shifts_df[shifts_df["date"].dt.strftime("%Y-%m") == current_month_str]
            upcoming = shifts_df[shifts_df["date"].dt.date >= now.date()].head(7)

            # ── KPIs: This month ──
            mc1, mc2, mc3, mc4 = st.columns(4)
            mc1.metric("This Month", f"{len(month_shifts)} shifts")
            mc2.metric("Total Hours", f"{month_shifts['hours'].sum():.0f} h")
            mc3.metric("Expected Gross", f"{month_shifts['gross'].sum():,.0f} DKK")
            mc4.metric("Expected Net", f"{month_shifts['net'].sum():,.0f} DKK")

            # ── Upcoming shifts table ──
            if not upcoming.empty:
                st.markdown("**Upcoming shifts (next 7)**")
                display_upcoming = upcoming.copy()
                display_upcoming["Date"] = display_upcoming["date"].dt.strftime("%a %d %b")
                display_upcoming["Time"] = display_upcoming.apply(
                    lambda r: (
                        f"{r['start'].strftime('%H:%M')}-{r['end'].strftime('%H:%M')}"
                        if pd.notna(r.get("start")) and r["start"] is not None
                        and pd.notna(r.get("end")) and r["end"] is not None
                        else f"{r['hours']:.0f}h (all-day)"
                    ), axis=1
                )
                display_upcoming["Hours"] = display_upcoming["hours"]
                display_upcoming["OT"] = display_upcoming["ot_h"].apply(lambda v: f"{v:.1f}h" if v > 0 else "-")
                display_upcoming["Est. Net"] = display_upcoming["net"].apply(lambda v: f"{v:,.0f} DKK")
                st.dataframe(
                    display_upcoming[["Date", "Time", "Hours", "OT", "Est. Net"]],
                    use_container_width=True, hide_index=True,
                )

            # ── Past shifts history ──
            if not past_shifts.empty:
                st.markdown("---")
                st.subheader("Shift History")

                hc1, hc2, hc3, hc4 = st.columns(4)
                hc1.metric("Total Past Shifts", f"{len(past_shifts)}")
                hc2.metric("Total Hours Worked", f"{past_shifts['hours'].sum():.0f} h")
                hc3.metric("Est. Total Gross", f"{past_shifts['gross'].sum():,.0f} DKK")
                hc4.metric("Est. Total Net", f"{past_shifts['net'].sum():,.0f} DKK")

                with st.expander("All past shifts", expanded=False):
                    display_past = past_shifts.sort_values("date", ascending=False).copy()
                    display_past["Date"] = display_past["date"].dt.strftime("%a %d %b %Y")
                    display_past["Time"] = display_past.apply(
                        lambda r: (
                            f"{r['start'].strftime('%H:%M')}-{r['end'].strftime('%H:%M')}"
                            if pd.notna(r.get("start")) and r["start"] is not None
                            and pd.notna(r.get("end")) and r["end"] is not None
                            else f"{r['hours']:.0f}h (all-day)"
                        ), axis=1
                    )
                    display_past["Hours"] = display_past["hours"]
                    display_past["OT"] = display_past["ot_h"].apply(lambda v: f"{v:.1f}h" if v > 0 else "-")
                    display_past["Est. Net"] = display_past["net"].apply(lambda v: f"{v:,.0f} DKK")
                    st.dataframe(
                        display_past[["Date", "Time", "Hours", "OT", "Est. Net"]],
                        use_container_width=True, hide_index=True,
                    )

            # ── Monthly chart (all time: past + future) ──
            if len(shifts_df) > 0:
                st.markdown("---")
                st.subheader("Monthly Shift Overview")

                shifts_by_month = shifts_df.copy()
                shifts_by_month["month"] = shifts_by_month["date"].dt.to_period("M").astype(str)
                monthly_agg = shifts_by_month.groupby("month").agg(
                    shifts=("date", "count"),
                    hours=("hours", "sum"),
                    gross=("gross", "sum"),
                    net=("net", "sum"),
                ).reset_index()
                monthly_agg["label"] = pd.to_datetime(monthly_agg["month"]).dt.strftime("%b %y")
                monthly_agg["is_past"] = monthly_agg["month"] < current_month_str

                fig_cal = go.Figure()
                # Past months in a muted color, current/future in green
                fig_cal.add_trace(go.Bar(
                    x=monthly_agg["label"], y=monthly_agg["net"],
                    name="Est. Net Pay",
                    marker_color=[
                        "#3498db" if past else "#2ecc71"
                        for past in monthly_agg["is_past"]
                    ],
                    text=monthly_agg["net"].apply(lambda v: f"{v:,.0f}"),
                    textposition="outside", textfont_size=11,
                ))
                fig_cal.add_trace(go.Scatter(
                    x=monthly_agg["label"],
                    y=monthly_agg["shifts"] * (monthly_agg["net"].max() / monthly_agg["shifts"].max()) if monthly_agg["shifts"].max() > 0 else monthly_agg["shifts"],
                    name="Shift count", yaxis="y2",
                    mode="lines+markers+text",
                    line=dict(color="#e67e22", width=2),
                    text=monthly_agg["shifts"].apply(lambda v: f"{v}x"),
                    textposition="top center", textfont_size=11,
                ))
                fig_cal.update_layout(
                    height=380, yaxis_title="DKK", font=dict(size=13),
                    margin=dict(t=30, b=40, l=60, r=20),
                    legend=dict(orientation="h", y=1.08, x=0.5, xanchor="center"),
                    yaxis2=dict(overlaying="y", side="right", showgrid=False, showticklabels=False),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                )
                st.caption("Blue = past months, Green = current/future")
                st.plotly_chart(fig_cal, use_container_width=True)

                # ── Calendar vs Bank comparison ──
                salary_txs = df[df["description"].str.contains("noverf", case=False, na=False) & (df["amount"] > 0)]
                if not salary_txs.empty and not past_shifts.empty:
                    st.markdown("---")
                    st.subheader("Calendar vs Actual Salary")
                    st.caption("Compare estimated pay from calendar shifts against actual salary deposits from bank")

                    sal_monthly = salary_txs.groupby("month")["amount"].sum()
                    past_by_month = shifts_by_month[shifts_by_month["is_past"]].copy() if "is_past" in shifts_by_month.columns else shifts_by_month[shifts_by_month["month"] < current_month_str]
                    cal_monthly = past_by_month.groupby("month")["net"].sum()

                    compare_months = sorted(set(sal_monthly.index) | set(cal_monthly.index))
                    compare_df = pd.DataFrame({"month": compare_months})
                    compare_df["Calendar Est."] = compare_df["month"].map(cal_monthly).fillna(0)
                    compare_df["Bank Salary"] = compare_df["month"].map(sal_monthly).fillna(0)
                    compare_df["label"] = pd.to_datetime(compare_df["month"]).dt.strftime("%b %y")

                    fig_cmp = go.Figure()
                    fig_cmp.add_trace(go.Bar(
                        x=compare_df["label"], y=compare_df["Calendar Est."],
                        name="Calendar Est. Net", marker_color="#3498db",
                    ))
                    fig_cmp.add_trace(go.Bar(
                        x=compare_df["label"], y=compare_df["Bank Salary"],
                        name="Actual Salary (bank)", marker_color="#2ecc71",
                    ))
                    fig_cmp.update_layout(
                        barmode="group", height=350, yaxis_title="DKK", font=dict(size=13),
                        margin=dict(t=30, b=40, l=60, r=20),
                        legend=dict(orientation="h", y=1.08, x=0.5, xanchor="center"),
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    )
                    st.plotly_chart(fig_cmp, use_container_width=True)

        else:
            ics_url = get_setting(conn, "gcal_ics_url", "")
            if not ics_url:
                st.info("Connect your Google Calendar to auto-import shifts. Set it up in **Settings**.")
            else:
                keyword = get_setting(conn, "gcal_shift_keyword", "Working time")
                st.warning(f"No shifts found matching \"{keyword}\". Check your calendar or keyword in Settings.")

        # ── Manual what-if planner ──
        st.markdown("---")
        st.subheader("What-If Planner")
        planned_shifts = st.slider("Shifts this month", 0, 15, value=3, key="planned_shifts_tab")

        net_month = planned_shifts * net_per_shift
        st.metric("Estimated Income", f"{net_month:,.0f} DKK", f"{planned_shifts * shift_hours} hours")

        # Salary history chart
        st.markdown("---")
        st.subheader("Salary History (from bank)")
        salary_txs = df[df["description"].str.contains("noverf", case=False, na=False) & (df["amount"] > 0)]
        if not salary_txs.empty:
            sal_monthly = salary_txs.groupby("month")["amount"].sum().reset_index()
            sal_monthly["label"] = pd.to_datetime(sal_monthly["month"]).dt.strftime("%b %y")

            fig6 = go.Figure(go.Bar(
                x=sal_monthly["label"], y=sal_monthly["amount"],
                name="Net salary", marker_color="#3498db"
            ))
            fig6.update_layout(height=300, margin=dict(t=10, b=20, l=40, r=20), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig6, use_container_width=True)

    # ── Tax (Restskat) Tab ──
    with tab_tax:
        st.header("Restskat Payment Tracker")

        # ── Settings ──
        with st.expander("⚙️ Tax Settings", expanded=False):
            tc1, tc2 = st.columns(2)
            with tc1:
                restskat_amount_default = float(get_setting(conn, "restskat_amount", "9000"))
                restskat_amount = st.number_input(
                    "Restskat owed (DKK)", 0.0, 200000.0,
                    value=restskat_amount_default, step=100.0, key="restskat_amount",
                    help="Total amount from your årsopgørelse"
                )
                if restskat_amount != restskat_amount_default:
                    set_setting(conn, "restskat_amount", restskat_amount)

                restskat_paid_default = float(get_setting(conn, "restskat_paid", "0"))
                restskat_paid = st.number_input(
                    "Already paid (DKK)", 0.0, 200000.0,
                    value=restskat_paid_default, step=100.0, key="restskat_paid",
                    help="Amount you've already paid voluntarily"
                )
                if restskat_paid != restskat_paid_default:
                    set_setting(conn, "restskat_paid", restskat_paid)

            with tc2:
                restskat_year_default = get_setting(conn, "restskat_year", "2025")
                restskat_year = st.text_input(
                    "Tax year", value=restskat_year_default, key="restskat_year",
                    help="The year your restskat is for"
                )
                if restskat_year != restskat_year_default:
                    set_setting(conn, "restskat_year", restskat_year)

                restskat_deadline_default = get_setting(conn, "restskat_deadline", "2026-03-31")
                restskat_deadline = st.text_input(
                    "Voluntary payment deadline", value=restskat_deadline_default, key="restskat_deadline",
                    help="Last day to pay voluntarily (YYYY-MM-DD)"
                )
                if restskat_deadline != restskat_deadline_default:
                    set_setting(conn, "restskat_deadline", restskat_deadline)

        # ── Calculations ──
        remaining = max(restskat_amount - restskat_paid, 0)

        try:
            deadline_date = datetime.strptime(restskat_deadline, "%Y-%m-%d").date()
        except ValueError:
            deadline_date = date(2026, 3, 31)

        days_until = (deadline_date - date.today()).days

        # Procenttillæg: ~4.3% on unpaid amount if SKAT collects
        PROCENT_TILLAEG = 0.043
        interest_cost = remaining * PROCENT_TILLAEG

        # If SKAT collects, they spread it over remaining months of the year (typically Aug-Dec)
        months_remaining_in_year = max(12 - date.today().month + 1, 1)
        monthly_deduction = (remaining + interest_cost) / months_remaining_in_year

        # ── KPI Row ──
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Remaining", f"{remaining:,.0f} DKK",
                   f"-{restskat_paid:,.0f} paid" if restskat_paid > 0 else None)
        k2.metric("Days Until Deadline", f"{days_until}" if days_until >= 0 else "PAST DUE",
                   "voluntary payment" if days_until >= 0 else "⚠️ deadline passed",
                   delta_color="off")
        k3.metric("Interest if Unpaid", f"{interest_cost:,.0f} DKK",
                   f"{PROCENT_TILLAEG * 100:.1f}% procenttillæg", delta_color="off")
        k4.metric("Monthly Deduction", f"{monthly_deduction:,.0f} DKK",
                   f"over {months_remaining_in_year} months", delta_color="off")

        # ── Progress Bar ──
        if restskat_amount > 0:
            progress = min(restskat_paid / restskat_amount, 1.0)
            st.progress(progress, text=f"Paid {restskat_paid:,.0f} / {restskat_amount:,.0f} DKK ({progress:.0%})")

        st.markdown("---")

        # ── Payment Options Comparison ──
        st.subheader("Payment Options")
        opt1, opt2 = st.columns(2)

        with opt1:
            st.markdown("#### ✅ Option 1: Pay Now")
            st.markdown(f"- **Total cost:** {remaining:,.0f} DKK")
            st.markdown(f"- **You save:** {interest_cost:,.0f} DKK in interest")
            st.markdown(f"- **Deadline:** {restskat_deadline}")
            if days_until > 0:
                st.info(f"💡 Pay before {restskat_deadline} to avoid the {PROCENT_TILLAEG * 100:.1f}% procenttillæg.")
            else:
                st.warning("⚠️ Voluntary payment deadline has passed.")

        with opt2:
            st.markdown("#### 🏦 Option 2: Let SKAT Collect")
            total_with_interest = remaining + interest_cost
            st.markdown(f"- **Total cost:** {total_with_interest:,.0f} DKK")
            st.markdown(f"- **Extra interest:** {interest_cost:,.0f} DKK")
            st.markdown(f"- **Monthly deduction:** ~{monthly_deduction:,.0f} DKK/month")
            st.markdown(f"- **Collected over:** {months_remaining_in_year} remaining months of {date.today().year}")
            st.warning(f"⚠️ SKAT deducts {monthly_deduction:,.0f} DKK/month from your paycheck/SU.")

        # ── Comparison Chart ──
        fig_tax = go.Figure(data=[
            go.Bar(name="Pay Now", x=["Total Cost"], y=[remaining], marker_color="#2ecc71"),
            go.Bar(name="Let SKAT Collect", x=["Total Cost"], y=[total_with_interest], marker_color="#e74c3c"),
        ])
        fig_tax.update_layout(
            barmode="group", height=250,
            margin=dict(t=10, b=20, l=40, r=20),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig_tax, use_container_width=True)

        st.markdown("---")

        # ══════════════════════════════════════════════════════════════
        # Section B: Forskudsopgørelse Guide
        # ══════════════════════════════════════════════════════════════
        st.header("Forskudsopgørelse Guide (Prevent Next Year)")

        # ── Income Projection ──
        st.subheader("📊 Income Projection for Forskudsopgørelse")

        # Annual SU grant
        grant_monthly_avg = su_grant["amount"].mean() if not su_grant.empty else 6500
        annual_su_grant = grant_monthly_avg * 12

        # Annual SU loan
        loan_monthly_avg = su_loan["amount"].mean() if not su_loan.empty else 0
        annual_su_loan = loan_monthly_avg * 12

        # Job income: from salary history (bank deposits containing "noverf")
        salary_txs = df[df["description"].str.contains("noverf", case=False, na=False) & (df["amount"] > 0)]
        if not salary_txs.empty:
            salary_monthly_avg = salary_txs.groupby("month")["amount"].sum().mean()
        else:
            salary_monthly_avg = 0
        annual_job_income = salary_monthly_avg * 12

        total_annual_income = annual_su_grant + annual_su_loan + annual_job_income

        ic1, ic2, ic3, ic4 = st.columns(4)
        ic1.metric("SU Grant (annual)", f"{annual_su_grant:,.0f} DKK", f"~{grant_monthly_avg:,.0f}/mo")
        ic2.metric("SU Loan (annual)", f"{annual_su_loan:,.0f} DKK", f"~{loan_monthly_avg:,.0f}/mo")
        ic3.metric("Job Income (annual)", f"{annual_job_income:,.0f} DKK", f"~{salary_monthly_avg:,.0f}/mo")
        ic4.metric("Total Annual Income", f"{total_annual_income:,.0f} DKK", "enter this on skat.dk")

        st.caption("*Based on your transaction history averages. Adjust if your income is changing.*")

        # ── Tax Estimate ──
        st.markdown("---")
        st.subheader("📋 Estimated Tax Breakdown")

        PERSONFRADRAG = 49700  # 2026 estimate
        AM_BIDRAG_RATE = 0.08
        BUNDSKAT_RATE = 0.1209
        KOMMUNESKAT_RATE = 0.249  # ~typical Lyngby-Taarbæk / Copenhagen area

        am_bidrag = total_annual_income * AM_BIDRAG_RATE
        taxable_after_am = total_annual_income - am_bidrag
        taxable_income = max(taxable_after_am - PERSONFRADRAG, 0)

        bundskat = taxable_income * BUNDSKAT_RATE
        kommuneskat = taxable_income * KOMMUNESKAT_RATE
        total_tax = am_bidrag + bundskat + kommuneskat
        effective_rate = (total_tax / total_annual_income * 100) if total_annual_income > 0 else 0

        tx1, tx2, tx3, tx4 = st.columns(4)
        tx1.metric("AM-bidrag (8%)", f"{am_bidrag:,.0f} DKK")
        tx2.metric(f"Bundskat ({BUNDSKAT_RATE*100:.1f}%)", f"{bundskat:,.0f} DKK")
        tx3.metric(f"Kommuneskat ({KOMMUNESKAT_RATE*100:.1f}%)", f"{kommuneskat:,.0f} DKK")
        tx4.metric("Total Estimated Tax", f"{total_tax:,.0f} DKK", f"{effective_rate:.1f}% effective rate", delta_color="off")

        # Tax breakdown chart
        fig_breakdown = go.Figure(data=[
            go.Bar(
                x=[am_bidrag, bundskat, kommuneskat],
                y=["AM-bidrag", "Bundskat", "Kommuneskat"],
                orientation="h",
                marker_color=["#3498db", "#e67e22", "#e74c3c"],
                text=[f"{am_bidrag:,.0f}", f"{bundskat:,.0f}", f"{kommuneskat:,.0f}"],
                textposition="auto",
            )
        ])
        fig_breakdown.update_layout(
            height=200,
            margin=dict(t=10, b=20, l=100, r=20),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            xaxis_title="DKK",
        )
        st.plotly_chart(fig_breakdown, use_container_width=True)

        st.markdown(f"""
        **Key figures for your forskudsopgørelse:**
        - Personfradrag (personal allowance): **{PERSONFRADRAG:,} DKK** (2026)
        - Taxable income after AM-bidrag & fradrag: **{taxable_income:,.0f} DKK**
        - Monthly tax to withhold: **~{total_tax/12:,.0f} DKK/month**
        """)

        # ── Checklist ──
        st.markdown("---")
        st.subheader("✅ What to Update on skat.dk")
        st.markdown(f"""
        1. **Log in** to [skat.dk](https://skat.dk) → *Forskudsopgørelse*
        2. **Update "Lønindkomst"** (salary income) → enter **{annual_job_income:,.0f} DKK**
        3. **Check SU** is correctly listed (~{annual_su_grant:,.0f} DKK grant + {annual_su_loan:,.0f} DKK loan)
        4. **Review fradrag** (deductions) — transport, union fees, etc.
        5. **Submit** the updated forskudsopgørelse

        **When to update:**
        - Whenever your income changes significantly (new job, more/fewer shifts)
        - If you start or stop taking SU loan
        - At the beginning of each calendar year
        """)
        st.info("💡 Updating your forskudsopgørelse ensures the right tax is withheld each month — no surprise restskat next year!")


# ──────────────────────────── Snus Tracker ────────────────────────────

# Stores where snus is typically bought (gas stations, kiosks)
SNUS_STORES = re.compile(r"Q8|Circle K|Shell|7-Eleven|7-eleven|Narvesen|kiosk", re.IGNORECASE)
SNUS_PURCHASE_MIN = 55   # Snus costs ~65, so any gas station purchase >= 55 likely includes snus
SNUS_FUEL_THRESHOLD = 130  # Purchases >= 130 are likely fuel, not snus


def detect_snus_purchases(df):
    """Detect likely snus purchases: gas station/kiosk transactions between 55-130 DKK.

    Below 55 = just a drink/snack. Above 130 = fuel fill-up.
    The snus portion within each purchase is ~65 DKK, the rest is drinks/snacks.
    """
    mask = (
        df["description"].str.contains(SNUS_STORES, na=False)
        & (df["amount"] < 0)
        & (df["amount"].abs() >= SNUS_PURCHASE_MIN)
        & (df["amount"].abs() < SNUS_FUEL_THRESHOLD)
    )
    return df[mask].copy()


def render_snus_tracker(df, conn):
    st.header("Snus Tracker")
    st.caption("Track your nicotine spending and reduction progress")

    if df.empty:
        return

    n_months = max(df["month"].nunique(), 1)
    
    # Calculate streaks
    snus_df_all = detect_snus_purchases(df)
    if not snus_df_all.empty:
        last_purchase = snus_df_all["date"].max()
        days_free = (datetime.now() - last_purchase).days
    else:
        days_free = "Unknown"

    # Settings (Collapsed to reduce noise)
    with st.expander("⚙️ Tracker Settings", expanded=False):
        col_s1, col_s2 = st.columns(2)
        with col_s1:
            snus_price = float(get_setting(conn, "snus_price", "65"))
            snus_price = st.number_input(
                "Price per snus tin/pouch (DKK)", min_value=1.0,
                value=snus_price, step=5.0, key="snus_price",
            )
            set_setting(conn, "snus_price", snus_price)

        with col_s2:
            snus_per_day_default = float(get_setting(conn, "snus_per_day", "1"))
            snus_per_day = st.number_input(
                "Tins/pouches per day (current habit)", min_value=0.0,
                value=snus_per_day_default, step=0.5, key="snus_per_day",
            )
            set_setting(conn, "snus_per_day", snus_per_day)

    # Calculate costs
    daily_cost = snus_price * snus_per_day
    monthly_cost = daily_cost * 30
    yearly_cost = daily_cost * 365
    
    # Estimate drink cost
    if not snus_df_all.empty:
        avg_drink_per_purchase = max(0, snus_df_all["amount"].abs().mean() - snus_price)
        drink_daily = avg_drink_per_purchase * snus_per_day
    else:
        avg_drink_per_purchase = 25
        drink_daily = avg_drink_per_purchase * snus_per_day
    
    monthly_with_drinks = (daily_cost + drink_daily) * 30
    yearly_with_drinks = (daily_cost + drink_daily) * 365
    
    # ── Impact Dashboard ──
    st.subheader("Your Impact Dashboard")
    
    c1, c2, c3, c4 = st.columns(4)
    # Highlight days free
    c1.metric("Days Snus Fee", f"{days_free} days", help="Days since last detected purchase")
    c2.metric("Monthly Cost", f"{monthly_with_drinks:,.0f} DKK", f"{yearly_with_drinks:,.0f}/yr")
    
    total_income = df[df["amount"] > 0]["amount"].sum() / n_months
    pct_of_income = (monthly_with_drinks / total_income * 100) if total_income > 0 else 0
    c3.metric("% of Income", f"{pct_of_income:.1f}%")
    
    # Lost savings potential
    c4.metric("Lost Savings (2yr)", f"{yearly_with_drinks * 2:,.0f} DKK")
    
    st.markdown("---")
    
    # "What you could buy" - Visual cards
    st.subheader("What you could buy instead (in 1 year)")
    col_a, col_b, col_c = st.columns(3)
    with col_a:
        st.info(f"✈️ **{yearly_with_drinks / 5000:.1f}x** Weekend Trips\n\n(at 5,000 DKK each)")
    with col_b:
        st.info(f"🍔 **{yearly_with_drinks / 150:.0f}x** Nice Dinners\n\n(at 150 DKK each)")
    with col_c:
        st.success(f"💰 **{yearly_with_drinks / 12:,.0f} DKK/mo**\n\ninto savings account")

    # ── Detected purchases from bank data ──
    st.markdown("---")
    st.subheader("Detected Snus Purchases (from bank data)")
    snus_df = detect_snus_purchases(df)

    if not snus_df.empty:
        total_spent = snus_df["amount"].abs().sum()
        snus_portion = len(snus_df) * snus_price
        drink_portion = total_spent - snus_portion

        detected_monthly = snus_df.groupby("month")["amount"].agg(["sum", "count"]).reset_index()
        detected_monthly["sum"] = detected_monthly["sum"].abs()
        detected_monthly.columns = ["month", "spent", "purchases"]
        detected_monthly["snus_cost"] = detected_monthly["purchases"] * snus_price
        detected_monthly["drink_cost"] = detected_monthly["spent"] - detected_monthly["snus_cost"]
        detected_monthly["label"] = pd.to_datetime(detected_monthly["month"]).dt.strftime("%b %y")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Snus Purchases", f"{len(snus_df)}")
        c2.metric("Snus Cost", f"{snus_portion:,.0f} DKK",
                  f"{snus_portion / n_months:,.0f}/mo")
        c3.metric("Drink Tax", f"{drink_portion:,.0f} DKK",
                  f"{drink_portion / n_months:,.0f}/mo",
                  help="Extra spent on drinks bought alongside snus")
        c4.metric("Total (snus + drinks)", f"{total_spent:,.0f} DKK",
                  f"{total_spent / n_months:,.0f}/mo")

        st.caption(f"Snus = {snus_price:.0f} DKK/purchase, remaining amount = drinks/snacks bought alongside")

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=detected_monthly["label"], y=detected_monthly["snus_cost"],
            name=f"Snus ({snus_price:.0f} DKK each)", marker_color="#e74c3c",
            text=detected_monthly["snus_cost"].apply(lambda v: f"{v:,.0f}"),
            textposition="inside", textfont_size=11,
        ))
        fig.add_trace(go.Bar(
            x=detected_monthly["label"], y=detected_monthly["drink_cost"],
            name="Drinks bought with snus", marker_color="#e67e22",
            text=detected_monthly["drink_cost"].apply(lambda v: f"{v:,.0f}" if v > 0 else ""),
            textposition="inside", textfont_size=11,
        ))
        # Purchases count as line
        fig.add_trace(go.Scatter(
            x=detected_monthly["label"],
            y=detected_monthly["purchases"] * (detected_monthly["spent"].max() / detected_monthly["purchases"].max()) if detected_monthly["purchases"].max() > 0 else detected_monthly["purchases"],
            name="Purchase count", yaxis="y2",
            mode="lines+markers+text",
            line=dict(color="#2c3e50", width=2),
            text=detected_monthly["purchases"].apply(lambda v: f"{v:.0f}x"),
            textposition="top center", textfont_size=11,
        ))
        fig.update_layout(
            barmode="stack", height=450, yaxis_title="DKK", font=dict(size=13),
            margin=dict(t=30, b=40, l=60, r=20),
            legend=dict(orientation="h", y=1.08, x=0.5, xanchor="center"),
            yaxis2=dict(overlaying="y", side="right", showgrid=False, showticklabels=False),
        )
        st.plotly_chart(fig, use_container_width=True)

        # Drink tax callout
        if drink_portion > 0:
            st.info(
                f"**The drink tax:** You spend an extra **{drink_portion/n_months:,.0f} DKK/month** "
                f"on drinks bought alongside snus. If you quit snus, you also eliminate these impulse buys."
            )
    else:
        st.info("No snus purchases detected in bank data for this period.")

    # ── Reduction Plan ──
    st.markdown("---")
    st.subheader("Quit / Reduction Plan")

    target = st.radio(
        "What's your goal?",
        ["Quit completely", "Reduce to weekends only", "Cut in half", "Custom target"],
        horizontal=True, key="snus_goal",
    )

    if target == "Quit completely":
        target_per_day = 0.0
    elif target == "Reduce to weekends only":
        target_per_day = snus_per_day * (2 / 7)
    elif target == "Cut in half":
        target_per_day = snus_per_day / 2
    else:
        target_per_day = st.number_input(
            "Target tins/day", min_value=0.0, max_value=snus_per_day,
            value=snus_per_day / 2, step=0.1, key="snus_custom_target",
        )

    target_daily_cost = target_per_day * snus_price
    target_drink_daily = target_per_day * avg_drink_per_purchase
    target_monthly_cost = (target_daily_cost + target_drink_daily) * 30
    monthly_savings = monthly_with_drinks - target_monthly_cost
    yearly_savings = monthly_savings * 12

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Current total", f"{monthly_with_drinks:,.0f} DKK/mo")
    col2.metric("Target snus", f"{target_daily_cost * 30:,.0f} DKK/mo",
                f"{target_per_day:.1f}/day")
    col3.metric("Target + drinks", f"{target_monthly_cost:,.0f} DKK/mo")
    col4.metric("Monthly savings", f"{monthly_savings:,.0f} DKK/mo",
                f"{yearly_savings:,.0f}/year")

    # Reduction timeline
    if monthly_savings > 0:
        weeks_to_target = max(1, int(get_setting(conn, "snus_weeks_to_target", "8")))
        weeks_to_target = st.slider(
            "Weeks to reach target (gradual reduction)",
            min_value=1, max_value=26, value=weeks_to_target, step=1,
            key="snus_weeks",
        )
        set_setting(conn, "snus_weeks_to_target", weeks_to_target)

        # Build reduction schedule
        schedule = []
        cost_per_purchase = snus_price + avg_drink_per_purchase
        for week in range(weeks_to_target + 1):
            progress = week / weeks_to_target
            daily = snus_per_day - (snus_per_day - target_per_day) * progress
            weekly_snus = daily * snus_price * 7
            weekly_total = daily * cost_per_purchase * 7
            schedule.append({
                "week": week,
                "label": f"Week {week}",
                "per_day": daily,
                "weekly_snus": weekly_snus,
                "weekly_total": weekly_total,
            })
        sched_df = pd.DataFrame(schedule)

        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=sched_df["label"], y=sched_df["per_day"],
            name="Tins/day", fill="tozeroy",
            line=dict(color="#e74c3c", width=3),
            fillcolor="rgba(231,76,60,0.15)",
        ))
        fig2.update_layout(
            height=350, yaxis_title="Tins per day", font=dict(size=13),
            margin=dict(t=20, b=40, l=60, r=20),
        )
        st.plotly_chart(fig2, use_container_width=True)

        # Weekly targets table
        st.caption("Weekly reduction targets (snus + drink savings)")
        display_sched = sched_df.copy()
        current_weekly = snus_per_day * cost_per_purchase * 7
        display_sched["Daily usage"] = display_sched["per_day"].apply(lambda v: f"{v:.1f}")
        display_sched["Snus cost"] = display_sched["weekly_snus"].apply(lambda v: f"{v:,.0f} DKK")
        display_sched["Total cost"] = display_sched["weekly_total"].apply(lambda v: f"{v:,.0f} DKK")
        display_sched["Saved vs now"] = (current_weekly - display_sched["weekly_total"]).apply(lambda v: f"{v:,.0f} DKK")
        st.dataframe(
            display_sched[["label", "Daily usage", "Snus cost", "Total cost", "Saved vs now"]].rename(
                columns={"label": "Week"}
            ),
            use_container_width=True, hide_index=True,
        )

    # ── Long-term savings projection ──
    st.markdown("---")
    st.subheader("Savings Projection: What You'll Save")

    if monthly_savings > 0:
        projection = []
        cumulative_saved = 0
        cost_per_purchase = snus_price + avg_drink_per_purchase
        current_monthly_total = snus_per_day * cost_per_purchase * 30
        for month in range(1, 25):
            if month <= (weeks_to_target / 4.33):
                # During reduction: partial savings
                progress = min(1.0, month / (weeks_to_target / 4.33))
                avg_daily = snus_per_day - (snus_per_day - target_per_day) * (progress * 0.5 + 0.5)
                saved_this_month = (snus_per_day - avg_daily) * cost_per_purchase * 30
            else:
                saved_this_month = monthly_savings
            cumulative_saved += saved_this_month
            projection.append({
                "month": month,
                "label": f"Month {month}",
                "monthly_saved": saved_this_month,
                "cumulative": cumulative_saved,
            })
        proj_df = pd.DataFrame(projection)

        fig3 = go.Figure()
        fig3.add_trace(go.Bar(
            x=proj_df["label"], y=proj_df["monthly_saved"],
            name="Monthly savings", marker_color="rgba(46,204,113,0.5)",
        ))
        fig3.add_trace(go.Scatter(
            x=proj_df["label"], y=proj_df["cumulative"],
            name="Cumulative savings", line=dict(color="#27ae60", width=3),
            mode="lines+markers",
        ))
        fig3.update_layout(
            height=420, yaxis_title="DKK", font=dict(size=13),
            margin=dict(t=30, b=40, l=60, r=20),
            legend=dict(orientation="h", y=1.08, x=0.5, xanchor="center"),
        )
        st.plotly_chart(fig3, use_container_width=True)

        c1, c2, c3 = st.columns(3)
        c1.metric("6-month savings", f"{proj_df[proj_df['month'] <= 6]['monthly_saved'].sum():,.0f} DKK")
        c2.metric("1-year savings", f"{proj_df[proj_df['month'] <= 12]['monthly_saved'].sum():,.0f} DKK")
        c3.metric("2-year savings", f"{proj_df['monthly_saved'].sum():,.0f} DKK")

    # ── Health impact ──
    st.markdown("---")
    st.subheader("Beyond Money")
    st.markdown(f"""
Reducing from **{snus_per_day:.0f}** to **{target_per_day:.1f}** tins/day means:

- **{(snus_per_day - target_per_day) * 20:.0f} fewer nicotine pouches/day** (assuming ~20/tin)
- **{(snus_per_day - target_per_day):.0f} fewer gas station visits/day** - no more impulse drinks
- **Lower nicotine tolerance** - you'll need less to feel the effect
- **Better gum health** - reduced risk of gum recession and oral sores
- **Lower blood pressure** - nicotine raises cardiovascular stress
- **{yearly_savings:,.0f} DKK/year** back in your pocket (snus + drinks)
""")


# ──────────────────────────── Subscription Detector ────────────────────────────

def detect_subscriptions(df):
    """Detect true recurring subscriptions (consistent amount, ~1x/month).

    Finds things like Spotify, phone bills, rent, streaming — NOT groceries
    or restaurants that just happen to be visited often.

    Returns DataFrame with columns: merchant, avg_amount, months, count,
        annual_cost, category, is_fixed, first_seen, last_seen.
    """
    expenses = df[df["amount"] < 0].copy()
    if expenses.empty:
        return pd.DataFrame()

    # Exclude categories that are never subscriptions
    NON_SUB_CATS = {"Dagligvarer", "Fornøjelser og fritid", "Tøj, sko og personlig pleje"}

    expenses["merchant"] = (
        expenses["description"]
        .str.split(r"\)\)\)\)")
        .str[0]
        .str.split(r"\\\\")
        .str[0]
        .str.strip()
    )

    grouped = expenses.groupby("merchant").agg(
        months=("month", "nunique"),
        count=("amount", "count"),
        avg_amount=("amount", lambda x: x.abs().mean()),
        total=("amount", lambda x: x.abs().sum()),
        last_seen=("date", "max"),
        category=("category", "first"),
        std=("amount", lambda x: x.abs().std()),
    ).reset_index()

    # Fill NaN std (single-transaction merchants) with a high value
    grouped["std"] = grouped["std"].fillna(9999)

    # Coefficient of variation: std / mean — subscriptions have low CV
    grouped["cv"] = grouped["std"] / grouped["avg_amount"].clip(lower=1)

    # Charges per month — subscriptions are ~1x/month, groceries are 5-10x
    grouped["per_month"] = grouped["count"] / grouped["months"].clip(lower=1)

    # True subscriptions:
    # 1) Appear in 3+ months
    # 2) Consistent amount (CV < 0.3) OR from a known subscription category
    # 3) Roughly 1 charge per month (< 2.5 per month)
    # 4) Not internal transfers
    # 5) Not grocery/restaurant categories (unless CV is very low, e.g. a fixed meal plan)
    subs = grouped[
        (grouped["months"] >= 3)
        & (grouped["avg_amount"] < 5000)
        & (grouped["per_month"] < 2.5)
        & (~grouped["category"].isin(["Overførsel", "Opsparing"]))
        & (
            (grouped["cv"] < 0.3)  # consistent amount = subscription
            | (grouped["category"].isin(["Bolig", "Transport"]))  # rent/transport always included
        )
        & (
            (~grouped["category"].isin(NON_SUB_CATS))
            | (grouped["cv"] < 0.05)  # only include groceries if amount is nearly identical every time
        )
    ].copy()

    if subs.empty:
        return pd.DataFrame()

    subs["annual_cost"] = subs["avg_amount"] * 12
    subs["is_fixed"] = subs["category"].isin(["Bolig", "Transport"])
    subs["first_seen"] = expenses.groupby("merchant")["date"].min().reindex(subs["merchant"]).values
    subs = subs.sort_values("annual_cost", ascending=False)

    return subs


# ──────────────────────────── Smart Alerts ────────────────────────────

def generate_alerts(df, budget_map, current_month_str):
    """Generate smart financial alerts.

    Returns list of (severity, message) tuples.
    severity: 'error', 'warning', 'success'
    """
    alerts = []

    if not budget_map or all(v == 0 for v in budget_map.values()):
        return [("info", "Set budget targets in Settings to enable smart alerts.")]

    cur_expenses = df[(df["month"] == current_month_str) & (df["amount"] < 0)]
    if cur_expenses.empty:
        return [("success", "No spending recorded this month yet.")]

    cat_spending = cur_expenses.groupby("category")["amount"].sum().abs()

    today = date.today()
    if today.month == 12:
        last_day = date(today.year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(today.year, today.month + 1, 1) - timedelta(days=1)
    days_in_month = last_day.day
    days_elapsed = today.day
    days_left = days_in_month - days_elapsed
    month_progress = days_elapsed / days_in_month

    has_alert = False

    for cat, limit in budget_map.items():
        if limit <= 0:
            continue
        spent = cat_spending.get(cat, 0)
        pct = spent / limit

        if pct > 1.0:
            over_by = spent - limit
            alerts.append(("error", f"**{cat}** is OVER budget by {over_by:,.0f} DKK ({pct:.0%} of {limit:,.0f})"))
            has_alert = True
        elif pct > 0.6 and month_progress < 0.6:
            alerts.append(("warning", f"**{cat}**: {spent:,.0f}/{limit:,.0f} DKK spent ({pct:.0%}) with {days_left} days left"))
            has_alert = True

    # Unusual day: yesterday's spend vs daily average
    yesterday = today - timedelta(days=1)
    all_expenses = df[df["amount"] < 0]
    daily_avg = all_expenses.groupby(all_expenses["date"].dt.date)["amount"].sum().abs().mean()
    yesterday_spend = all_expenses[all_expenses["date"].dt.date == yesterday]["amount"].sum()
    yesterday_abs = abs(yesterday_spend)
    if daily_avg > 0 and yesterday_abs > daily_avg * 2.5:
        ratio = yesterday_abs / daily_avg
        alerts.append(("warning", f"Yesterday you spent {yesterday_abs:,.0f} DKK — {ratio:.1f}x your daily average"))
        has_alert = True

    if not has_alert:
        alerts.append(("success", "All categories on track this month!"))

    # Sort: errors first, then warnings, then success
    severity_order = {"error": 0, "warning": 1, "info": 2, "success": 3}
    alerts.sort(key=lambda x: severity_order.get(x[0], 9))

    return alerts[:5]


# ──────────────────────────── Overview ────────────────────────────

def render_overview(df, budgets_df, budget_map, conn):
    # ── 1. Data Prep ──
    if df.empty:
        st.info("No transactions found.")
        return

    # Sort by date ensures we get correct balance trend
    df = df.sort_values("date")
    
    current_month_str = datetime.now().strftime("%Y-%m")
    prev_month_str = (datetime.now().replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    
    # Monthly aggregations
    monthly_stats = df.groupby("month").agg({
        "amount": [
            ("income", lambda x: x[x > 0].sum()),
            ("expense", lambda x: x[x < 0].sum())
        ]
    })
    monthly_stats.columns = monthly_stats.columns.droplevel(0)
    monthly_stats["net"] = monthly_stats["income"] + monthly_stats["expense"]
    
    # Use LAST COMPLETE month for KPIs (current month is incomplete — salary
    # typically arrives at end of month, skewing mid-month figures).
    complete_months = sorted([m for m in monthly_stats.index if m < current_month_str])
    if len(complete_months) >= 2:
        last_complete = complete_months[-1]
        prev_complete = complete_months[-2]
    elif len(complete_months) == 1:
        last_complete = complete_months[-1]
        prev_complete = None
    else:
        last_complete = current_month_str  # fallback if no complete months
        prev_complete = None

    try:
        cur_inc = monthly_stats.loc[last_complete, "income"] if last_complete in monthly_stats.index else 0
        cur_exp = monthly_stats.loc[last_complete, "expense"] if last_complete in monthly_stats.index else 0
    except KeyError:
        cur_inc, cur_exp = 0, 0

    try:
        prev_inc = monthly_stats.loc[prev_complete, "income"] if prev_complete and prev_complete in monthly_stats.index else 0
        prev_exp = monthly_stats.loc[prev_complete, "expense"] if prev_complete and prev_complete in monthly_stats.index else 0
    except KeyError:
        prev_inc, prev_exp = 0, 0

    # Current (incomplete) month values for caption
    try:
        partial_inc = monthly_stats.loc[current_month_str, "income"] if current_month_str in monthly_stats.index else 0
        partial_exp = monthly_stats.loc[current_month_str, "expense"] if current_month_str in monthly_stats.index else 0
    except KeyError:
        partial_inc, partial_exp = 0, 0

    cur_net = cur_inc + cur_exp # expense is negative
    current_balance = df.iloc[-1]["balance"]
    
    # Savings Rate
    savings_rate = (cur_net / cur_inc * 100) if cur_inc > 0 else 0
    prev_savings_rate = ((prev_inc + prev_exp) / prev_inc * 100) if prev_inc > 0 else 0
    
    # Health Score Calculation (0-100)
    # 1. Savings Rate component (max 40 pts if > 20%)
    score_savings = min(max(savings_rate, 0) * 2, 40)
    # 2. Budget adherence (max 30 pts). Approx: if exp < 80% of inc -> good
    spend_ratio = abs(cur_exp) / cur_inc if cur_inc > 0 else 1.0
    score_budget = 30 if spend_ratio < 0.8 else (30 * (1 - spend_ratio) if spend_ratio < 1 else 0)
    # 3. Buffer component (max 30 pts). If balance > 1 month expense
    avg_exp = abs(monthly_stats["expense"].mean()) if not monthly_stats.empty else 1000
    runway = current_balance / avg_exp if avg_exp > 0 else 0
    score_buffer = min(runway * 10, 30)
    
    health_score = int(score_savings + score_budget + score_buffer)
    
    # Hero Greeting
    hour = datetime.now().hour
    greeting = "Good Morning" if 5 <= hour < 12 else "Good Afternoon" if 12 <= hour < 18 else "Good Evening"
    xp = compute_xp(df, budgets_df)
    level = get_level(xp)
    level_name = LEVELS.get(level, "Unknown")

    # ── 2. Hero Section ──
    with st.container():
        c_hero1, c_hero2 = st.columns([2, 1])
        with c_hero1:
            st.markdown(f"# {greeting}, Mads! 👋")
            st.caption(f"⭐ **Level {level}: {level_name}** | {xp} XP")
        with c_hero2:
            # Health Score Gauge
            fig_gauge = go.Figure(go.Indicator(
                mode = "gauge+number",
                value = health_score,
                domain = {'x': [0, 1], 'y': [0, 1]},
                title = {'text': "Health Score"},
                gauge = {
                    'axis': {'range': [0, 100]},
                    'bar': {'color': "#2ecc71" if health_score >= 70 else "#f1c40f" if health_score >= 40 else "#e74c3c"},
                    'steps': [
                        {'range': [0, 40], 'color': "rgba(231, 76, 60, 0.2)"},
                        {'range': [40, 70], 'color': "rgba(241, 196, 15, 0.2)"},
                        {'range': [70, 100], 'color': "rgba(46, 204, 113, 0.2)"}
                    ],
                }
            ))
            fig_gauge.update_layout(height=160, margin=dict(t=50, b=10, l=30, r=30), paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_gauge, use_container_width=True)

    st.markdown("---")

    # ── Smart Alerts ──
    alerts = generate_alerts(df, budget_map, current_month_str)
    if alerts:
        for severity, message in alerts[:3]:
            if severity == "error":
                st.error(message)
            elif severity == "warning":
                st.warning(message)
            elif severity == "success":
                st.success(message)
            else:
                st.info(message)
        st.markdown("---")

    # ── 3. KPI Grid (with Deltas) ──
    last_complete_label = pd.to_datetime(last_complete).strftime("%b") if last_complete != current_month_str else "Mo"
    k1, k2, k3, k4 = st.columns(4)

    k1.metric(
        "Net Worth",
        f"{current_balance:,.0f} DKK",
        delta=None, # Net worth delta needs history
        help="Current account balance"
    )
    k2.metric(
        f"Income ({last_complete_label})",
        f"{cur_inc:,.0f} DKK",
        f"{cur_inc - prev_inc:,.0f} vs prev",
        delta_color="normal",
        help="Last complete month (salary arrives end of month)"
    )
    k3.metric(
        f"Spending ({last_complete_label})",
        f"{abs(cur_exp):,.0f} DKK",
        f"{abs(cur_exp) - abs(prev_exp):,.0f} vs prev",
        delta_color="inverse", # Negative delta (less spending) is green
        help="Last complete month"
    )
    k4.metric(
        "Savings Rate",
        f"{savings_rate:.1f}%", 
        f"{savings_rate - prev_savings_rate:.1f}%",
        delta_color="normal"
    )

    # Current month progress
    current_month_label = datetime.now().strftime("%B")
    st.caption(f"📊 {current_month_label} so far: {partial_inc:,.0f} DKK income · {abs(partial_exp):,.0f} DKK spent")

    # ── Shift Callout (Calendar) ──
    shifts_df = fetch_shifts(conn)
    if not shifts_df.empty:
        now = datetime.now()
        month_shifts = shifts_df[shifts_df["date"].dt.strftime("%Y-%m") == now.strftime("%Y-%m")]
        if not month_shifts.empty:
            upcoming = month_shifts[month_shifts["date"].dt.date >= now.date()]
            total_net = month_shifts["net"].sum()
            next_shift = upcoming.head(1)
            
            with st.expander(f"📅 **Work Shift Status** ({len(upcoming)} upcoming)", expanded=False):
                sc1, sc2 = st.columns([3, 1])
                with sc1:
                    st.write(f"You have **{len(month_shifts)} shifts** this month (~{total_net:,.0f} DKK).")
                    if not next_shift.empty:
                        ns = next_shift.iloc[0]
                        st.info(f"Next: **{ns['date'].strftime('%a %d %b')}** ({ns['start'].strftime('%H:%M')}-{ns['end'].strftime('%H:%M')})")
                with sc2:
                     st.metric("Est. Pay", f"{total_net:,.0f} DKK")

    st.markdown("---")

    # ── 3b. Monthly Budget Tracker ──
    if budget_map and any(v > 0 for v in budget_map.values()):
        st.subheader("Monthly Budget")
        total_budget = sum(v for v in budget_map.values() if v > 0)
        current_month_expenses = df[(df["month"] == current_month_str) & (df["amount"] < 0)].copy()
        total_spent = abs(current_month_expenses["amount"].sum()) if not current_month_expenses.empty else 0
        total_remaining = max(total_budget - total_spent, 0)

        # Days left in month
        today = date.today()
        if today.month == 12:
            last_day = date(today.year + 1, 1, 1) - timedelta(days=1)
        else:
            last_day = date(today.year, today.month + 1, 1) - timedelta(days=1)
        days_left = max((last_day - today).days, 1)
        daily_allowance = total_remaining / days_left

        # Budget KPIs
        bk1, bk2, bk3, bk4 = st.columns(4)
        bk1.metric("Budget", f"{total_budget:,.0f} DKK")
        bk2.metric("Spent", f"{total_spent:,.0f} DKK",
                    f"{total_spent / total_budget * 100:.0f}%" if total_budget > 0 else None,
                    delta_color="inverse")
        bk3.metric("Remaining", f"{total_remaining:,.0f} DKK",
                    f"{days_left} days left", delta_color="off")
        bk4.metric("Daily Allowance", f"{daily_allowance:,.0f} DKK",
                    "spend limit per day", delta_color="off")

        # Per-category budget bars with end-of-month prediction
        cat_spending = current_month_expenses.groupby("category")["amount"].sum().abs() if not current_month_expenses.empty else pd.Series(dtype=float)
        days_in_month = last_day.day
        days_elapsed = max(today.day, 1)

        budget_rows = []
        for cat, limit in sorted(budget_map.items(), key=lambda x: x[1], reverse=True):
            if limit <= 0:
                continue
            spent = cat_spending.get(cat, 0)
            pct = spent / limit * 100 if limit > 0 else 0
            left = max(limit - spent, 0)
            # Predict end-of-month: spending velocity projection
            projected = (spent / days_elapsed) * days_in_month if days_elapsed > 0 else spent
            proj_pct = projected / limit * 100 if limit > 0 else 0
            budget_rows.append({
                "category": cat, "budget": limit, "spent": spent,
                "remaining": left, "pct": pct, "over": spent > limit,
                "projected": projected, "proj_pct": proj_pct,
            })

        for row in budget_rows:
            bc1, bc2, bc3 = st.columns([3, 1, 1])
            status = "🔴" if row["over"] else ("🟡" if row["pct"] > 75 else "🟢")
            bc1.markdown(f"{status} **{row['category']}**")
            bc2.markdown(f"**{row['spent']:,.0f}** / {row['budget']:,.0f}")
            # Show prediction if not already over
            if not row["over"] and row["proj_pct"] > 100:
                bc3.markdown(f"~{row['projected']:,.0f} projected")
            elif not row["over"]:
                bc3.markdown(f"{row['remaining']:,.0f} left")
            else:
                bc3.markdown("**OVER**")
            pct_clamped = min(row["pct"] / 100, 1.0)
            st.progress(pct_clamped)

        # Financial plan callout
        restskat_remaining = float(get_setting(conn, "restskat_amount", "9000")) - float(get_setting(conn, "restskat_paid", "0"))
        monthly_surplus = 14550 - total_budget

        if restskat_remaining > 0:
            months_to_clear = math.ceil(restskat_remaining / monthly_surplus) if monthly_surplus > 0 else 99
            st.info(
                f"**Your plan:** {total_budget:,.0f} DKK budget → ~{monthly_surplus:,.0f} DKK/mo surplus → "
                f"restskat gone in **{months_to_clear} months** → then drop SU loan and save 3,625/mo"
            )
        else:
            st.success(
                f"**Restskat paid off!** With your {monthly_surplus:,.0f} DKK/mo surplus, "
                f"you can now drop the SU loan and pocket 3,625 DKK/mo extra."
            )

        st.markdown("---")

    # ── Cash Flow Forecast ──
    st.subheader("90-Day Cash Flow Forecast")

    today_dt = datetime.now()
    hist_start = today_dt - timedelta(days=90)

    # Use FULL unfiltered data so sidebar filters don't skew the forecast
    df_all = load_transactions()
    if not df_all.empty:
        df_all = df_all.sort_values("date")

        # Only use COMPLETE months (exclude current month where income may not have arrived yet)
        complete_months = sorted([
            m for m in df_all["month"].unique()
            if m < current_month_str
        ])
        # Take last 3 complete months
        forecast_months = complete_months[-3:] if len(complete_months) >= 3 else complete_months

        if forecast_months:
            forecast_data = df_all[df_all["month"].isin(forecast_months)]
            # Monthly net (income + expenses) averaged across complete months
            monthly_nets = forecast_data.groupby("month")["amount"].sum()
            avg_monthly_net = monthly_nets.mean()
            avg_daily_net = avg_monthly_net / 30.0
        else:
            avg_daily_net = 0

        # Historical balance line (from unfiltered data)
        daily_bal_fc = df_all.groupby("date")["balance"].last().reset_index()
        daily_bal_fc = daily_bal_fc[daily_bal_fc["date"] >= hist_start]

        # Projected balance
        last_balance = df_all.iloc[-1]["balance"]
        proj_dates = [today_dt + timedelta(days=d) for d in range(1, 91)]
        proj_balances = [last_balance + avg_daily_net * d for d in range(1, 91)]

        fig_fc = go.Figure()
        fig_fc.add_trace(go.Scatter(
            x=daily_bal_fc["date"], y=daily_bal_fc["balance"],
            mode="lines", name="Historical",
            line=dict(color="#58a6ff", width=2),
        ))
        fig_fc.add_trace(go.Scatter(
            x=proj_dates, y=proj_balances,
            mode="lines", name="Projected",
            line=dict(color="#58a6ff", width=2, dash="dash"),
        ))
        fig_fc.add_hline(y=0, line_dash="dot", line_color="#e74c3c", annotation_text="0 DKK")

        fig_fc.update_layout(
            height=350, hovermode="x unified",
            yaxis_title="DKK",
            margin=dict(t=10, b=40, l=60, r=20),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", y=1.08, x=0.5, xanchor="center"),
            font=dict(color="#8b949e"),
        )
        st.plotly_chart(fig_fc, use_container_width=True)

        # Forecast KPIs
        fc1, fc2, fc3 = st.columns(3)
        bal_30 = last_balance + avg_daily_net * 30
        bal_60 = last_balance + avg_daily_net * 60
        bal_90 = last_balance + avg_daily_net * 90
        fc1.metric("Balance in 30 days", f"{bal_30:,.0f} DKK",
                   f"{avg_daily_net * 30:+,.0f}" if avg_daily_net != 0 else None)
        fc2.metric("Balance in 60 days", f"{bal_60:,.0f} DKK")
        fc3.metric("Balance in 90 days", f"{bal_90:,.0f} DKK")

        months_used = ", ".join(pd.to_datetime(forecast_months).strftime("%b %y")) if forecast_months else "none"
        st.caption(f"Based on avg monthly net of {avg_monthly_net:+,.0f} DKK from complete months: {months_used}")
    st.markdown("---")

    # ── 3c. Upcoming Bills Calendar ──
    st.subheader("Upcoming Bills")
    all_expenses = df[df["amount"] < 0].copy()
    if not all_expenses.empty:
        # Detect recurring bills: items appearing in 3+ months on similar days
        all_expenses["day"] = pd.to_datetime(all_expenses["date"]).dt.day
        all_expenses["abs_amt"] = all_expenses["amount"].abs()

        bill_candidates = (
            all_expenses.groupby("description")
            .agg(
                months=("month", "nunique"),
                avg_amount=("abs_amt", "mean"),
                std_amount=("abs_amt", "std"),
                avg_day=("day", "mean"),
                count=("description", "count"),
            )
            .reset_index()
        )
        bill_candidates["std_amount"] = bill_candidates["std_amount"].fillna(0)
        bill_candidates["cv"] = bill_candidates["std_amount"] / bill_candidates["avg_amount"].replace(0, 1)

        # Filter: at least 3 months, low CV (consistent amount), not too many per month
        recurring = bill_candidates[
            (bill_candidates["months"] >= 3)
            & (bill_candidates["cv"] < 0.35)
            & ((bill_candidates["count"] / bill_candidates["months"]) < 2.5)
        ].sort_values("avg_day")

        # Exclude grocery-like categories
        grocery_descs = set(
            all_expenses[all_expenses["category"].isin(["Dagligvarer", "Fornøjelser og fritid"])]["description"].unique()
        )
        recurring = recurring[~recurring["description"].isin(grocery_descs)]

        if not recurring.empty:
            today_day = date.today().day
            bill_rows = []
            for _, row in recurring.iterrows():
                expected_day = int(round(row["avg_day"]))
                status = "Paid" if expected_day <= today_day else "Upcoming"
                bill_rows.append({
                    "Day": expected_day,
                    "Bill": row["description"][:35],
                    "Amount": f"{row['avg_amount']:,.0f} DKK",
                    "Status": status,
                })

            bill_df_display = pd.DataFrame(bill_rows)
            total_bills = recurring["avg_amount"].sum()
            upcoming_bills = recurring[recurring["avg_day"].apply(lambda d: int(round(d)) > today_day)]["avg_amount"].sum()

            bc1, bc2, bc3 = st.columns(3)
            bc1.metric("Monthly Bills", f"{total_bills:,.0f} DKK", f"{len(recurring)} recurring")
            bc2.metric("Still Due This Month", f"{upcoming_bills:,.0f} DKK")
            remaining_after_bills = (last_balance - upcoming_bills) if last_balance else -upcoming_bills
            bc3.metric("After Bills", f"{remaining_after_bills:,.0f} DKK")

            with st.expander(f"Bill schedule ({len(recurring)} items)", expanded=False):
                st.dataframe(bill_df_display, use_container_width=True, hide_index=True)
        else:
            st.caption("Not enough history to detect recurring bills yet.")
    st.markdown("---")

    # ── 4. Main Visuals ──
    tab1, tab2 = st.tabs(["📈 Net Worth & Trends", "🧱 Spending Breakdown"])

    with tab1:
        # Net Worth Trend (Balance over time)
        daily_bal = df.groupby("date")["balance"].last().reset_index()
        
        fig_bal = go.Figure()
        fig_bal.add_trace(go.Scatter(
            x=daily_bal["date"], 
            y=daily_bal["balance"],
            mode='lines',
            name='Balance',
            line=dict(color='#58a6ff', width=3, shape='spline'),
            fill='tozeroy',
            fillcolor='rgba(88, 166, 255, 0.1)'
        ))
        fig_bal.update_layout(
            height=350,
            hovermode="x unified",
            yaxis_title="DKK",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(t=10, b=20, l=60, r=20),
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.05)'),
            font=dict(color="#8b949e")
        )
        st.subheader("Net Worth Trend")
        st.plotly_chart(fig_bal, use_container_width=True)

    with tab2:
        # ── Refined Spending Breakdown (Horizontal Bar) ──
        current_month_df = df[(df["month"] == current_month_str) & (df["amount"] < 0)].copy()
        if current_month_df.empty:
            st.info("No spending data for this month yet.")
        else:
            cat_totals = current_month_df.groupby("category")["amount"].sum().abs().sort_values(ascending=True)
            
            fig_spend = go.Figure(go.Bar(
                x=cat_totals.values,
                y=cat_totals.index,
                orientation='h',
                marker=dict(
                    color='#3498db',
                    line=dict(color='rgba(255,255,255,0.1)', width=1)
                ),
                text=cat_totals.apply(lambda v: f"{v:,.0f} DKK"),
                textposition='outside',
                cliponaxis=False
            ))
            
            fig_spend.update_layout(
                height=max(350, len(cat_totals) * 40),
                margin=dict(t=10, b=20, l=10, r=80),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.05)'),
                yaxis=dict(showgrid=False),
                font=dict(color="#8b949e", size=13)
            )
            st.subheader(f"Spending in {datetime.now().strftime('%B')}")
            st.plotly_chart(fig_spend, use_container_width=True)

    # ── 5. Budget Progress Grid ──
    if budget_map:
        st.markdown("#### 🎯 Key Budget Progress")
        # Current month spending for budget comparison
        cur_month_df = df[df["month"] == current_month_str]
        cur_month_spend = cur_month_df[cur_month_df["amount"] < 0].groupby("category")["amount"].sum().abs()
        
        budgeted_cats = [c for c in budget_map.keys() if budget_map[c] > 0]
        if budgeted_cats:
            # Sort by % used to show most "critical" first
            b_progress = []
            for cat in budgeted_cats:
                limit = budget_map[cat]
                spent = cur_month_spend.get(cat, 0)
                b_progress.append({"cat": cat, "spent": spent, "limit": limit, "pct": spent/limit})
            
            sorted_b = sorted(b_progress, key=lambda x: x["pct"], reverse=True)[:4]
            
            b_cols = st.columns(len(sorted_b))
            for i, b in enumerate(sorted_b):
                with b_cols[i]:
                    pct_clamped = min(b["pct"], 1.0)
                    st.caption(f"**{b['cat']}**")
                    st.progress(pct_clamped)
                    st.write(f"{b['spent']:,.0f} / {b['limit']:,.0f}")

    st.markdown("---")

    # ── 6. Recent Activity ──
    st.markdown("#### 🕒 Recent Activity")
    recent = df.sort_values("date", ascending=False).head(5)
    
    for _, row in recent.iterrows():
        amt_fmt = f"{row['amount']:+,.0f} DKK"
        color = '#2ecc71' if row['amount'] > 0 else '#e74c3c'
        
        # Use a more compact, styled row
        st.markdown(f"""
            <div style="display: flex; justify-content: space-between; align-items: center; padding: 10px 0; border-bottom: 1px solid rgba(255,255,255,0.05);">
                <div style="display: flex; align-items: center; gap: 15px;">
                    <div style="color: #8b949e; font-size: 0.8rem; width: 45px;">{row['date'].strftime('%d %b')}</div>
                    <div style="font-weight: 500;">{row['description']}</div>
                </div>
                <div style="color: {color}; font-weight: 600;">{amt_fmt}</div>
            </div>
        """, unsafe_allow_html=True)


# ──────────────────────────── Deep Dive ────────────────────────────

def render_analytics(df, budgets_df, budget_map):
    st.header("Analytics")

    tab_spend, tab_health, tab_report = st.tabs(["📊 Spending Analysis", "❤️ Financial Health", "📋 Monthly Report"])

    with tab_spend:
        # ── Monthly spending overview ──
        st.subheader("Monthly Spending")
        expenses = df[df["amount"] < 0].copy()
        if not expenses.empty:
            monthly_total = expenses.groupby("month")["amount"].sum().abs().reset_index()
            monthly_total.columns = ["month", "total"]
            monthly_total = monthly_total.sort_values("month")
            monthly_total["label"] = pd.to_datetime(monthly_total["month"]).dt.strftime("%b %y")
            avg_spend = monthly_total["total"].mean()

            fig_ms = go.Figure()
            fig_ms.add_trace(go.Bar(
                x=monthly_total["label"], y=monthly_total["total"],
                marker_color=[
                    "#e74c3c" if v > avg_spend else "#3498db"
                    for v in monthly_total["total"]
                ],
                text=monthly_total["total"].apply(lambda v: f"{v:,.0f}"),
                textposition="outside", textfont_size=11,
                hovertemplate="%{x}: %{y:,.0f} DKK<extra></extra>",
            ))
            fig_ms.add_hline(
                y=avg_spend, line_dash="dot", line_color="#8b949e",
                annotation_text=f"Avg: {avg_spend:,.0f}",
                annotation_position="top left",
                annotation_font_color="#8b949e",
            )
            fig_ms.update_layout(
                height=380,
                yaxis_title="DKK",
                margin=dict(t=30, b=40, l=60, r=20),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.05)"),
                font=dict(color="#8b949e", size=13),
                showlegend=False,
            )
            st.plotly_chart(fig_ms, use_container_width=True)
            st.caption("Blue = below average, Red = above average")
        else:
            st.info("No spending data available.")
        
        st.markdown("---")

        # ── Top merchants ──
        st.subheader("Top Merchants")
        if not df.empty:
            month_filter = st.selectbox("Filter by month", ["All"] + sorted(df["month"].unique().tolist()), key="dd_month")
            df_m = df if month_filter == "All" else df[df["month"] == month_filter]
            merchants = df_m[df_m["amount"] < 0].copy()
            # Clean merchant names (Nordea text often has weird suffixes)
            if not merchants.empty:
                merchants["merchant"] = merchants["description"].str.split(r"\)\)\)\)").str[0].str.split(r"\\\\").str[0].str.strip()
                merch_total = merchants.groupby("merchant")["amount"].agg(["sum", "count"]).reset_index()
                merch_total.columns = ["merchant", "total", "count"]
                merch_total["total"] = merch_total["total"].abs()
                merch_total = merch_total.sort_values("total", ascending=False).head(20)

                merch_total = merch_total.sort_values("total", ascending=True)  # ascending for horizontal bar readability
                fig = px.bar(merch_total, y="merchant", x="total", orientation="h",
                            text="total", color="total", color_continuous_scale="Reds")
                fig.update_layout(height=max(500, len(merch_total) * 35),
                                margin=dict(t=10, b=20, l=20, r=80),
                                font=dict(size=13), coloraxis_showscale=False,
                                yaxis=dict(tickfont=dict(size=12)),
                                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
                fig.update_traces(texttemplate="%{text:,.0f} DKK", textposition="outside", textfont_size=11)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No expenses found for selected period.")
        
        st.markdown("---")

        # ── Category drill-down ──
        st.subheader("Category Drill-Down")
        categories = sorted(df[df["amount"] < 0]["category"].unique())
        if categories:
            selected_cat = st.selectbox("Select category", categories, key="dd_cat")
            cat_data = df[(df["category"] == selected_cat) & (df["amount"] < 0)]

            if not cat_data.empty:
                sub_totals = cat_data.groupby("subcategory")["amount"].sum().abs().sort_values(ascending=True)
                fig2 = px.bar(x=sub_totals.values, y=sub_totals.index, orientation="h",
                            labels={"x": "DKK", "y": "Subcategory"}, text=sub_totals.values)
                fig2.update_layout(height=max(300, len(sub_totals) * 45),
                                margin=dict(t=10, b=20, l=20, r=60),
                                font=dict(size=13),
                                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
                fig2.update_traces(texttemplate="%{text:,.0f}", textposition="outside", textfont_size=11)
                st.plotly_chart(fig2, use_container_width=True)

                st.caption(f"Recent transactions in {selected_cat}")
                st.dataframe(
                    cat_data[["date", "subcategory", "description", "amount"]].sort_values("date", ascending=False).head(50),
                    use_container_width=True,
                    height=400,
                )
        else:
            st.info("No categories found.")

        st.markdown("---")

        # ── Spending trend (rolling averages) ──
        st.subheader("Spending Trend")
        daily_spend = df[df["amount"] < 0].groupby(df["date"].dt.date)["amount"].sum().abs()
        if not daily_spend.empty:
            date_range = pd.date_range(daily_spend.index.min(), daily_spend.index.max())
            daily_full = daily_spend.reindex(date_range, fill_value=0)
            trend_df = pd.DataFrame({"date": daily_full.index, "daily": daily_full.values})
            trend_df["7d_avg"] = trend_df["daily"].rolling(7, min_periods=1).mean()
            trend_df["30d_avg"] = trend_df["daily"].rolling(30, min_periods=1).mean()

            fig3 = go.Figure()
            fig3.add_trace(go.Bar(x=trend_df["date"], y=trend_df["daily"], name="Daily",
                                marker_color="rgba(189,195,199,0.4)"))
            fig3.add_trace(go.Scatter(x=trend_df["date"], y=trend_df["7d_avg"], name="7-day avg",
                                    line=dict(color="#3498db", width=2.5)))
            fig3.add_trace(go.Scatter(x=trend_df["date"], y=trend_df["30d_avg"], name="30-day avg",
                                    line=dict(color="#e74c3c", width=3)))
            fig3.update_layout(height=420, margin=dict(t=20, b=40, l=60, r=20),
                            yaxis_title="DKK", font=dict(size=13),
                            legend=dict(orientation="h", y=1.08, x=0.5, xanchor="center"),
                            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig3, use_container_width=True)

        st.markdown("---")

        # ── Enhanced Subscriptions tracker ──
        st.subheader("Detected Subscriptions & Recurring Payments")
        subs_df = detect_subscriptions(df)
        if not subs_df.empty:
            # KPI row
            monthly_sub_total = subs_df["avg_amount"].sum()
            annual_sub_total = subs_df["annual_cost"].sum()
            num_active = len(subs_df)

            sk1, sk2, sk3 = st.columns(3)
            sk1.metric("Monthly Subscriptions", f"{monthly_sub_total:,.0f} DKK")
            sk2.metric("Annual Cost", f"{annual_sub_total:,.0f} DKK")
            sk3.metric("Active Subscriptions", f"{num_active}")

            # Separate fixed vs lifestyle
            fixed = subs_df[subs_df["is_fixed"]].copy()
            lifestyle = subs_df[~subs_df["is_fixed"]].copy()

            if not lifestyle.empty:
                st.markdown("**Lifestyle Subscriptions**")
                display_life = lifestyle[["merchant", "avg_amount", "annual_cost", "months", "category"]].copy()
                display_life.columns = ["Service", "Avg Monthly (DKK)", "Annual Cost (DKK)", "Months Seen", "Category"]
                display_life["Avg Monthly (DKK)"] = display_life["Avg Monthly (DKK)"].round(0)
                display_life["Annual Cost (DKK)"] = display_life["Annual Cost (DKK)"].round(0)
                st.dataframe(display_life.head(20), use_container_width=True, hide_index=True)

            if not fixed.empty:
                st.markdown("**Fixed Costs (Rent, Transport, Utilities)**")
                display_fixed = fixed[["merchant", "avg_amount", "annual_cost", "months", "category"]].copy()
                display_fixed.columns = ["Service", "Avg Monthly (DKK)", "Annual Cost (DKK)", "Months Seen", "Category"]
                display_fixed["Avg Monthly (DKK)"] = display_fixed["Avg Monthly (DKK)"].round(0)
                display_fixed["Annual Cost (DKK)"] = display_fixed["Annual Cost (DKK)"].round(0)
                st.dataframe(display_fixed.head(20), use_container_width=True, hide_index=True)

            # Highlight new subscriptions
            two_months_ago = (datetime.now() - timedelta(days=60))
            new_subs = subs_df[subs_df["first_seen"] >= two_months_ago]
            if not new_subs.empty:
                st.warning(f"**New recurring charges** (first seen in last 2 months): "
                           + ", ".join(f"{r['merchant']} ({r['avg_amount']:,.0f} DKK)" for _, r in new_subs.iterrows()))
        else:
            st.info("No recurring payments detected.")

    with tab_health:
        st.subheader("Financial Health Check")
        
        # Savings Rate Gauge, Runway, etc.
        if not df.empty:
            current_balance = df.iloc[-1]["balance"]
            total_income = df[df["amount"] > 0]["amount"].sum()
            total_expense = df[df["amount"] < 0]["amount"].sum()
            n_months = max(df["month"].nunique(), 1)
            
            monthly_income = total_income / n_months
            monthly_spend = abs(total_expense) / n_months
            savings_rate = ((total_income + total_expense) / total_income * 100) if total_income > 0 else 0
            # Runway: how many months can we survive on current balance?
            runway = current_balance / monthly_spend if monthly_spend > 0 else 999
            
            c1, c2 = st.columns(2)
            with c1:
                st.metric("Savings Rate", f"{savings_rate:+.1f}%")
                # Using a simpler gauge indicator
                fig_g = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=savings_rate,
                    domain={'x': [0, 1], 'y': [0, 1]},
                    title={'text': "Savings Rate"},
                    gauge={
                        'axis': {'range': [-20, 50]},
                        'bar': {'color': "#2ecc71" if savings_rate > 20 else "#f1c40f" if savings_rate > 0 else "#e74c3c"},
                        'steps': [
                            {'range': [-20, 0], 'color': "rgba(231, 76, 60, 0.2)"},
                            {'range': [0, 20], 'color': "rgba(241, 196, 15, 0.2)"},
                            {'range': [20, 50], 'color': "rgba(46, 204, 113, 0.2)"}],
                    }
                ))
                fig_g.update_layout(height=250, margin=dict(t=30, b=10, l=30, r=40), paper_bgcolor="rgba(0,0,0,0)", font=dict(color="white"))
                st.plotly_chart(fig_g, use_container_width=True)
                
            with c2:
                st.metric("Runway (Months)", f"{runway:.1f} months", help="How long your current balance lasts at avg spending rate")
                if runway < 3:
                    st.warning("⚠️ Runway is below 3 months. Consider building an emergency fund.")
                elif runway > 6:
                    st.success("✅ Healthy runway (>6 months).")
                else:
                    st.info("ℹ️ Runway is between 3-6 months.")
            
            st.markdown("---")
            st.subheader("Top 3 Most Expensive Days")
            daily_sum = df[df["amount"] < 0].groupby("date")["amount"].sum().abs().reset_index().sort_values("amount", ascending=False).head(3)
            for i, row in daily_sum.iterrows():
                st.markdown(f"**{i+1}. {row['date'].strftime('%d %b %Y')}**: {row['amount']:,.0f} DKK")
                # Show top purchase that day
                day_tx = df[(df["date"] == row["date"]) & (df["amount"] < 0)].sort_values("amount", ascending=True).head(1)
                if not day_tx.empty:
                    desc = day_tx.iloc[0]["description"]
                    amt = day_tx.iloc[0]["amount"]
                    st.caption(f"Main culprit: {desc} ({abs(amt):,.0f} DKK)")
        else:
            st.info("No data available for health check.")

    with tab_report:
        st.subheader("Monthly Report Card")

        if df.empty or not budget_map:
            st.info("Need transaction data and budget targets to generate a report.")
        else:
            all_months = sorted(df["month"].unique())
            # Default to previous complete month
            current_m = datetime.now().strftime("%Y-%m")
            default_month = all_months[-2] if len(all_months) >= 2 and all_months[-1] == current_m else all_months[-1]
            report_month = st.selectbox("Select month", all_months, index=all_months.index(default_month), key="report_month")

            month_df = df[(df["month"] == report_month) & (df["amount"] < 0)]

            if month_df.empty:
                st.info(f"No spending data for {report_month}.")
            else:
                cat_spending = month_df.groupby("category")["amount"].sum().abs()

                # Grade function
                def grade_category(spent, budget):
                    if budget <= 0:
                        return "N/A"
                    pct = spent / budget
                    if pct <= 0.8:
                        return "A"
                    elif pct <= 1.0:
                        return "B"
                    elif pct <= 1.2:
                        return "C"
                    elif pct <= 1.5:
                        return "D"
                    return "F"

                # Previous month for comparison
                month_idx = all_months.index(report_month)
                prev_m = all_months[month_idx - 1] if month_idx > 0 else None
                prev_spending = df[(df["month"] == prev_m) & (df["amount"] < 0)].groupby("category")["amount"].sum().abs() if prev_m else pd.Series(dtype=float)

                # Build grades table
                grade_rows = []
                grade_values = {"A": 4, "B": 3, "C": 2, "D": 1, "F": 0, "N/A": None}
                total_weight = 0
                weighted_sum = 0

                for cat, limit in sorted(budget_map.items()):
                    if limit <= 0:
                        continue
                    spent = cat_spending.get(cat, 0)
                    pct = spent / limit
                    g = grade_category(spent, limit)
                    prev_val = prev_spending.get(cat, 0) if not prev_spending.empty else 0
                    change = spent - prev_val

                    grade_rows.append({
                        "Category": cat,
                        "Budget": f"{limit:,.0f}",
                        "Spent": f"{spent:,.0f}",
                        "% Used": f"{pct:.0%}",
                        "Grade": g,
                        "vs Last Month": f"{change:+,.0f}",
                    })

                    if grade_values.get(g) is not None:
                        weighted_sum += grade_values[g] * limit
                        total_weight += limit

                # Overall grade
                if total_weight > 0:
                    avg_score = weighted_sum / total_weight
                    if avg_score >= 3.5:
                        overall = "A"
                    elif avg_score >= 2.5:
                        overall = "B"
                    elif avg_score >= 1.5:
                        overall = "C"
                    elif avg_score >= 0.5:
                        overall = "D"
                    else:
                        overall = "F"
                else:
                    overall = "N/A"

                grade_colors = {"A": "#2ecc71", "B": "#27ae60", "C": "#f1c40f", "D": "#e67e22", "F": "#e74c3c", "N/A": "#95a5a6"}

                # Display overall grade
                gc1, gc2 = st.columns([1, 4])
                with gc1:
                    grade_color = grade_colors.get(overall, '#fff')
                    st.markdown(
                        f"<div style='text-align:center; font-size:60px; font-weight:bold; "
                        f"color:{grade_color};'>{overall}</div>",
                        unsafe_allow_html=True,
                    )
                    st.caption("Overall Grade")
                with gc2:
                    # Highlights
                    if grade_rows:
                        # Biggest win
                        budgeted_cats = {r["Category"]: float(r["% Used"].strip("%")) / 100 for r in grade_rows}
                        best_cat = min(budgeted_cats, key=budgeted_cats.get)
                        worst_cat = max(budgeted_cats, key=budgeted_cats.get)
                        st.success(f"**Biggest Win:** {best_cat} ({budgeted_cats[best_cat]:.0%} of budget)")
                        if budgeted_cats[worst_cat] > 1.0:
                            st.error(f"**Biggest Overspend:** {worst_cat} ({budgeted_cats[worst_cat]:.0%} of budget)")

                st.markdown("---")

                # Grades table
                st.dataframe(pd.DataFrame(grade_rows), use_container_width=True, hide_index=True)

                st.markdown("---")

                # Radar chart
                st.subheader("Budget Radar")
                radar_cats = [r["Category"] for r in grade_rows]
                radar_vals = [float(r["% Used"].strip("%")) for r in grade_rows]

                if radar_cats:
                    # Close the polygon
                    radar_cats_closed = radar_cats + [radar_cats[0]]
                    radar_vals_closed = radar_vals + [radar_vals[0]]

                    fig_radar = go.Figure()
                    fig_radar.add_trace(go.Scatterpolar(
                        r=radar_vals_closed, theta=radar_cats_closed,
                        fill="toself", name="% of Budget",
                        line=dict(color="#3498db"),
                        fillcolor="rgba(52,152,219,0.2)",
                    ))
                    # 100% reference line
                    fig_radar.add_trace(go.Scatterpolar(
                        r=[100] * len(radar_cats_closed), theta=radar_cats_closed,
                        name="100% Budget", line=dict(color="#e74c3c", dash="dash", width=1),
                    ))
                    fig_radar.update_layout(
                        polar=dict(radialaxis=dict(visible=True, range=[0, max(max(radar_vals) * 1.2, 120)])),
                        height=400, margin=dict(t=30, b=30, l=60, r=60),
                        paper_bgcolor="rgba(0,0,0,0)",
                        legend=dict(orientation="h", y=-0.1, x=0.5, xanchor="center"),
                    )
                    st.plotly_chart(fig_radar, use_container_width=True)

                # Month-over-month comparison
                if prev_m:
                    st.markdown("---")
                    st.subheader("Month-over-Month Comparison")

                    compare_cats = sorted(set(cat_spending.index) | set(prev_spending.index))
                    this_vals = [cat_spending.get(c, 0) for c in compare_cats]
                    prev_vals = [prev_spending.get(c, 0) for c in compare_cats]

                    fig_mom = go.Figure()
                    fig_mom.add_trace(go.Bar(
                        x=compare_cats, y=prev_vals,
                        name=prev_m, marker_color="#95a5a6",
                    ))
                    fig_mom.add_trace(go.Bar(
                        x=compare_cats, y=this_vals,
                        name=report_month, marker_color="#3498db",
                    ))
                    fig_mom.update_layout(
                        barmode="group", height=400, yaxis_title="DKK",
                        margin=dict(t=30, b=40, l=60, r=20),
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                        legend=dict(orientation="h", y=1.08, x=0.5, xanchor="center"),
                    )
                    st.plotly_chart(fig_mom, use_container_width=True)

                # Budget streak counter
                st.markdown("---")
                st.subheader("Budget Streaks")
                streak_data = []
                for cat, limit in budget_map.items():
                    if limit <= 0:
                        continue
                    streak = 0
                    for m in reversed(all_months):
                        m_spend = df[(df["month"] == m) & (df["amount"] < 0) & (df["category"] == cat)]["amount"].sum()
                        if abs(m_spend) <= limit:
                            streak += 1
                        else:
                            break
                    streak_data.append({"Category": cat, "Consecutive Months Under Budget": streak})

                if streak_data:
                    streak_sorted = sorted(streak_data, key=lambda x: x["Consecutive Months Under Budget"], reverse=True)
                    st.dataframe(pd.DataFrame(streak_sorted), use_container_width=True, hide_index=True)

                # PDF Export
                st.markdown("---")
                st.subheader("Export Report")
                try:
                    from report_generator import generate_monthly_report
                    pdf_bytes = generate_monthly_report(df, budgets_df, report_month)
                    st.download_button(
                        label=f"Download PDF Report ({report_month})",
                        data=pdf_bytes,
                        file_name=f"budget_report_{report_month}.pdf",
                        mime="application/pdf",
                        type="primary",
                    )
                except ImportError:
                    st.warning("PDF export requires fpdf2: `pip install fpdf2`")
                except Exception as e:
                    st.error(f"Failed to generate PDF: {e}")


# ──────────────────────────── AI Insights ────────────────────────────

def render_ai_insights(df, budgets_df, conn):
    st.header("AI Financial Advisor")

    import os
    api_key = os.environ.get("ANTHROPIC_API_KEY", get_setting(conn, "anthropic_api_key", ""))

    if not api_key:
        st.warning(
            "To use the AI advisor, set your Anthropic API key:\n\n"
            "1. Go to **Settings** and enter it under 'AI Insights', or\n"
            "2. Set the `ANTHROPIC_API_KEY` environment variable."
        )
        # Allow setting key here too
        new_key = st.text_input("Enter Anthropic API key", type="password", key="ai_key_input")
        if new_key:
            set_setting(conn, "anthropic_api_key", new_key)
            os.environ["ANTHROPIC_API_KEY"] = new_key
            st.success("API key saved!")
            st.rerun()
        return

    # Ensure env var is set for the SDK
    if not os.environ.get("ANTHROPIC_API_KEY"):
        os.environ["ANTHROPIC_API_KEY"] = api_key

    try:
        from ai_insights import get_financial_insights
    except ImportError:
        st.error("AI insights module not found. Ensure `ai_insights.py` is in the finances folder and `pip install anthropic`.")
        return

    # Chat interface
    if "ai_chat_history" not in st.session_state:
        st.session_state.ai_chat_history = []

    # Quick action buttons
    st.markdown("**Quick insights:**")
    qc1, qc2, qc3 = st.columns(3)
    quick_q = None
    with qc1:
        if st.button("Monthly Check-in", use_container_width=True):
            quick_q = "GENERAL"
    with qc2:
        if st.button("How to save more?", use_container_width=True):
            quick_q = "What are 3 specific ways I can reduce spending this month based on my actual data?"
    with qc3:
        if st.button("SU Loan Strategy", use_container_width=True):
            quick_q = "Based on my income and spending, how close am I to being able to drop the SU loan? What would it take?"

    # Custom question
    user_q = st.chat_input("Ask about your finances...")

    question = user_q or quick_q

    # Display chat history
    for msg in st.session_state.ai_chat_history:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    if question is not None:
        # "GENERAL" means no specific question (triggers general monthly analysis)
        api_question = None if question == "GENERAL" else question
        display_q = "Give me a monthly financial check-in." if question == "GENERAL" else question

        # Add user message to history
        st.session_state.ai_chat_history.append({"role": "user", "content": display_q})
        with st.chat_message("user"):
            st.markdown(display_q)

        # Generate response
        with st.chat_message("assistant"):
            with st.spinner("Analyzing your finances..."):
                # ai_insights expects 'budget' column, our DB uses 'monthly_limit'
                ai_budgets = budgets_df.rename(columns={"monthly_limit": "budget"}) if not budgets_df.empty else budgets_df
                response = get_financial_insights(df, ai_budgets, question=api_question)
            st.markdown(response)

        st.session_state.ai_chat_history.append({"role": "assistant", "content": response})

    if st.session_state.ai_chat_history:
        if st.button("Clear chat", key="clear_ai_chat"):
            st.session_state.ai_chat_history = []
            st.rerun()


# ──────────────────────────── Achievements & Goals ────────────────────────────

def render_achievements(df, achievements_df, budgets_df, budget_map, conn):
    st.header("🏆 Achievements & Goals")

    # ── Level & XP ──
    xp = compute_xp(df, budgets_df)
    level = get_level(xp)
    level_name = LEVELS.get(level, "Unknown")
    xp_in_level = xp % XP_PER_LEVEL
    progress_pct = min(xp_in_level / XP_PER_LEVEL, 1.0)

    # Hero section for Level
    with st.container(border=True):
        c1, c2 = st.columns([1, 4])
        with c1:
            st.markdown(f"<h1 style='text-align: center; font-size: 50px;'>{level}</h1>", unsafe_allow_html=True)
            st.caption(f"Current Level", unsafe_allow_html=True)
        with c2:
            st.subheader(f"{level_name}")
            st.progress(progress_pct)
            st.caption(f"**{xp_in_level} / {XP_PER_LEVEL} XP** to next level | Total XP: {xp}")
            if level < 10:
                next_level_name = LEVELS.get(level + 1, "Unknown")
                st.caption(f"Next Rank: **{next_level_name}**")

    st.markdown("---")

    # ── Achievement Badges ──
    st.subheader("Badges")
    unlocked = set(achievements_df["name"].tolist()) if not achievements_df.empty else set()
    
    # Grid layout
    cols = st.columns(4)
    for i, (name, info) in enumerate(ACHIEVEMENT_DEFS.items()):
        col = cols[i % 4]
        is_unlocked = name in unlocked
        icon = info["icon"]
        
        with col.container(border=True):
            if is_unlocked:
                st.markdown(f"<div style='text-align: center; font-size: 40px;'>{icon}</div>", unsafe_allow_html=True)
                st.markdown(f"<div style='text-align: center; font-weight: bold;'>{name}</div>", unsafe_allow_html=True)
                st.caption(f"{info['desc']}")
            else:
                # Locked state: grayscale / opacity
                st.markdown(f"<div style='text-align: center; font-size: 40px; filter: grayscale(100%); opacity: 0.3;'>{icon}</div>", unsafe_allow_html=True)
                st.markdown(f"<div style='text-align: center; font-weight: bold; color: #7f8c8d;'>{name}</div>", unsafe_allow_html=True)
                st.caption(f"🔒 {info['desc']}")

    st.markdown("---")

    # ── Monthly Challenge ──
    st.subheader("🔥 Monthly Quest")
    if not df.empty:
        n_months = max(df["month"].nunique(), 1)
        cat_monthly_avg = df[df["amount"] < 0].groupby("category")["amount"].sum().abs() / n_months
        if not cat_monthly_avg.empty:
            worst_cat = cat_monthly_avg.idxmax()
            worst_val = cat_monthly_avg.max()
            target = worst_val * 0.8
            
            with st.container(border=True):
                qc1, qc2 = st.columns([1, 4])
                with qc1:
                    st.markdown("<div style='font-size: 40px; text-align: center;'>⚔️</div>", unsafe_allow_html=True)
                with qc2:
                    st.markdown(f"**Tame the Beast: {worst_cat}**")
                    st.write(f"Reduce spending to under **{target:,.0f} DKK** this month.")
                    st.caption(f"Current Avg: {worst_val:,.0f} DKK. Reward: +200 XP")

    st.markdown("---")

    # ── Savings Goals ──
    st.subheader("🎯 Savings Goals")

    goals_df = load_goals()
    monthly_net = df.groupby("month")["amount"].sum().mean() if not df.empty else 0

    # Display existing goals as cards
    if not goals_df.empty:
        goal_cols = st.columns(min(len(goals_df), 3))
        for idx, (_, goal_row) in enumerate(goals_df.iterrows()):
            with goal_cols[idx % 3]:
                with st.container(border=True):
                    pct = goal_row["saved"] / goal_row["target"] if goal_row["target"] > 0 else 0
                    pct_clamped = min(pct, 1.0)

                    # Progress ring (donut chart)
                    fig_ring = go.Figure(go.Pie(
                        values=[goal_row["saved"], max(goal_row["target"] - goal_row["saved"], 0)],
                        hole=0.7,
                        marker_colors=["#2ecc71", "rgba(255,255,255,0.05)"],
                        textinfo="none", hoverinfo="skip",
                    ))
                    fig_ring.update_layout(
                        height=140, margin=dict(t=10, b=10, l=10, r=10),
                        paper_bgcolor="rgba(0,0,0,0)",
                        showlegend=False,
                        annotations=[dict(text=f"{pct:.0%}", x=0.5, y=0.5, font_size=20, showarrow=False)],
                    )
                    st.plotly_chart(fig_ring, use_container_width=True)

                    st.markdown(f"**{goal_row['name']}**")
                    st.caption(f"{goal_row['saved']:,.0f} / {goal_row['target']:,.0f} DKK")

                    # Estimated completion
                    remaining = goal_row["target"] - goal_row["saved"]
                    if remaining > 0 and monthly_net > 0:
                        months_to_go = remaining / monthly_net
                        est_date = datetime.now() + timedelta(days=months_to_go * 30)
                        st.caption(f"Est. completion: {est_date.strftime('%b %Y')}")

                    if goal_row["deadline"]:
                        days_to_deadline = (datetime.strptime(goal_row["deadline"], "%Y-%m-%d").date() - date.today()).days
                        st.caption(f"Deadline: {goal_row['deadline']} ({days_to_deadline} days)")

                    # Update progress
                    new_saved = st.number_input(
                        "Update saved", min_value=0.0, value=float(goal_row["saved"]),
                        step=500.0, key=f"goal_saved_{goal_row['id']}",
                    )
                    if new_saved != goal_row["saved"]:
                        update_goal_progress(conn, goal_row["id"], new_saved)
                        st.rerun()

                    if st.button("Delete", key=f"goal_del_{goal_row['id']}"):
                        delete_goal(conn, goal_row["id"])
                        st.rerun()
    else:
        st.info("No savings goals yet. Add one below!")

    # Add new goal form
    st.markdown("---")
    with st.expander("Add New Goal"):
        with st.form("new_goal_form"):
            g_name = st.text_input("Goal name", placeholder="e.g. Emergency Fund")
            g_target = st.number_input("Target amount (DKK)", min_value=100.0, value=10000.0, step=500.0)
            g_deadline = st.date_input("Deadline (optional)", value=None)
            submitted = st.form_submit_button("Add Goal")
            if submitted and g_name:
                deadline_str = g_deadline.strftime("%Y-%m-%d") if g_deadline else None
                save_goal(conn, g_name, g_target, deadline_str)
                st.rerun()

        # Suggestions
        st.caption("**Quick suggestions:**")
        sg1, sg2 = st.columns(2)
        with sg1:
            if st.button("Emergency Fund (10,000 DKK)"):
                save_goal(conn, "Emergency Fund", 10000)
                st.rerun()
        with sg2:
            if st.button("Pay off Restskat (9,000 DKK)"):
                save_goal(conn, "Pay off Restskat", 9000)
                st.rerun()



def render_deals(conn, df=None, budgets_df=None):
    st.header("Grocery Deals")

    if not DEALS_AVAILABLE:
        st.error("Deals module not available. Ensure `deals.py` is in the project directory.")
        return

    lat = float(get_setting(conn, "deals_lat", "55.786"))
    lng = float(get_setting(conn, "deals_lng", "12.524"))
    radius = int(get_setting(conn, "deals_radius", "10000"))
    api_kwargs = {"lat": lat, "lng": lng, "radius": radius}

    # ── A. Budget callout ──
    budget_status = get_grocery_budget_status(conn)
    if budget_status["limit"] > 0:
        pct = budget_status["pct_used"]
        if pct < 60:
            color = "green"
        elif pct < 80:
            color = "orange"
        else:
            color = "red"
        c1, c2, c3 = st.columns(3)
        c1.metric("Dagligvarer spent", f"{budget_status['spent']:,.0f} / {budget_status['limit']:,.0f} DKK")
        c2.metric("Remaining", f"{budget_status['remaining']:,.0f} DKK")
        c3.metric("Daily budget left", f"{budget_status['daily_remaining']:,.0f} DKK/day ({budget_status['days_left']}d)")
        st.progress(min(1.0, pct / 100), text=f"**:{color}[{pct:.0f}% used]**")

    # ── B. Suggested For You ──
    cache_key = "deals_suggestions"
    cache_ts_key = "deals_suggestions_ts"
    suggestions = st.session_state.get(cache_key)
    suggestions_ts = st.session_state.get(cache_ts_key, 0)
    if suggestions is None or (time.time() - suggestions_ts) > 900:
        freq = get_frequent_searches(conn, 6)
        if freq:
            suggestions = []
            for item in freq:
                try:
                    offers = smart_search(conn, item["name"], source="auto", limit=3, **api_kwargs)
                    if offers:
                        suggestions.append({"query": item["name"], "offer": offers[0]})
                except Exception:
                    pass
            st.session_state[cache_key] = suggestions
            st.session_state[cache_ts_key] = time.time()
        else:
            suggestions = []

    if suggestions:
        st.subheader("Suggested For You")
        for i in range(0, len(suggestions), 2):
            cols = st.columns(2)
            for j, col in enumerate(cols):
                idx = i + j
                if idx >= len(suggestions):
                    break
                s = suggestions[idx]
                o = s["offer"]
                with col:
                    price_str = f"{o['price']:,.2f}".replace(",", ".") if o["price"] else "?"
                    heading = o["heading"] or s["query"]
                    dealer = o["dealer"] or "Unknown"
                    card = f"**{heading}**\n\n**{price_str} DKK** at {dealer}"
                    if o.get("discount_pct", 0) > 0:
                        card += f" — **{o['discount_pct']:.0f}% off**"
                    if o.get("is_all_time_low"):
                        card += "\n\n:red[ALL-TIME LOW!]"
                    st.markdown(card)
                    # Action buttons
                    bc1, bc2 = st.columns(2)
                    with bc1:
                        with st.popover("Price History", use_container_width=True):
                            product_key = (heading or "").lower().strip()
                            hist = get_price_history(conn, product_key)
                            if len(hist) >= 2:
                                hdf = pd.DataFrame(hist)
                                hdf["date"] = pd.to_datetime(hdf["date"])
                                fig_h = px.line(hdf, x="date", y="price", color="dealer", markers=True,
                                                labels={"price": "DKK", "date": ""})
                                fig_h.update_layout(height=250, margin=dict(t=5, b=5, l=5, r=5), showlegend=True)
                                st.plotly_chart(fig_h, use_container_width=True)
                            else:
                                st.caption("Not enough data yet — prices are tracked each time you search.")
                    with bc2:
                        if st.button("Add to List", key=f"sug_add_{idx}", use_container_width=True):
                            add_to_shopping_list(conn, heading, price=o.get("price"), as_staple=True)
                            st.toast(f"Added **{heading}** to your shopping list!")
                    st.divider()

    # ── C. Search bar + category chips ──
    col_search, col_btn = st.columns([4, 1])
    with col_search:
        query = st.text_input(
            "Search for groceries",
            placeholder="e.g. kylling, mælk, brød...",
            label_visibility="collapsed",
            key="deals_search",
        )
    with col_btn:
        search_clicked = st.button("Search", type="primary", use_container_width=True)

    categories = {
        "Kød": "kød",
        "Mejeri": "mælk ost yoghurt",
        "Brød": "brød",
        "Frugt & Grønt": "frugt grøntsager",
        "Drikkevarer": "sodavand øl juice",
        "Frost": "frost frossen",
        "Snacks": "chips slik snacks",
    }
    chip_cols = st.columns(len(categories))
    selected_category = None
    for i, (label, search_term) in enumerate(categories.items()):
        with chip_cols[i]:
            if st.button(label, use_container_width=True, key=f"deal_cat_{label}"):
                selected_category = search_term

    search_query = None
    if search_clicked and query:
        search_query = query
    elif selected_category:
        search_query = selected_category

    # ── D. Deal cards (with smart_search) ──
    if search_query:
        with st.spinner(f"Searching for \"{search_query}\"..."):
            try:
                offers = smart_search(conn, search_query, source="dashboard", limit=20, **api_kwargs)
            except Exception as exc:
                st.error(f"API error: {exc}")
                return

        if not offers:
            st.info(f"No deals found for \"{search_query}\". Try a different search term.")
        else:
            st.subheader(f"Deals: \"{search_query}\" ({len(offers)} results)")

            for i in range(0, len(offers), 2):
                cols = st.columns(2)
                for j, col in enumerate(cols):
                    idx = i + j
                    if idx >= len(offers):
                        break
                    o = offers[idx]
                    with col:
                        discount = o["discount_pct"]
                        if discount >= 30:
                            badge_color = "green"
                        elif discount >= 15:
                            badge_color = "orange"
                        else:
                            badge_color = "gray"

                        price_str = f"{o['price']:,.2f}".replace(",", ".") if o["price"] else "?"
                        dealer = o["dealer"] or "Unknown"
                        heading = o["heading"] or "Unknown"

                        card_md = f"**{heading}**\n\n"
                        card_md += f"**{price_str} DKK** at {dealer}"

                        if o["pre_price"] and discount > 0:
                            pre_str = f"{o['pre_price']:,.2f}".replace(",", ".")
                            card_md += f"  \nWas {pre_str} DKK — **:{badge_color}[{discount:.0f}% off]**"

                        if o.get("is_all_time_low"):
                            card_md += "\n\n:red[ALL-TIME LOW!]"

                        if o["valid_from"] and o["valid_until"]:
                            try:
                                vf = datetime.fromisoformat(o["valid_from"].replace("Z", "+00:00"))
                                vt = datetime.fromisoformat(o["valid_until"].replace("Z", "+00:00"))
                                card_md += f"  \nValid: {vf.strftime('%d %b')} - {vt.strftime('%d %b')}"
                            except (ValueError, AttributeError):
                                pass

                        if o["quantity"]:
                            card_md += f"  \nQty: {o['quantity']}"

                        st.markdown(card_md)

                        # Action buttons per card
                        ac1, ac2 = st.columns(2)
                        with ac1:
                            with st.popover("Price History", use_container_width=True):
                                product_key = (heading or "").lower().strip()
                                hist = get_price_history(conn, product_key)
                                if len(hist) >= 2:
                                    hdf = pd.DataFrame(hist)
                                    hdf["date"] = pd.to_datetime(hdf["date"])
                                    fig_h = px.line(hdf, x="date", y="price", color="dealer", markers=True,
                                                    labels={"price": "DKK", "date": ""})
                                    fig_h.update_layout(height=250, margin=dict(t=5, b=5, l=5, r=5), showlegend=True)
                                    st.plotly_chart(fig_h, use_container_width=True)
                                    atl = None
                                    for h in hist:
                                        if atl is None or h["price"] < atl["price"]:
                                            atl = h
                                    if atl:
                                        st.caption(f"Lowest recorded: **{atl['price']:.2f} DKK** at {atl['dealer']} ({atl['date']})")
                                else:
                                    st.caption("Not enough data yet — prices are tracked each time you search.")
                        with ac2:
                            if st.button("Add to List", key=f"deal_add_{idx}", use_container_width=True):
                                add_to_shopping_list(conn, heading, price=o.get("price"), as_staple=True)
                                st.toast(f"Added **{heading}** to your shopping list!")
                        st.divider()

    # ── F. Smart Shopping List ──
    st.markdown("---")
    st.subheader("Smart Shopping List")

    # Show tracked items with staple toggles and remove buttons
    freq_items = get_frequent_searches(conn, 30)
    if freq_items:
        st.caption("Your tracked items — toggle staple to include in smart lists, or remove items you don't want.")
        for item in freq_items:
            col_name, col_staple, col_del = st.columns([5, 1, 1])
            with col_name:
                label = item["name"]
                if item["search_count"] > 1:
                    label += f"  ({item['search_count']}x)"
                if item["last_deal_price"]:
                    label += f"  — last {item['last_deal_price']:.0f} DKK"
                st.text(label)
            with col_staple:
                new_val = st.checkbox(
                    "Staple", value=item["is_staple"],
                    key=f"staple_{item['name']}", label_visibility="collapsed",
                )
                if new_val != item["is_staple"]:
                    set_staple(conn, item["name"], new_val)
                    st.rerun()
            with col_del:
                if st.button("X", key=f"del_{item['name']}", help=f"Remove {item['name']}"):
                    remove_shopping_item(conn, item["name"])
                    st.session_state.pop("deals_suggestions", None)
                    st.rerun()

        # Clear all button
        st.markdown("")
        if st.button("Clear All Search History", type="secondary"):
            st.session_state["confirm_clear_history"] = True

        if st.session_state.get("confirm_clear_history"):
            st.warning("This will remove all tracked items, search history, and price history. Are you sure?")
            cc1, cc2, _ = st.columns([1, 1, 3])
            with cc1:
                if st.button("Yes, clear everything", type="primary"):
                    clear_all_shopping_data(conn)
                    st.session_state.pop("confirm_clear_history", None)
                    st.session_state.pop("deals_suggestions", None)
                    st.rerun()
            with cc2:
                if st.button("Cancel"):
                    st.session_state.pop("confirm_clear_history", None)
                    st.rerun()

    if st.button("Generate Smart List", type="primary", key="deals_smart_list"):
        with st.spinner("Building your personalized shopping list..."):
            try:
                result = generate_smart_list(conn, **api_kwargs)
            except Exception as exc:
                st.error(f"Error: {exc}")
                result = None

        if result and result["items"]:
            rows = []
            for item in result["items"]:
                o = item.get("best_offer")
                if o:
                    rows.append({
                        "Item": item["name"].title(),
                        "Best Deal": o.get("heading", ""),
                        "Price": f"{o['price']:,.2f} DKK".replace(",", ".") if o.get("price") else "?",
                        "Store": o.get("dealer", ""),
                        "Reason": item["reason"],
                    })
                else:
                    rows.append({
                        "Item": item["name"].title(),
                        "Best Deal": "No deals found",
                        "Price": "-",
                        "Store": "-",
                        "Reason": item["reason"],
                    })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

            mc1, mc2, mc3 = st.columns(3)
            mc1.metric("Estimated Total", f"{result['estimated_total']:,.0f} DKK")
            mc2.metric("Budget Remaining", f"{result['budget_remaining']:,.0f} DKK")
            mc3.metric("Savings vs Regular", f"{result['savings_vs_regular']:,.0f} DKK")
        elif result:
            st.info("No items to generate a list from. Search for some items first, or mark staples above.")

    # ── G. Shopping Habits ──
    with st.expander("Shopping Habits"):
        habits = analyze_grocery_habits(conn)
        if habits["top_stores"]:
            hc1, hc2 = st.columns(2)
            with hc1:
                stores_df = pd.DataFrame(habits["top_stores"][:8])
                fig_stores = px.pie(
                    stores_df, names="name", values="total",
                    title="Top Stores by Spending",
                )
                fig_stores.update_layout(height=350, margin=dict(t=40, b=10))
                st.plotly_chart(fig_stores, use_container_width=True)
            with hc2:
                if habits["shopping_days"]:
                    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
                    days_data = [{"day": d, "trips": habits["shopping_days"].get(d, 0)} for d in day_order]
                    fig_days = px.bar(
                        pd.DataFrame(days_data), x="day", y="trips",
                        title="Shopping Trips by Day",
                    )
                    fig_days.update_layout(height=350, margin=dict(t=40, b=10))
                    st.plotly_chart(fig_days, use_container_width=True)

            st.metric("Average Trip Amount", f"{habits['avg_trip_amount']:,.0f} DKK")
            st.metric("Average Weekly Spend", f"{habits['avg_weekly_spend']:,.0f} DKK")
        else:
            st.info("No grocery transactions found to analyze.")


def render_settings(df, budgets_df, conn):
    st.header("Settings")
    
    st.subheader("Budget Targets (DKK/month)")
    st.info("Set your monthly spending limits here. These limits drive your 'Budget vs Actual' charts and gamification XP.")

    # Get all categories that have expenses
    expense_cats = sorted(df[df["amount"] < 0]["category"].unique())
    budget_map = dict(zip(budgets_df["category"], budgets_df["monthly_limit"])) if not budgets_df.empty else {}

    # ── Suggest from Data ──
    with st.expander("Suggest budgets from spending data"):
        six_months_ago = (pd.Timestamp.now() - pd.DateOffset(months=6)).strftime("%Y-%m-%d")
        recent = df[(df["amount"] < 0) & (df["date"] >= six_months_ago)].copy()
        if recent.empty:
            st.warning("No expense data in the last 6 months.")
        else:
            recent["abs_amount"] = recent["amount"].abs()
            monthly_avg = (
                recent.groupby("category")["abs_amount"]
                .sum()
                .div(max(recent["date"].dt.to_period("M").nunique(), 1))
            )
            suggestion_rows = []
            for cat in expense_cats:
                avg = monthly_avg.get(cat, 0.0)
                suggested = round(avg / 100) * 100  # round to nearest 100
                current_b = budget_map.get(cat, 0.0)
                suggestion_rows.append({
                    "Category": cat,
                    "Current Budget": f"{current_b:,.0f}",
                    "6-mo Avg Spend": f"{avg:,.0f}",
                    "Suggested": f"{suggested:,.0f}",
                })
            suggest_df = pd.DataFrame(suggestion_rows)
            st.dataframe(suggest_df, use_container_width=True, hide_index=True)
            total_suggested = sum(round(monthly_avg.get(c, 0.0) / 100) * 100 for c in expense_cats)
            st.caption(f"Total suggested: **{total_suggested:,.0f} DKK/mo**")

            if st.button("Apply suggestions", type="primary", key="apply_budget_suggestions"):
                for cat in expense_cats:
                    avg = monthly_avg.get(cat, 0.0)
                    suggested = round(avg / 100) * 100
                    if suggested > 0:
                        save_budget(conn, cat, float(suggested))
                st.success("Budget suggestions applied!")
                st.rerun()

    # Grid layout for budget inputs
    cols = st.columns(3)
    for i, cat in enumerate(expense_cats):
        with cols[i % 3]:
            current = budget_map.get(cat, 0.0)
            new_val = st.number_input(
                cat,
                min_value=0.0,
                value=current,
                step=100.0,
                key=f"setting_budget_{cat}"
            )
            if new_val != current:
                save_budget(conn, cat, new_val)

    total_budget = sum(budget_map.get(cat, 0.0) for cat in expense_cats)
    st.markdown(f"**Total monthly budget: {total_budget:,.0f} DKK**")

    # ── Auto-Categorize ──
    st.markdown("---")
    st.subheader("Auto-Categorize Transactions")

    # Count uncategorized
    uncat_count = conn.execute(
        "SELECT COUNT(*) FROM transactions WHERE category = 'Ukategoriseret'"
    ).fetchone()[0]

    st.write(f"**{uncat_count}** uncategorized transactions found.")

    if uncat_count > 0:
        # Preview
        preview = auto_categorize(conn, dry_run=True)
        if preview:
            st.success(f"**{len(preview)}** transactions can be auto-categorized.")
            with st.expander(f"Preview changes ({len(preview)} transactions)"):
                preview_df = pd.DataFrame(
                    [(desc, cat, subcat) for _, desc, cat, subcat in preview],
                    columns=["Description", "New Category", "New Subcategory"],
                )
                st.dataframe(preview_df, use_container_width=True, hide_index=True)

            if st.button("Apply Auto-Categorize", type="primary"):
                count = auto_categorize(conn)
                st.success(f"Re-categorized {count} transactions!")
                st.rerun()
        else:
            st.info("No matching rules found for remaining uncategorized transactions.")
    else:
        st.success("All transactions are categorized!")

    # ── Google Calendar Integration ──
    st.markdown("---")
    st.subheader("Google Calendar Integration")
    st.markdown(
        "Connect your Google Calendar to automatically pull work shifts into the Shift Planner.\n\n"
        "**How to get your ICS URL:**\n"
        "1. Open [Google Calendar Settings](https://calendar.google.com/calendar/r/settings)\n"
        "2. Click the calendar that has your shifts\n"
        "3. Scroll to **\"Secret address in iCal format\"**\n"
        "4. Copy the URL and paste it below"
    )

    current_ics = get_setting(conn, "gcal_ics_url", "")
    new_ics = st.text_input(
        "Private ICS URL",
        value=current_ics,
        type="password",
        key="setting_gcal_ics",
        help="This URL is stored locally in budget.db (gitignored). Never shared.",
    )
    if new_ics != current_ics:
        set_setting(conn, "gcal_ics_url", new_ics)
        # Clear cache so next page load fetches fresh
        st.session_state.pop("gcal_shifts_cache", None)
        st.session_state.pop("gcal_shifts_ts", None)
        st.success("ICS URL saved!")

    current_keyword = get_setting(conn, "gcal_shift_keyword", "Working time")
    new_keyword = st.text_input(
        "Shift keyword filter",
        value=current_keyword,
        key="setting_gcal_keyword",
        help="Only calendar events whose title contains this keyword will be treated as shifts.",
    )
    if new_keyword != current_keyword:
        set_setting(conn, "gcal_shift_keyword", new_keyword)
        st.session_state.pop("gcal_shifts_cache", None)
        st.session_state.pop("gcal_shifts_ts", None)

    # Test connection
    if new_ics:
        if st.button("Test Calendar Connection", key="test_gcal"):
            with st.spinner("Fetching calendar..."):
                st.session_state.pop("gcal_shifts_cache", None)
                st.session_state.pop("gcal_shifts_ts", None)
                test_df = fetch_shifts(conn)
            if test_df.empty:
                st.warning(f"Connected, but no events found matching \"{new_keyword}\".")
                # Show diagnostics: re-parse the cached ICS response
                try:
                    resp = requests.get(new_ics, timeout=15)
                    resp.raise_for_status()
                    cal = iCalendar.from_ical(resp.content)
                    summaries = {}
                    for comp in cal.walk():
                        if comp.name == "VEVENT":
                            s = str(comp.get("summary", ""))
                            if s:
                                summaries[s] = repr(s)
                    if summaries:
                        st.info(f"**Event names found in your calendar ({len(summaries)}):**")
                        for s, r in sorted(summaries.items())[:25]:
                            st.code(f"{s}  →  {r}", language=None)
                        st.caption("Update the keyword above to match one of these event names.")
                    else:
                        st.error("No events found in the calendar at all. Is this the right URL?")
                except Exception as e:
                    st.error(f"Could not re-fetch for diagnostics: {e}")
            else:
                st.success(f"Found {len(test_df)} shifts! Earliest: {test_df['date'].min().strftime('%d %b %Y')}, Latest: {test_df['date'].max().strftime('%d %b %Y')}")

    # ── Telegram Bot ──
    st.markdown("---")
    st.subheader("Telegram Bot")
    st.markdown(
        "Get daily budget alerts and spending summaries on Telegram.\n\n"
        "**Setup:**\n"
        "1. Message [@BotFather](https://t.me/BotFather) on Telegram → `/newbot`\n"
        "2. Copy the bot token and paste below\n"
        "3. Run `python telegram_bot.py` to start the bot\n"
        "4. Message your bot `/start` to register"
    )

    current_token = get_setting(conn, "telegram_bot_token", "")
    new_token = st.text_input(
        "Bot Token",
        value=current_token,
        type="password",
        key="setting_telegram_token",
        help="Get this from @BotFather on Telegram",
    )
    if new_token != current_token:
        set_setting(conn, "telegram_bot_token", new_token)
        st.success("Telegram bot token saved!")

    current_chat_id = get_setting(conn, "telegram_chat_id", "")
    if current_chat_id:
        st.success(f"Chat registered (ID: {current_chat_id})")
    else:
        st.info("No chat registered yet. Start the bot and send `/start`.")

    # ── AI Insights ──
    st.markdown("---")
    st.subheader("AI Insights (Claude API)")
    st.markdown("Power the AI Financial Advisor with your Anthropic API key.")

    import os
    current_ai_key = get_setting(conn, "anthropic_api_key", "")
    new_ai_key = st.text_input(
        "Anthropic API Key",
        value=current_ai_key,
        type="password",
        key="setting_anthropic_key",
        help="Get your key from console.anthropic.com",
    )
    if new_ai_key != current_ai_key:
        set_setting(conn, "anthropic_api_key", new_ai_key)
        os.environ["ANTHROPIC_API_KEY"] = new_ai_key
        st.success("API key saved!")

    # ── eTilbudsavis (Grocery Deals) ──
    st.markdown("---")
    st.subheader("Grocery Deals (eTilbudsavis)")
    st.markdown(
        "Search for grocery deals near you via the eTilbudsavis API. "
        "No API key required — works out of the box."
    )

    # Location settings
    loc_col1, loc_col2, loc_col3 = st.columns(3)
    with loc_col1:
        current_lat = get_setting(conn, "deals_lat", "55.786")
        new_lat = st.text_input("Latitude", value=current_lat, key="setting_deals_lat")
        if new_lat != current_lat:
            set_setting(conn, "deals_lat", new_lat)
    with loc_col2:
        current_lng = get_setting(conn, "deals_lng", "12.524")
        new_lng = st.text_input("Longitude", value=current_lng, key="setting_deals_lng")
        if new_lng != current_lng:
            set_setting(conn, "deals_lng", new_lng)
    with loc_col3:
        current_radius = int(get_setting(conn, "deals_radius", "10000"))
        new_radius = st.number_input(
            "Radius (meters)", value=current_radius, step=1000,
            min_value=1000, max_value=50000, key="setting_deals_radius",
        )
        if new_radius != current_radius:
            set_setting(conn, "deals_radius", str(new_radius))

    st.caption("Default location: DTU/Lyngby (55.786, 12.524), 10 km radius")

    # Test connection
    if DEALS_AVAILABLE:
        if st.button("Test eTilbudsavis Connection", key="test_deals"):
            with st.spinner("Testing API connection..."):
                lat_val = float(get_setting(conn, "deals_lat", "55.786"))
                lng_val = float(get_setting(conn, "deals_lng", "12.524"))
                success, msg = deals_test(lat=lat_val, lng=lng_val)
            if success:
                st.success(msg)
            else:
                st.error(msg)

    st.markdown("---")
    st.caption("More settings (like tax rates and loan details) can be found on their respective pages.")


if __name__ == "__main__":
    main()
