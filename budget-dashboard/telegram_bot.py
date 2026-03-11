#!/usr/bin/env python3
"""Telegram bot — interactive financial companion.

Reads from the shared SQLite budget.db and provides:
- Interactive menus with inline keyboards
- Quick expense logging with auto-categorization
- AI-powered financial advice via Claude
- Snus accountability tracking
- Savings goals management
- XP/level gamification
- Smart alerts and morning briefings
- Shift schedule from Google Calendar

Usage:
    python telegram_bot.py

Bot token is read from the settings table (key: telegram_bot_token)
or from the TELEGRAM_BOT_TOKEN environment variable.
"""

import hashlib
import logging
import os
import re
import sqlite3
import time as _time
from datetime import date, datetime, timedelta
from pathlib import Path
from textwrap import dedent

import pandas as pd
import requests
from icalendar import Calendar as iCalendar
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ChatAction, ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# Import AI insights (no Streamlit dependency)
try:
    from ai_insights import get_financial_insights, prepare_financial_summary
    AI_AVAILABLE = True
except ImportError:
    AI_AVAILABLE = False

# Import deals module
try:
    from deals import (
        search_offers as deals_search, get_cheapest as deals_cheapest,
        format_offer_text, smart_search, record_search, record_prices,
        get_frequent_searches, generate_smart_list, find_price_drops,
        get_grocery_budget_status,
    )
    DEALS_AVAILABLE = True
except ImportError:
    DEALS_AVAILABLE = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_PATH = Path(__file__).parent / "budget.db"

logging.basicConfig(
    format="%(asctime)s [%(name)s] %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Scheduled job times (24-h, local timezone)
MORNING_BRIEF_HOUR = 8
MORNING_BRIEF_MINUTE = 0
SNUS_CHECKIN_HOUR = 20
SNUS_CHECKIN_MINUTE = 0
DAILY_ALERT_HOUR = 21
DAILY_ALERT_MINUTE = 0
WEEKLY_DIGEST_HOUR = 20
WEEKLY_DIGEST_MINUTE = 0
WEEKLY_DIGEST_DAY = 6  # 0=Mon ... 6=Sun
PRICE_DROP_HOUR = 10
PRICE_DROP_MINUTE = 0

# Bank sync
BANK_SYNC_HOUR = 3
BANK_SYNC_MINUTE = 0

# Snus cost estimate
SNUS_DAILY_COST = 65  # DKK/day

# Pay settings defaults
DEFAULT_HOURLY_RATE = 185
DEFAULT_SHIFT_HOURS = 12
DEFAULT_OT_AFTER = 8

# Undo window
UNDO_WINDOW_SECONDS = 300  # 5 minutes

# ---------------------------------------------------------------------------
# Category rules (copied from dashboard.py — can't import due to Streamlit)
# ---------------------------------------------------------------------------

CATEGORY_RULES = [
    (r"P\.?O\.?\s*Pedersen|Kollegiet", "Bolig", "Husleje"),
    (r"Norlys", "Bolig", "Forsyning"),
    (r"Rejsekort|DSB|MOB\.PAY\*DSB", "Transport", "Offentlig transport"),
    (r"Coop|REMA|F[\u00d8@]TEX|F\u00f8tex|Netto|Lidl|ALDI|Fakta|Meny|Irma|COOP365|Bilka|Spar\b",
     "Dagligvarer", "Supermarked"),
    (r"McDonalds|Burger King|Max Burgers|Sunset Boulevard", "Dagligvarer", "Fast food"),
    (r"Wolt|Just.?Eat|Hungry", "Forn\u00f8jelser og fritid", "Takeaway"),
    (r"Kaffestuen|Starbucks|Espresso|Cafe|Caf\u00e9", "Forn\u00f8jelser og fritid", "Caf\u00e9"),
    (r"Durumbar|Kebap|Shawarma|Pizza|Pakhus", "Forn\u00f8jelser og fritid", "Restaurant"),
    (r"WINTER|Bar |Bodega|Pub\b", "Forn\u00f8jelser og fritid", "Bar"),
    (r"TIDAL|Spotify|Netflix|Disney|HBO|YouTube|Viaplay",
     "\u00d8vrige udgifter", "Streaming"),
    (r"Microsoft|OPENAI|CLAUDE|ANTHROPIC|Google\s*\*?Google\s*One|Google\s*Play",
     "\u00d8vrige udgifter", "Abonnement"),
    (r"Carlsen|Barbershop|Fris\u00f8r|fris\u00f8r",
     "T\u00f8j, sko og personlig pleje", "Fris\u00f8r"),
    (r"Matas|Apotek|Normal\b|N.rrebro Apotek",
     "T\u00f8j, sko og personlig pleje", "Personlig pleje"),
    (r"temashop|Brinck Elektronik|ESSENCEVAULT", "\u00d8vrige udgifter", "Shopping"),
    (r"forsikri|Tryg\b", "Personforsikringer", "Forsikring"),
    (r"Kr.ftens Bek.mpelse|Red Barnet|UNICEF", "\u00d8vrige udgifter", "Donation"),
    (r"Til Opsparing|Fra Opsparing", "Opsparing", "Overf\u00f8rsel"),
    (r"^MobilePay\s+[A-Z\u00c6\u00d8\u00c5][a-z\u00e6\u00f8\u00e5]+\s+[A-Z\u00c6\u00d8\u00c5]",
     "\u00d8vrige udgifter", "MobilePay overf\u00f8rsel"),
]

# ---------------------------------------------------------------------------
# Gamification (copied from dashboard.py)
# ---------------------------------------------------------------------------

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

XP_PER_LEVEL = 500

BUDGET_START = "2026-02"  # XP only counted from this month onward


def compute_xp(df, budgets_df):
    """Compute XP from under-budget performance."""
    if df.empty or budgets_df.empty:
        return 0

    budget_col = "monthly_limit" if "monthly_limit" in budgets_df.columns else "budget"
    budget_map = dict(zip(budgets_df["category"], budgets_df[budget_col]))
    if not budget_map or all(v == 0 for v in budget_map.values()):
        return 0

    expenses = df[(df["amount"] < 0) & (df["month"] >= BUDGET_START)].copy()
    if expenses.empty:
        return 0

    xp = 0
    total_monthly_budget = sum(v for v in budget_map.values() if v > 0)
    daily_budget = total_monthly_budget / 30.0
    weekly_budget = daily_budget * 7

    # Monthly XP: +200 per month where ALL categories under budget
    for _month, grp in expenses.groupby("month"):
        month_spend = grp.groupby("category")["amount"].sum().abs()
        all_under = all(
            month_spend.get(cat, 0) <= limit
            for cat, limit in budget_map.items() if limit > 0
        )
        if all_under:
            xp += 200

    # Weekly XP: +50 per week under total weekly budget
    expenses_idx = expenses.set_index("date")
    for _week_start, grp in expenses_idx.resample("W"):
        week_spend = abs(grp["amount"].sum())
        if 0 < week_spend < weekly_budget:
            xp += 50

    # Daily XP: +10 per day under daily budget
    daily_spend = expenses.groupby(expenses["date"].dt.date)["amount"].sum().abs()
    for _day, spend in daily_spend.items():
        if spend < daily_budget:
            xp += 10

    return xp


def get_level(xp):
    return int(min(10, max(1, xp // XP_PER_LEVEL + 1)))


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _get_conn() -> sqlite3.Connection:
    """Open a new connection (safe for use in async callbacks)."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _get_setting(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    row = conn.execute(
        "SELECT value FROM settings WHERE key = ?", (key,)
    ).fetchone()
    return row["value"] if row else default


def _set_setting(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
        (key, str(value)),
    )
    conn.commit()


def _init_snus_table(conn: sqlite3.Connection) -> None:
    """Create the snus_checkins table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS snus_checkins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL,
            note TEXT
        )
    """)
    conn.commit()


def _fmt(amount: float) -> str:
    """Format a DKK amount: 1.234 DKK or 1.234,50 DKK."""
    if amount == int(amount):
        return f"{int(amount):,} DKK".replace(",", ".")
    return f"{amount:,.2f} DKK".replace(",", "X").replace(".", ",").replace("X", ".")


def _fmt_signed(amount: float) -> str:
    sign = "+" if amount >= 0 else ""
    return f"{sign}{_fmt(amount)}"


def _progress_bar(pct: float, width: int = 10) -> str:
    """Text progress bar: [######....] 60%."""
    filled = int(round(pct / 100 * width))
    filled = max(0, min(width, filled))
    empty = width - filled
    return f"[{'█' * filled}{'░' * empty}] {pct:.0f}%"


# ---------------------------------------------------------------------------
# Data loading helpers
# ---------------------------------------------------------------------------

def _load_transactions_df(conn: sqlite3.Connection) -> pd.DataFrame:
    """Load transactions into a DataFrame with month column."""
    df = pd.read_sql("SELECT * FROM transactions ORDER BY date", conn)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
        df["month"] = df["date"].dt.to_period("M").astype(str)
    return df


def _load_budgets_df(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM budgets", conn)


def _load_goals(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM savings_goals ORDER BY created"
    ).fetchall()
    return [dict(r) for r in rows]


def _save_goal(conn: sqlite3.Connection, name: str, target: float,
               deadline: str | None = None) -> int:
    cur = conn.execute(
        "INSERT INTO savings_goals (name, target, deadline) VALUES (?, ?, ?)",
        (name, target, deadline),
    )
    conn.commit()
    return cur.lastrowid


def _update_goal_progress(conn: sqlite3.Connection, goal_id: int, saved: float) -> None:
    conn.execute("UPDATE savings_goals SET saved = ? WHERE id = ?", (saved, goal_id))
    conn.commit()


# ---------------------------------------------------------------------------
# Category matching
# ---------------------------------------------------------------------------

def _match_category(desc: str) -> tuple[str, str]:
    """Match a description against CATEGORY_RULES. Returns (category, subcategory)."""
    desc_stripped = re.sub(r"^MobilePay\s+", "", desc)
    for pattern, cat, subcat in CATEGORY_RULES:
        if (re.search(pattern, desc, re.IGNORECASE)
                or re.search(pattern, desc_stripped, re.IGNORECASE)):
            return cat, subcat
    return "Ukategoriseret", ""


def _insert_manual_tx(conn: sqlite3.Connection, desc: str, amount: float,
                      cat: str, subcat: str) -> int:
    """Insert a manual transaction. Returns the new row ID."""
    today = datetime.now().strftime("%Y-%m-%d")
    bal_row = conn.execute(
        "SELECT balance FROM transactions ORDER BY date DESC, id DESC LIMIT 1"
    ).fetchone()
    balance = (bal_row["balance"] + amount) if bal_row else amount

    h = hashlib.sha256(f"{today}|{amount}|{desc}#telegram".encode()).hexdigest()
    cur = conn.execute(
        "INSERT INTO transactions (date, category, subcategory, description, "
        "amount, balance, import_hash) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (today, cat, subcat, desc, amount, balance, h),
    )
    conn.commit()
    return cur.lastrowid


# ---------------------------------------------------------------------------
# Snus tracking
# ---------------------------------------------------------------------------

def _snus_streak(conn: sqlite3.Connection) -> dict:
    """Calculate snus streak info."""
    rows = conn.execute(
        "SELECT date, status FROM snus_checkins ORDER BY date DESC"
    ).fetchall()

    if not rows:
        return {"current": 0, "longest": 0, "total_clean": 0, "saved": 0.0}

    # Current streak
    current = 0
    for r in rows:
        if r["status"] == "clean":
            current += 1
        else:
            break

    # Longest streak and total clean
    longest = 0
    streak = 0
    total_clean = 0
    # Process chronologically
    for r in reversed(rows):
        if r["status"] == "clean":
            streak += 1
            total_clean += 1
            longest = max(longest, streak)
        else:
            streak = 0

    saved = total_clean * SNUS_DAILY_COST

    return {
        "current": current,
        "longest": longest,
        "total_clean": total_clean,
        "saved": saved,
    }


def _snus_motivation(streak: int) -> str:
    """Return motivational message based on streak length."""
    if streak == 0:
        return "Every journey starts with day one."
    elif streak < 3:
        return f"{streak} day{'s' if streak > 1 else ''} strong! Keep it up."
    elif streak < 7:
        return f"{streak} days strong! You've saved {_fmt(streak * SNUS_DAILY_COST)}."
    elif streak < 14:
        return "One week+ clean! Cravings get easier from here."
    elif streak < 30:
        return f"{streak} days! The habit is breaking. Saved {_fmt(streak * SNUS_DAILY_COST)}."
    else:
        monthly_save = SNUS_DAILY_COST * 30
        return (
            f"A whole month+! Saving ~{_fmt(monthly_save)}/mo "
            f"- that's one fewer SU loan payment."
        )


# ---------------------------------------------------------------------------
# Shifts (standalone, no Streamlit)
# ---------------------------------------------------------------------------

# In-memory shift cache
_shift_cache: dict = {"data": None, "ts": 0}
SHIFT_CACHE_TTL = 900  # 15 min


def fetch_shifts_standalone(conn: sqlite3.Connection) -> pd.DataFrame:
    """Fetch work shifts from Google Calendar ICS (no Streamlit caching)."""
    global _shift_cache

    if (_shift_cache["data"] is not None
            and _time.time() - _shift_cache["ts"] < SHIFT_CACHE_TTL):
        return _shift_cache["data"]

    ics_url = _get_setting(conn, "gcal_ics_url", "")
    keyword = _get_setting(conn, "gcal_shift_keyword", "Working time")
    if not ics_url:
        return pd.DataFrame()

    try:
        resp = requests.get(ics_url, timeout=15)
        resp.raise_for_status()
    except Exception:
        cached = _shift_cache["data"]
        return cached if cached is not None else pd.DataFrame()

    try:
        cal = iCalendar.from_ical(resp.content)
    except Exception:
        cached = _shift_cache["data"]
        return cached if cached is not None else pd.DataFrame()

    hourly_rate = float(_get_setting(conn, "hourly_rate", str(DEFAULT_HOURLY_RATE)))
    shift_hours_setting = int(float(_get_setting(conn, "shift_hours", str(DEFAULT_SHIFT_HOURS))))
    ot_after = int(float(_get_setting(conn, "ot_after", str(DEFAULT_OT_AFTER))))

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

        if isinstance(dt_start, date) and not isinstance(dt_start, datetime):
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

    _shift_cache["data"] = df
    _shift_cache["ts"] = _time.time()
    return df


# ---------------------------------------------------------------------------
# Smart alerts (adapted from dashboard.py generate_alerts)
# ---------------------------------------------------------------------------

def _generate_alerts(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """Generate smart financial alerts. Returns list of (severity, message)."""
    df = _load_transactions_df(conn)
    budgets_df = _load_budgets_df(conn)

    budget_col = "monthly_limit" if "monthly_limit" in budgets_df.columns else "budget"
    budget_map = (
        dict(zip(budgets_df["category"], budgets_df[budget_col]))
        if not budgets_df.empty else {}
    )

    alerts: list[tuple[str, str]] = []

    if not budget_map or all(v == 0 for v in budget_map.values()):
        return [("info", "Set budget targets in Settings to enable smart alerts.")]

    current_month_str = datetime.now().strftime("%Y-%m")
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
            alerts.append((
                "error",
                f"*{cat}* is OVER budget by {over_by:,.0f} DKK "
                f"({pct:.0%} of {limit:,.0f})"
            ))
            has_alert = True
        elif pct > 0.6 and month_progress < 0.6:
            alerts.append((
                "warning",
                f"*{cat}*: {spent:,.0f}/{limit:,.0f} DKK spent "
                f"({pct:.0%}) with {days_left} days left"
            ))
            has_alert = True

    # Unusual day: yesterday vs daily average
    yesterday = today - timedelta(days=1)
    all_expenses = df[df["amount"] < 0]
    daily_avg = (
        all_expenses.groupby(all_expenses["date"].dt.date)["amount"]
        .sum().abs().mean()
    )
    yesterday_spend = all_expenses[
        all_expenses["date"].dt.date == yesterday
    ]["amount"].sum()
    yesterday_abs = abs(yesterday_spend)
    if daily_avg > 0 and yesterday_abs > daily_avg * 2.5:
        ratio = yesterday_abs / daily_avg
        alerts.append((
            "warning",
            f"Yesterday you spent {yesterday_abs:,.0f} DKK "
            f"- {ratio:.1f}x your daily average"
        ))
        has_alert = True

    if not has_alert:
        alerts.append(("success", "All categories on track this month!"))

    severity_order = {"error": 0, "warning": 1, "info": 2, "success": 3}
    alerts.sort(key=lambda x: severity_order.get(x[0], 9))
    return alerts[:5]


# ---------------------------------------------------------------------------
# Upcoming subscriptions predictor
# ---------------------------------------------------------------------------

def _upcoming_subscriptions(conn: sqlite3.Connection, days_ahead: int = 7) -> list[dict]:
    """Predict upcoming subscription charges based on last occurrence."""
    today = date.today()
    future = today + timedelta(days=days_ahead)

    # Known subscription patterns
    sub_patterns = [
        "Spotify", "Netflix", "Disney", "HBO", "YouTube", "Viaplay",
        "TIDAL", "Microsoft", "OPENAI", "Google.*One",
    ]
    # Fetch all expense transactions, filter in Python
    all_tx = conn.execute(
        "SELECT description, amount, date FROM transactions WHERE amount < 0 "
        "ORDER BY date DESC"
    ).fetchall()

    # Group by description, find subscription-like ones
    subs: dict[str, list] = {}
    for r in all_tx:
        desc = r["description"]
        for sp in sub_patterns:
            if re.search(sp, desc, re.IGNORECASE):
                subs.setdefault(desc, []).append({
                    "amount": r["amount"],
                    "date": r["date"],
                })
                break

    results = []
    for desc, occurrences in subs.items():
        if len(occurrences) < 2:
            continue
        # Estimate next date: last occurrence + average interval
        dates = sorted([datetime.strptime(o["date"][:10], "%Y-%m-%d").date()
                       for o in occurrences], reverse=True)
        if len(dates) >= 2:
            avg_interval = (dates[0] - dates[-1]).days / (len(dates) - 1)
            if 25 <= avg_interval <= 35:  # Monthly
                next_date = dates[0] + timedelta(days=round(avg_interval))
                if today <= next_date <= future:
                    results.append({
                        "description": desc,
                        "amount": abs(occurrences[0]["amount"]),
                        "expected_date": next_date,
                        "days_until": (next_date - today).days,
                    })

    results.sort(key=lambda x: x["expected_date"])
    return results


# ---------------------------------------------------------------------------
# Message helpers
# ---------------------------------------------------------------------------

async def _send_long_message(bot, chat_id: int, text: str,
                             parse_mode: str = "Markdown") -> None:
    """Split and send messages longer than 4096 chars on paragraph boundaries."""
    MAX_LEN = 4096
    if len(text) <= MAX_LEN:
        await bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode)
        return

    chunks = []
    current = ""
    for paragraph in text.split("\n\n"):
        if len(current) + len(paragraph) + 2 > MAX_LEN:
            if current:
                chunks.append(current)
            # If a single paragraph exceeds limit, split on lines
            if len(paragraph) > MAX_LEN:
                lines = paragraph.split("\n")
                sub = ""
                for line in lines:
                    if len(sub) + len(line) + 1 > MAX_LEN:
                        chunks.append(sub)
                        sub = line
                    else:
                        sub = f"{sub}\n{line}" if sub else line
                if sub:
                    current = sub
            else:
                current = paragraph
        else:
            current = f"{current}\n\n{paragraph}" if current else paragraph
    if current:
        chunks.append(current)

    for chunk in chunks:
        try:
            await bot.send_message(
                chat_id=chat_id, text=chunk, parse_mode=parse_mode
            )
        except Exception:
            # Fall back to plain text if markdown fails
            await bot.send_message(chat_id=chat_id, text=chunk)


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def _today_summary(conn: sqlite3.Connection) -> dict:
    today = datetime.now().strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT category, amount FROM transactions WHERE date = ?", (today,)
    ).fetchall()

    expenses = [r for r in rows if r["amount"] < 0]
    income = [r for r in rows if r["amount"] > 0]

    by_category: dict[str, float] = {}
    for r in expenses:
        by_category[r["category"]] = by_category.get(r["category"], 0) + abs(r["amount"])

    return {
        "date": today,
        "total_spent": sum(abs(r["amount"]) for r in expenses),
        "total_income": sum(r["amount"] for r in income),
        "tx_count": len(rows),
        "expense_count": len(expenses),
        "by_category": dict(sorted(by_category.items(), key=lambda x: -x[1])),
    }


def _week_summary(conn: sqlite3.Connection) -> dict:
    end = datetime.now()
    start = end - timedelta(days=6)
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")

    rows = conn.execute(
        "SELECT category, amount, date FROM transactions "
        "WHERE date BETWEEN ? AND ? ORDER BY date",
        (start_str, end_str),
    ).fetchall()

    expenses = [r for r in rows if r["amount"] < 0]

    by_category: dict[str, float] = {}
    by_date: dict[str, float] = {}
    for r in expenses:
        cat = r["category"]
        by_category[cat] = by_category.get(cat, 0) + abs(r["amount"])
        d = r["date"]
        by_date[d] = by_date.get(d, 0) + abs(r["amount"])

    total_spent = sum(abs(r["amount"]) for r in expenses)

    return {
        "start": start_str,
        "end": end_str,
        "total_spent": total_spent,
        "daily_avg": total_spent / 7,
        "tx_count": len(expenses),
        "top_categories": dict(
            sorted(by_category.items(), key=lambda x: -x[1])[:5]
        ),
    }


def _budget_status(conn: sqlite3.Connection) -> list[dict]:
    now = datetime.now()
    month_start = now.strftime("%Y-%m-01")
    month_end = now.strftime("%Y-%m-31")

    budgets = conn.execute("SELECT category, monthly_limit FROM budgets").fetchall()
    if not budgets:
        return []

    results = []
    for b in budgets:
        cat = b["category"]
        limit_val = b["monthly_limit"]

        row = conn.execute(
            "SELECT COALESCE(SUM(ABS(amount)), 0) as spent "
            "FROM transactions WHERE category = ? AND amount < 0 "
            "AND date BETWEEN ? AND ?",
            (cat, month_start, month_end),
        ).fetchone()
        spent = row["spent"]
        pct = (spent / limit_val * 100) if limit_val > 0 else 0

        if pct >= 100:
            emoji = "\U0001F6A8"
        elif pct >= 80:
            emoji = "\u26A0\uFE0F"
        elif pct >= 50:
            emoji = "\U0001F7E1"
        else:
            emoji = "\u2705"

        results.append({
            "category": cat,
            "limit": limit_val,
            "spent": spent,
            "remaining": limit_val - spent,
            "pct": pct,
            "emoji": emoji,
        })

    results.sort(key=lambda x: -x["pct"])
    return results


def _latest_balance(conn: sqlite3.Connection) -> float | None:
    row = conn.execute(
        "SELECT balance FROM transactions ORDER BY date DESC, id DESC LIMIT 1"
    ).fetchone()
    return row["balance"] if row else None


def _upcoming_bills_amount(conn: sqlite3.Connection) -> float:
    """Calculate upcoming recurring bill amount for the rest of this month.

    Uses (MAX-MIN)/AVG as a consistency proxy since SQLite lacks STDEV.
    A ratio < 0.7 roughly corresponds to CV < 0.35.
    """
    today_day = date.today().day
    rows = conn.execute("""
        WITH expense_stats AS (
            SELECT description, category,
                   COUNT(DISTINCT strftime('%Y-%m', date)) as months,
                   AVG(ABS(amount)) as avg_amount,
                   MAX(ABS(amount)) as max_amount,
                   MIN(ABS(amount)) as min_amount,
                   AVG(CAST(strftime('%d', date) AS INTEGER)) as avg_day,
                   COUNT(*) as cnt
            FROM transactions
            WHERE amount < 0
            GROUP BY description
        )
        SELECT description, avg_amount, avg_day FROM expense_stats
        WHERE months >= 3
          AND ((max_amount - min_amount) /
               CASE WHEN avg_amount > 0 THEN avg_amount ELSE 1 END) < 0.7
          AND (CAST(cnt AS REAL) / months) < 2.5
          AND category NOT IN ('Dagligvarer', 'Fornøjelser og fritid', 'Snus')
          AND avg_day > ?
    """, (today_day,)).fetchall()
    return sum(r["avg_amount"] for r in rows)


def _balance_aware_status(conn: sqlite3.Connection) -> dict:
    """Calculate balance-aware budget info.

    Returns dict with keys: balance, upcoming_bills, available, daily_allowance,
    is_constrained, adjusted (dict of category -> adjusted remaining).
    """
    bal = _latest_balance(conn) or 0
    upcoming = _upcoming_bills_amount(conn)
    available = bal - upcoming

    statuses = _budget_status(conn)
    total_static_remaining = sum(max(b["remaining"], 0) for b in statuses)

    today_d = date.today()
    if today_d.month == 12:
        last_day = date(today_d.year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(today_d.year, today_d.month + 1, 1) - timedelta(days=1)
    days_left = max((last_day - today_d).days, 1)

    is_constrained = available < total_static_remaining

    adjusted = {}
    if is_constrained and total_static_remaining > 0 and available > 0:
        scale = available / total_static_remaining
        for b in statuses:
            adjusted[b["category"]] = max(b["remaining"], 0) * scale
    elif available <= 0:
        for b in statuses:
            adjusted[b["category"]] = 0
    else:
        for b in statuses:
            adjusted[b["category"]] = max(b["remaining"], 0)

    effective_remaining = sum(adjusted.values())
    daily_allowance = effective_remaining / days_left

    return {
        "balance": bal,
        "upcoming_bills": upcoming,
        "available": available,
        "daily_allowance": daily_allowance,
        "days_left": days_left,
        "is_constrained": is_constrained,
        "effective_remaining": effective_remaining,
        "static_remaining": total_static_remaining,
        "adjusted": adjusted,
    }


def _yesterday_summary(conn: sqlite3.Connection) -> dict:
    """Yesterday's spending for morning briefing."""
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT amount FROM transactions WHERE date = ? AND amount < 0",
        (yesterday,),
    ).fetchall()
    total = sum(abs(r["amount"]) for r in rows)
    return {"total": total, "count": len(rows)}


# ---------------------------------------------------------------------------
# Bot token / chat resolution
# ---------------------------------------------------------------------------

def _resolve_token() -> str:
    try:
        conn = _get_conn()
        token = _get_setting(conn, "telegram_bot_token")
        conn.close()
        if token:
            return token
    except Exception:
        pass

    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    if not token:
        raise RuntimeError(
            "No Telegram bot token found. Set it in the settings table "
            "(key: telegram_bot_token) or via the TELEGRAM_BOT_TOKEN env var."
        )
    return token


def _resolve_chat_id() -> int | None:
    try:
        conn = _get_conn()
        val = _get_setting(conn, "telegram_chat_id")
        conn.close()
        return int(val) if val else None
    except Exception:
        return None


def _resolve_api_key() -> str | None:
    """Resolve Anthropic API key from env or settings."""
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if key:
        return key
    try:
        conn = _get_conn()
        key = _get_setting(conn, "anthropic_api_key", "")
        conn.close()
        return key if key else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Inline keyboard builders
# ---------------------------------------------------------------------------

def _main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Today", callback_data="m:today"),
            InlineKeyboardButton("This Week", callback_data="m:week"),
            InlineKeyboardButton("Budget", callback_data="m:budget"),
        ],
        [
            InlineKeyboardButton("Balance", callback_data="m:bal"),
            InlineKeyboardButton("Add Expense", callback_data="m:add"),
            InlineKeyboardButton("Goals", callback_data="m:goals"),
        ],
        [
            InlineKeyboardButton("Snus", callback_data="m:snus"),
            InlineKeyboardButton("AI Advisor", callback_data="m:ask"),
            InlineKeyboardButton("XP", callback_data="m:xp"),
        ],
        [
            InlineKeyboardButton("Deals", callback_data="m:deals"),
            InlineKeyboardButton("Smart List", callback_data="m:smartlist"),
            InlineKeyboardButton("Alerts", callback_data="m:alerts"),
        ],
        [
            InlineKeyboardButton("Shifts", callback_data="m:shifts"),
        ],
    ])


def _snus_checkin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Clean Today \u2713", callback_data="s:clean"),
            InlineKeyboardButton("Had Snus \u2717", callback_data="s:slip"),
        ],
    ])


# ---------------------------------------------------------------------------
# Reusable send helpers (used by both commands and callbacks)
# ---------------------------------------------------------------------------

async def _send_today(bot, chat_id: int) -> None:
    try:
        conn = _get_conn()
        s = _today_summary(conn)
        conn.close()
    except Exception as exc:
        logger.error("Error fetching today's summary: %s", exc)
        await bot.send_message(chat_id=chat_id, text="Error reading database.")
        return

    if s["tx_count"] == 0:
        await bot.send_message(
            chat_id=chat_id,
            text=f"No transactions recorded for today ({s['date']}).",
        )
        return

    lines = [
        f"\U0001F4C5 *Today's Summary* ({s['date']})",
        "",
        f"Transactions: {s['tx_count']}",
        f"Total spent: {_fmt(s['total_spent'])}",
    ]
    if s["total_income"] > 0:
        lines.append(f"Income: {_fmt(s['total_income'])}")

    if s["by_category"]:
        lines.append("")
        lines.append("*Breakdown:*")
        for cat, amt in s["by_category"].items():
            lines.append(f"  {cat}: {_fmt(amt)}")

    await bot.send_message(
        chat_id=chat_id, text="\n".join(lines), parse_mode="Markdown"
    )


async def _send_week(bot, chat_id: int) -> None:
    try:
        conn = _get_conn()
        s = _week_summary(conn)
        conn.close()
    except Exception as exc:
        logger.error("Error fetching weekly summary: %s", exc)
        await bot.send_message(chat_id=chat_id, text="Error reading database.")
        return

    lines = [
        "\U0001F4CA *Weekly Summary*",
        f"{s['start']} to {s['end']}",
        "",
        f"Total spent: {_fmt(s['total_spent'])}",
        f"Daily average: {_fmt(s['daily_avg'])}",
        f"Transactions: {s['tx_count']}",
    ]

    if s["top_categories"]:
        lines.append("")
        lines.append("*Top categories:*")
        for i, (cat, amt) in enumerate(s["top_categories"].items(), 1):
            lines.append(f"  {i}. {cat}: {_fmt(amt)}")

    await bot.send_message(
        chat_id=chat_id, text="\n".join(lines), parse_mode="Markdown"
    )


async def _send_budget(bot, chat_id: int) -> None:
    try:
        conn = _get_conn()
        statuses = _budget_status(conn)
        ba = _balance_aware_status(conn)
        conn.close()
    except Exception as exc:
        logger.error("Error fetching budget status: %s", exc)
        await bot.send_message(chat_id=chat_id, text="Error reading database.")
        return

    if not statuses:
        await bot.send_message(
            chat_id=chat_id,
            text="No budgets configured. Set them in the dashboard Settings page.",
        )
        return

    month_label = datetime.now().strftime("%B %Y")
    lines = [f"\U0001F4B0 *Budget Status - {month_label}*", ""]

    # Balance-aware header
    if ba["is_constrained"]:
        lines.append(
            f"\u26A0\uFE0F *Balance: {_fmt(ba['balance'])}*\n"
            f"Upcoming bills: {_fmt(ba['upcoming_bills'])}\n"
            f"Available to spend: *{_fmt(ba['available'])}*\n"
            f"Daily allowance: *{_fmt(ba['daily_allowance'])}* "
            f"({ba['days_left']} days left)"
        )
        lines.append("")
    else:
        lines.append(
            f"Can spend: *{_fmt(ba['effective_remaining'])}* "
            f"| {_fmt(ba['daily_allowance'])}/day "
            f"({ba['days_left']} days left)"
        )
        lines.append("")

    keyboard_buttons = []
    for b in statuses:
        adj = ba["adjusted"].get(b["category"], b["remaining"])
        remaining_str = _fmt(adj) if ba["is_constrained"] else _fmt(b["remaining"])
        lines.append(
            f"{b['emoji']} *{b['category']}*\n"
            f"    {_fmt(b['spent'])} / {_fmt(b['limit'])} ({b['pct']:.0f}%)\n"
            f"    Can spend: {remaining_str}"
        )
        # Add drill-down button for categories with spending
        if b["spent"] > 0:
            keyboard_buttons.append(
                InlineKeyboardButton(
                    f"{b['category'][:12]}",
                    callback_data=f"bd:{b['category'][:20]}",
                )
            )

    # Arrange drill-down buttons in rows of 3
    kb_rows = []
    for i in range(0, len(keyboard_buttons), 3):
        kb_rows.append(keyboard_buttons[i:i + 3])

    reply_markup = InlineKeyboardMarkup(kb_rows) if kb_rows else None
    await bot.send_message(
        chat_id=chat_id, text="\n".join(lines),
        parse_mode="Markdown", reply_markup=reply_markup,
    )


async def _send_balance(bot, chat_id: int) -> None:
    try:
        conn = _get_conn()
        bal = _latest_balance(conn)
        conn.close()
    except Exception as exc:
        logger.error("Error fetching balance: %s", exc)
        await bot.send_message(chat_id=chat_id, text="Error reading database.")
        return

    if bal is None:
        await bot.send_message(chat_id=chat_id, text="No transactions in the database.")
        return

    emoji = "\U0001F7E2" if bal >= 0 else "\U0001F534"
    await bot.send_message(
        chat_id=chat_id,
        text=f"{emoji} *Current Balance:* {_fmt(bal)}",
        parse_mode="Markdown",
    )


async def _send_snus(bot, chat_id: int) -> None:
    try:
        conn = _get_conn()
        _init_snus_table(conn)
        info = _snus_streak(conn)
        conn.close()
    except Exception as exc:
        logger.error("Error fetching snus data: %s", exc)
        await bot.send_message(chat_id=chat_id, text="Error reading database.")
        return

    lines = [
        "\U0001F6AD *Snus Tracker*",
        "",
        f"Current streak: *{info['current']} days* clean",
        f"Longest streak: {info['longest']} days",
        f"Total clean days: {info['total_clean']}",
        f"Saved so far: {_fmt(info['saved'])}",
        "",
        _snus_motivation(info["current"]),
        "",
        "How's today going?",
    ]

    await bot.send_message(
        chat_id=chat_id, text="\n".join(lines),
        parse_mode="Markdown", reply_markup=_snus_checkin_keyboard(),
    )


async def _send_goals(bot, chat_id: int) -> None:
    try:
        conn = _get_conn()
        goals = _load_goals(conn)
        conn.close()
    except Exception as exc:
        logger.error("Error fetching goals: %s", exc)
        await bot.send_message(chat_id=chat_id, text="Error reading database.")
        return

    if not goals:
        lines = [
            "\U0001F3AF *Savings Goals*",
            "",
            "No goals set yet. Create one!",
        ]
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("New Goal", callback_data="g:new")],
        ])
    else:
        lines = ["\U0001F3AF *Savings Goals*", ""]
        keyboard_buttons = []
        for i, g in enumerate(goals, 1):
            pct = (g["saved"] / g["target"] * 100) if g["target"] > 0 else 0
            bar = _progress_bar(pct)
            lines.append(
                f"{i}. *{g['name']}*\n"
                f"   {bar}\n"
                f"   {_fmt(g['saved'])} / {_fmt(g['target'])}"
            )
            if g.get("deadline"):
                lines.append(f"   Deadline: {g['deadline']}")
            lines.append("")
            keyboard_buttons.append(
                InlineKeyboardButton(
                    f"Add to {g['name'][:10]}",
                    callback_data=f"ga:{g['id']}",
                )
            )

        kb_rows = []
        for i in range(0, len(keyboard_buttons), 2):
            kb_rows.append(keyboard_buttons[i:i + 2])
        kb_rows.append([InlineKeyboardButton("New Goal", callback_data="g:new")])
        keyboard = InlineKeyboardMarkup(kb_rows)

    await bot.send_message(
        chat_id=chat_id, text="\n".join(lines),
        parse_mode="Markdown", reply_markup=keyboard,
    )


async def _send_xp(bot, chat_id: int) -> None:
    try:
        conn = _get_conn()
        df = _load_transactions_df(conn)
        budgets_df = _load_budgets_df(conn)
        conn.close()
    except Exception as exc:
        logger.error("Error computing XP: %s", exc)
        await bot.send_message(chat_id=chat_id, text="Error reading database.")
        return

    xp = compute_xp(df, budgets_df)
    level = get_level(xp)
    title = LEVELS.get(level, "Unknown")
    next_level_xp = level * XP_PER_LEVEL
    xp_in_level = xp - (level - 1) * XP_PER_LEVEL
    pct = (xp_in_level / XP_PER_LEVEL * 100) if XP_PER_LEVEL > 0 else 0
    bar = _progress_bar(pct)

    lines = [
        "\u2B50 *Gamification Status*",
        "",
        f"Level {level}: *{title}*",
        f"XP: {xp:,}",
        f"Progress to Level {min(level + 1, 10)}: {bar}",
        f"  {xp_in_level} / {XP_PER_LEVEL} XP",
        "",
        "Earn XP by staying under budget:",
        "  +200 XP per month all categories under budget",
        "  +50 XP per week under weekly budget",
        "  +10 XP per day under daily budget",
    ]

    await bot.send_message(
        chat_id=chat_id, text="\n".join(lines), parse_mode="Markdown"
    )


async def _send_alerts(bot, chat_id: int) -> None:
    try:
        conn = _get_conn()
        alerts = _generate_alerts(conn)
        conn.close()
    except Exception as exc:
        logger.error("Error generating alerts: %s", exc)
        await bot.send_message(chat_id=chat_id, text="Error reading database.")
        return

    severity_emoji = {
        "error": "\U0001F6A8",
        "warning": "\u26A0\uFE0F",
        "info": "\u2139\uFE0F",
        "success": "\u2705",
    }

    lines = ["\U0001F514 *Smart Alerts*", ""]
    for sev, msg in alerts:
        emoji = severity_emoji.get(sev, "")
        lines.append(f"{emoji} {msg}")

    await bot.send_message(
        chat_id=chat_id, text="\n".join(lines), parse_mode="Markdown"
    )


async def _send_shifts(bot, chat_id: int) -> None:
    try:
        conn = _get_conn()
        shifts_df = fetch_shifts_standalone(conn)
        conn.close()
    except Exception as exc:
        logger.error("Error fetching shifts: %s", exc)
        await bot.send_message(chat_id=chat_id, text="Error reading database.")
        return

    if shifts_df.empty:
        await bot.send_message(
            chat_id=chat_id,
            text=(
                "No shifts found. Make sure your Google Calendar ICS URL "
                "is configured in the dashboard Settings."
            ),
        )
        return

    today = pd.Timestamp.now().normalize()
    upcoming = shifts_df[shifts_df["date"] >= today].head(3)

    if upcoming.empty:
        await bot.send_message(
            chat_id=chat_id,
            text="No upcoming shifts scheduled.",
        )
        return

    lines = ["\U0001F4C5 *Upcoming Shifts*", ""]
    for _, s in upcoming.iterrows():
        d = s["date"].strftime("%a %b %d")
        lines.append(
            f"*{d}*: {s['hours']}h "
            f"({s['normal_h']}h normal + {s['ot_h']}h OT)\n"
            f"  Est. net: ~{_fmt(s['net'])}"
        )

    await bot.send_message(
        chat_id=chat_id, text="\n".join(lines), parse_mode="Markdown"
    )


async def _send_help(bot, chat_id: int) -> None:
    text = dedent("""\
    \U0001F4D6 *Command Reference*

    *Quick Info:*
    /today - Today's spending summary
    /week - Last 7 days overview
    /budget - Monthly budget status
    /balance - Current account balance
    /shifts - Next 3 upcoming work shifts
    /deals [query] - Search grocery deals nearby
    /smartlist - Generate personalized shopping list

    *Actions:*
    /add <amount> <description> - Log an expense
    /ask <question> - Ask the AI financial advisor
    /goals - View & manage savings goals
    /sync — Sync transactions from Danske Bank

    *Tracking:*
    /snus - Snus accountability tracker
    /xp - XP level & gamification status
    /alerts - Smart financial alerts

    *Navigation:*
    /menu - Main menu with quick buttons
    /help - This help message

    *Tips:*
    - Type expenses directly: "coffee 45" or "150 groceries"
    - Ask questions naturally: "how can I save more?"
    - Daily briefing at 08:00, snus check-in at 20:00
    """)
    await bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")


async def _cmd_sync(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /sync command — trigger manual bank sync."""
    chat_id = update.effective_chat.id
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
    try:
        from bank_sync import sync as bank_sync_run
        new_count = bank_sync_run()
        if new_count > 0:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"\U0001f3e6 Synced {new_count} new transaction(s) from Danske Bank.",
            )
        else:
            await context.bot.send_message(
                chat_id=chat_id,
                text="\U0001f3e6 Already up to date \u2014 no new transactions.",
            )
    except Exception as exc:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"\U0001f3e6 Sync failed: {exc}",
        )


# ---------------------------------------------------------------------------
# Deals helpers
# ---------------------------------------------------------------------------

DEAL_CATEGORIES = {
    "Kød": "kød",
    "Mejeri": "mælk",
    "Brød": "brød",
    "Frugt & Grønt": "frugt grøntsager",
    "Drikkevarer": "sodavand øl juice",
    "Frost": "frost frossen",
    "Snacks": "chips slik snacks",
}


def _deals_category_keyboard() -> InlineKeyboardMarkup:
    """Inline keyboard with grocery category buttons."""
    buttons = [
        InlineKeyboardButton(label, callback_data=f"dc:{term}")
        for label, term in DEAL_CATEGORIES.items()
    ]
    # Arrange in rows of 4
    rows = [buttons[i:i + 4] for i in range(0, len(buttons), 4)]
    rows.append([InlineKeyboardButton("Search...", callback_data="ds:")])
    return InlineKeyboardMarkup(rows)


async def _send_deals(bot, chat_id: int, query: str) -> None:
    """Search for deals and send formatted results."""
    if not DEALS_AVAILABLE:
        await bot.send_message(
            chat_id=chat_id,
            text="Deals module not available. Ensure deals.py is installed.",
        )
        return

    try:
        conn = _get_conn()
        lat = float(_get_setting(conn, "deals_lat", "55.786"))
        lng = float(_get_setting(conn, "deals_lng", "12.524"))
        radius = int(_get_setting(conn, "deals_radius", "10000"))
        conn.close()
    except Exception as exc:
        logger.error("Error reading deals settings: %s", exc)
        await bot.send_message(chat_id=chat_id, text="Error reading settings.")
        return

    try:
        conn2 = _get_conn()
        offers = smart_search(
            conn2, query, source="telegram",
            lat=lat, lng=lng, radius=radius, limit=5,
        )
        conn2.close()
    except Exception as exc:
        logger.error("Deals API error: %s", exc)
        await bot.send_message(
            chat_id=chat_id,
            text=f"Error searching deals: {exc}",
        )
        return

    if not offers:
        await bot.send_message(
            chat_id=chat_id,
            text=f"No deals found for \"{query}\". Try a different search.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Search again", callback_data="ds:")],
            ]),
        )
        return

    lines = [f"\U0001F6D2 *Deals near Lyngby:* \"{query}\"", ""]
    for i, offer in enumerate(offers, 1):
        lines.append(format_offer_text(offer, index=i))
        lines.append("")

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Search again", callback_data="ds:"),
            InlineKeyboardButton("Categories", callback_data="m:deals"),
        ],
    ])

    await bot.send_message(
        chat_id=chat_id,
        text="\n".join(lines),
        parse_mode="Markdown",
        reply_markup=keyboard,
    )


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/start -- register the chat for alerts and show menu."""
    chat_id = update.effective_chat.id
    try:
        conn = _get_conn()
        _set_setting(conn, "telegram_chat_id", str(chat_id))
        _init_snus_table(conn)
        conn.close()
    except Exception as exc:
        logger.error("Failed to save chat ID: %s", exc)
        await update.message.reply_text(
            "Failed to register chat ID. Check the database."
        )
        return

    await update.message.reply_text(
        dedent(f"""\
        \u2705 Registered! Chat ID {chat_id} saved.

        Welcome to your Budget Companion! Use the menu below or type /help for all commands."""),
        reply_markup=_main_menu_keyboard(),
    )


async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/menu -- show the main interactive menu."""
    await update.message.reply_text(
        "\U0001F4CA *What would you like to check?*",
        parse_mode="Markdown",
        reply_markup=_main_menu_keyboard(),
    )


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_today(context.bot, update.effective_chat.id)


async def cmd_week(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_week(context.bot, update.effective_chat.id)


async def cmd_budget(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_budget(context.bot, update.effective_chat.id)


async def cmd_balance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_balance(context.bot, update.effective_chat.id)


async def cmd_snus(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_snus(context.bot, update.effective_chat.id)


async def cmd_goals(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_goals(context.bot, update.effective_chat.id)


async def cmd_xp(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_xp(context.bot, update.effective_chat.id)


async def cmd_alerts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_alerts(context.bot, update.effective_chat.id)


async def cmd_shifts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_shifts(context.bot, update.effective_chat.id)


async def cmd_deals(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/deals [query] -- search for grocery deals."""
    if context.args:
        query = " ".join(context.args)
        await _send_deals(context.bot, update.effective_chat.id, query)
    else:
        await update.message.reply_text(
            "\U0001F6D2 *What are you looking for?*\n\n"
            "Pick a category or search for something specific:",
            parse_mode="Markdown",
            reply_markup=_deals_category_keyboard(),
        )


async def cmd_smartlist(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/smartlist -- generate a personalized smart shopping list."""
    chat_id = update.effective_chat.id

    if not DEALS_AVAILABLE:
        await update.message.reply_text("Deals module not available.")
        return

    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    try:
        conn = _get_conn()
        lat = float(_get_setting(conn, "deals_lat", "55.786"))
        lng = float(_get_setting(conn, "deals_lng", "12.524"))
        radius = int(_get_setting(conn, "deals_radius", "10000"))

        result = generate_smart_list(conn, lat=lat, lng=lng, radius=radius)
        budget = get_grocery_budget_status(conn)
        conn.close()
    except Exception as exc:
        logger.error("Smart list error: %s", exc)
        await update.message.reply_text(f"Error generating smart list: {exc}")
        return

    if not result["items"]:
        await update.message.reply_text(
            "No items to build a list from yet.\n"
            "Search for some items with /deals first, "
            "and they'll be remembered for next time!"
        )
        return

    lines = ["\U0001F6D2 *Smart Shopping List*", ""]

    for i, item in enumerate(result["items"], 1):
        o = item.get("best_offer")
        if o and o.get("price"):
            price_str = f"{o['price']:,.2f}".replace(",", ".")
            line = f"{i}. *{item['name'].title()}*"
            line += f"\n   {o['heading'][:35]} — {price_str} DKK at {o.get('dealer', '?')}"
            if o.get("discount_pct", 0) > 0:
                line += f" (-{o['discount_pct']:.0f}%)"
            if o.get("is_all_time_low"):
                line += " \U0001F525 ALL-TIME LOW"
            line += f"\n   _{item['reason']}_"
        else:
            line = f"{i}. *{item['name'].title()}* — no deals found"
            line += f"\n   _{item['reason']}_"
        lines.append(line)
        lines.append("")

    lines.append("---")
    lines.append(
        f"\U0001F4B0 Est. total: *{result['estimated_total']:,.0f} DKK*"
    )
    if result["savings_vs_regular"] > 0:
        lines.append(
            f"\U0001F4B5 Savings vs regular: *{result['savings_vs_regular']:,.0f} DKK*"
        )
    if budget["limit"] > 0:
        lines.append(
            f"\U0001F4CA Budget remaining: *{budget['remaining']:,.0f} / {budget['limit']:,.0f} DKK* "
            f"({budget['pct_used']:.0f}% used)"
        )

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Deals Menu", callback_data="m:deals"),
            InlineKeyboardButton("Main Menu", callback_data="m:menu"),
        ],
    ])

    await context.bot.send_message(
        chat_id=chat_id,
        text="\n".join(lines),
        parse_mode="Markdown",
        reply_markup=keyboard,
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_help(context.bot, update.effective_chat.id)


# ---------------------------------------------------------------------------
# /add command — quick expense logging
# ---------------------------------------------------------------------------

async def cmd_add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/add <amount> <desc> or /add <desc> <amount>."""
    chat_id = update.effective_chat.id
    args = context.args

    if not args:
        await update.message.reply_text(
            "Usage: /add <amount> <description>\n"
            "Examples:\n"
            "  /add 150 groceries\n"
            "  /add coffee 45"
        )
        return

    amount, desc = _parse_expense_args(args)
    if amount is None:
        await update.message.reply_text(
            "Could not parse amount. Try: /add 150 groceries"
        )
        return

    cat, subcat = _match_category(desc)
    context.user_data["pending_tx"] = {
        "desc": desc,
        "amount": -abs(amount),  # expenses are negative
        "cat": cat,
        "subcat": subcat,
        "time": _time.time(),
    }

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\u2705 Confirm", callback_data="tx:confirm"),
            InlineKeyboardButton("\u274C Cancel", callback_data="tx:cancel"),
        ],
        [
            InlineKeyboardButton("Edit Category", callback_data="tx:editcat"),
        ],
    ])

    await update.message.reply_text(
        f"*New Expense:*\n"
        f"  Amount: {_fmt(abs(amount))}\n"
        f"  Description: {desc}\n"
        f"  Category: {cat}" + (f" > {subcat}" if subcat else ""),
        parse_mode="Markdown",
        reply_markup=keyboard,
    )


def _parse_expense_args(args: list[str]) -> tuple[float | None, str]:
    """Parse expense args, allowing amount-first or desc-first."""
    # Try amount as first arg
    try:
        amount = float(args[0].replace(",", "."))
        desc = " ".join(args[1:]) if len(args) > 1 else "Manual expense"
        return amount, desc
    except ValueError:
        pass

    # Try amount as last arg
    try:
        amount = float(args[-1].replace(",", "."))
        desc = " ".join(args[:-1])
        return amount, desc
    except ValueError:
        pass

    return None, ""


# ---------------------------------------------------------------------------
# /ask command — AI-powered financial advice
# ---------------------------------------------------------------------------

async def cmd_ask(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/ask [question] — get AI financial advice."""
    chat_id = update.effective_chat.id

    if not AI_AVAILABLE:
        await update.message.reply_text(
            "AI features not available. Install the anthropic package: "
            "pip install anthropic"
        )
        return

    api_key = _resolve_api_key()
    if not api_key:
        await update.message.reply_text(
            "No Anthropic API key configured. Set ANTHROPIC_API_KEY env var "
            "or add it in dashboard Settings."
        )
        return

    # Set the key in env for the anthropic client
    os.environ["ANTHROPIC_API_KEY"] = api_key

    question = " ".join(context.args) if context.args else None

    # Show typing indicator
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    try:
        conn = _get_conn()
        df = _load_transactions_df(conn)
        budgets_df = _load_budgets_df(conn)
        conn.close()
    except Exception as exc:
        logger.error("Error loading data for AI: %s", exc)
        await update.message.reply_text("Error reading database.")
        return

    if df.empty:
        await update.message.reply_text("No transaction data available for analysis.")
        return

    # Rename column for ai_insights compatibility
    if "monthly_limit" in budgets_df.columns:
        budgets_df = budgets_df.rename(columns={"monthly_limit": "budget"})

    try:
        response = get_financial_insights(df, budgets_df, question)
    except Exception as exc:
        logger.error("AI error: %s", exc)
        await update.message.reply_text(f"Error getting AI response: {exc}")
        return

    await _send_long_message(context.bot, chat_id, f"\U0001F916 *AI Advisor*\n\n{response}")


# ---------------------------------------------------------------------------
# Callback query handler — routes all inline button presses
# ---------------------------------------------------------------------------

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Central callback handler for all inline keyboard buttons."""
    query = update.callback_query
    await query.answer()  # Acknowledge the button press

    data = query.data
    chat_id = query.message.chat_id
    bot = context.bot

    # --- Main menu buttons ---
    if data == "m:today":
        await _send_today(bot, chat_id)
    elif data == "m:week":
        await _send_week(bot, chat_id)
    elif data == "m:budget":
        await _send_budget(bot, chat_id)
    elif data == "m:bal":
        await _send_balance(bot, chat_id)
    elif data == "m:add":
        await bot.send_message(
            chat_id=chat_id,
            text=(
                "To add an expense, send:\n"
                "/add <amount> <description>\n\n"
                "Or just type it naturally:\n"
                "\"coffee 45\" or \"150 groceries\""
            ),
        )
    elif data == "m:goals":
        await _send_goals(bot, chat_id)
    elif data == "m:snus":
        await _send_snus(bot, chat_id)
    elif data == "m:ask":
        await bot.send_message(
            chat_id=chat_id,
            text=(
                "Ask me anything about your finances!\n\n"
                "Send: /ask <your question>\n\n"
                "Examples:\n"
                "- /ask how can I save more?\n"
                "- /ask what are my biggest expenses?\n"
                "- /ask can I afford to drop the SU loan?"
            ),
        )
    elif data == "m:xp":
        await _send_xp(bot, chat_id)
    elif data == "m:deals":
        await bot.send_message(
            chat_id=chat_id,
            text="\U0001F6D2 *What are you looking for?*\n\nPick a category or search:",
            parse_mode="Markdown",
            reply_markup=_deals_category_keyboard(),
        )
    elif data == "m:smartlist":
        # Trigger smart list generation
        if DEALS_AVAILABLE:
            await bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
            try:
                conn3 = _get_conn()
                lat3 = float(_get_setting(conn3, "deals_lat", "55.786"))
                lng3 = float(_get_setting(conn3, "deals_lng", "12.524"))
                radius3 = int(_get_setting(conn3, "deals_radius", "10000"))
                result = generate_smart_list(conn3, lat=lat3, lng=lng3, radius=radius3)
                budget3 = get_grocery_budget_status(conn3)
                conn3.close()

                if result["items"]:
                    sl_lines = ["\U0001F6D2 *Smart Shopping List*", ""]
                    for i, item in enumerate(result["items"], 1):
                        o = item.get("best_offer")
                        if o and o.get("price"):
                            price_s = f"{o['price']:,.2f}".replace(",", ".")
                            sl_lines.append(
                                f"{i}. *{item['name'].title()}* — {price_s} DKK at {o.get('dealer', '?')}"
                            )
                        else:
                            sl_lines.append(f"{i}. *{item['name'].title()}* — no deals")
                    sl_lines.append("")
                    sl_lines.append(f"Est. total: *{result['estimated_total']:,.0f} DKK*")
                    if budget3["limit"] > 0:
                        sl_lines.append(f"Budget: *{budget3['remaining']:,.0f}* DKK left")
                    await bot.send_message(chat_id=chat_id, text="\n".join(sl_lines), parse_mode="Markdown")
                else:
                    await bot.send_message(
                        chat_id=chat_id,
                        text="No items yet. Search with /deals first to build your list!",
                    )
            except Exception as exc:
                logger.error("Smart list callback error: %s", exc)
                await bot.send_message(chat_id=chat_id, text=f"Error: {exc}")
        else:
            await bot.send_message(chat_id=chat_id, text="Deals module not available.")
    elif data == "m:alerts":
        await _send_alerts(bot, chat_id)
    elif data == "m:shifts":
        await _send_shifts(bot, chat_id)

    # --- Deals ---
    elif data.startswith("dc:"):
        # Deal category search
        search_term = data[3:]
        await _send_deals(bot, chat_id, search_term)
    elif data == "ds:":
        # Deal search prompt
        context.user_data["deal_flow"] = "awaiting_query"
        await bot.send_message(
            chat_id=chat_id,
            text="What would you like to search for? Type your query:",
        )

    # --- Budget drill-down ---
    elif data.startswith("bd:"):
        category = data[3:]
        await _send_budget_drilldown(bot, chat_id, category)

    # --- Snus check-in ---
    elif data == "s:clean":
        await _handle_snus_checkin(bot, chat_id, "clean")
    elif data == "s:slip":
        await _handle_snus_checkin(bot, chat_id, "slipped")

    # --- Transaction confirm/cancel/undo ---
    elif data == "tx:confirm":
        await _handle_tx_confirm(bot, chat_id, context)
    elif data == "tx:cancel":
        context.user_data.pop("pending_tx", None)
        await query.edit_message_text("Transaction cancelled.")
    elif data == "tx:editcat":
        await _handle_tx_editcat(bot, chat_id, context, query)
    elif data.startswith("tc:"):
        # Category selection from edit
        await _handle_tx_category_select(bot, chat_id, context, query, data[3:])
    elif data.startswith("u:"):
        # Undo transaction
        tx_id = int(data[2:])
        await _handle_undo(bot, chat_id, tx_id)

    # --- Goals ---
    elif data == "g:new":
        context.user_data["goal_flow"] = "awaiting_name"
        await bot.send_message(
            chat_id=chat_id,
            text="What's the name of your new savings goal?",
        )
    elif data.startswith("ga:"):
        # Add to goal
        goal_id = int(data[3:])
        context.user_data["goal_add_id"] = goal_id
        context.user_data["goal_flow"] = "awaiting_add_amount"
        await bot.send_message(
            chat_id=chat_id,
            text="How much would you like to add? (enter amount in DKK)",
        )


async def _send_budget_drilldown(bot, chat_id: int, category: str) -> None:
    """Show recent transactions for a budget category."""
    try:
        conn = _get_conn()
        month_start = datetime.now().strftime("%Y-%m-01")
        month_end = datetime.now().strftime("%Y-%m-31")
        rows = conn.execute(
            "SELECT date, description, amount FROM transactions "
            "WHERE category = ? AND amount < 0 AND date BETWEEN ? AND ? "
            "ORDER BY date DESC LIMIT 10",
            (category, month_start, month_end),
        ).fetchall()
        conn.close()
    except Exception as exc:
        logger.error("Error in budget drill-down: %s", exc)
        await bot.send_message(chat_id=chat_id, text="Error reading database.")
        return

    if not rows:
        await bot.send_message(
            chat_id=chat_id,
            text=f"No transactions found for *{category}* this month.",
            parse_mode="Markdown",
        )
        return

    lines = [f"\U0001F50D *{category}* - Recent Transactions", ""]
    for r in rows:
        lines.append(f"  {r['date'][:10]}  {_fmt(abs(r['amount']))}  {r['description'][:30]}")

    await bot.send_message(
        chat_id=chat_id, text="\n".join(lines), parse_mode="Markdown"
    )


async def _handle_snus_checkin(bot, chat_id: int, status: str) -> None:
    """Record a snus check-in."""
    today_str = datetime.now().strftime("%Y-%m-%d")
    try:
        conn = _get_conn()
        _init_snus_table(conn)
        conn.execute(
            "INSERT OR REPLACE INTO snus_checkins (date, status) VALUES (?, ?)",
            (today_str, status),
        )
        conn.commit()
        info = _snus_streak(conn)
        conn.close()
    except Exception as exc:
        logger.error("Error recording snus check-in: %s", exc)
        await bot.send_message(chat_id=chat_id, text="Error saving check-in.")
        return

    if status == "clean":
        msg = (
            f"\u2705 Clean day recorded!\n\n"
            f"Streak: *{info['current']} days*\n"
            f"{_snus_motivation(info['current'])}"
        )
    else:
        msg = (
            f"Recorded. Don't beat yourself up.\n\n"
            f"Streak reset to 0. Your longest was {info['longest']} days.\n"
            f"{_snus_motivation(0)}"
        )

    await bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")


async def _handle_tx_confirm(bot, chat_id: int, context) -> None:
    """Confirm and insert a pending transaction."""
    pending = context.user_data.get("pending_tx")
    if not pending:
        await bot.send_message(chat_id=chat_id, text="No pending transaction.")
        return

    try:
        conn = _get_conn()
        tx_id = _insert_manual_tx(
            conn, pending["desc"], pending["amount"],
            pending["cat"], pending["subcat"],
        )
        conn.close()
    except sqlite3.IntegrityError:
        await bot.send_message(
            chat_id=chat_id, text="Transaction already exists (duplicate)."
        )
        context.user_data.pop("pending_tx", None)
        return
    except Exception as exc:
        logger.error("Error inserting transaction: %s", exc)
        await bot.send_message(chat_id=chat_id, text="Error saving transaction.")
        return

    context.user_data.pop("pending_tx", None)
    context.user_data["last_tx_id"] = tx_id
    context.user_data["last_tx_time"] = _time.time()

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Undo (5 min)", callback_data=f"u:{tx_id}")],
    ])

    # Show remaining budget for this category after saving
    try:
        conn2 = _get_conn()
        ba = _balance_aware_status(conn2)
        conn2.close()
        cat_remaining = ba["adjusted"].get(pending["cat"], 0)
        budget_line = (
            f"\n\U0001F4B5 {pending['cat']}: {_fmt(cat_remaining)} left"
            f" | Daily: {_fmt(ba['daily_allowance'])}"
        )
        if ba["is_constrained"]:
            budget_line += f"\n\u26A0\uFE0F Balance low: {_fmt(ba['balance'])}"
    except Exception:
        budget_line = ""

    await bot.send_message(
        chat_id=chat_id,
        text=(
            f"\u2705 Saved: {pending['desc']} "
            f"({_fmt(abs(pending['amount']))})\n"
            f"Category: {pending['cat']}"
            f"{budget_line}"
        ),
        reply_markup=keyboard,
    )


async def _handle_tx_editcat(bot, chat_id: int, context, query) -> None:
    """Show category selection buttons."""
    categories = [
        "Dagligvarer", "Forn\u00f8jelser og fritid", "Transport", "Bolig",
        "\u00d8vrige udgifter", "T\u00f8j, sko og personlig pleje",
        "Personforsikringer", "Opsparing", "Snus",
    ]

    kb_rows = []
    for i in range(0, len(categories), 2):
        row = []
        for cat in categories[i:i + 2]:
            short = cat[:15]
            row.append(InlineKeyboardButton(short, callback_data=f"tc:{cat}"))
        kb_rows.append(row)

    await bot.send_message(
        chat_id=chat_id,
        text="Select a category:",
        reply_markup=InlineKeyboardMarkup(kb_rows),
    )


async def _handle_tx_category_select(bot, chat_id: int, context, query,
                                     category: str) -> None:
    """Update pending tx category and confirm."""
    pending = context.user_data.get("pending_tx")
    if not pending:
        await bot.send_message(chat_id=chat_id, text="No pending transaction.")
        return

    pending["cat"] = category
    pending["subcat"] = ""

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\u2705 Confirm", callback_data="tx:confirm"),
            InlineKeyboardButton("\u274C Cancel", callback_data="tx:cancel"),
        ],
    ])

    await bot.send_message(
        chat_id=chat_id,
        text=(
            f"*Updated Expense:*\n"
            f"  Amount: {_fmt(abs(pending['amount']))}\n"
            f"  Description: {pending['desc']}\n"
            f"  Category: {category}"
        ),
        parse_mode="Markdown",
        reply_markup=keyboard,
    )


async def _handle_undo(bot, chat_id: int, tx_id: int) -> None:
    """Delete a transaction if within the undo window."""
    try:
        conn = _get_conn()
        # Check if tx exists
        row = conn.execute(
            "SELECT description, amount FROM transactions WHERE id = ?", (tx_id,)
        ).fetchone()
        if not row:
            await bot.send_message(chat_id=chat_id, text="Transaction not found.")
            conn.close()
            return

        conn.execute("DELETE FROM transactions WHERE id = ?", (tx_id,))
        conn.commit()
        conn.close()
    except Exception as exc:
        logger.error("Error undoing transaction: %s", exc)
        await bot.send_message(chat_id=chat_id, text="Error undoing transaction.")
        return

    await bot.send_message(
        chat_id=chat_id,
        text=f"\u21A9 Undone: {row['description']} ({_fmt(abs(row['amount']))})",
    )


# ---------------------------------------------------------------------------
# Free text message handler
# ---------------------------------------------------------------------------

# Question patterns (EN/DA)
QUESTION_PATTERN = re.compile(
    r"^\s*(how|what|why|can|should|will|would|is|are|do|does|"
    r"hvor|hvad|hvorfor|kan|skal|vil|er|har)\b",
    re.IGNORECASE,
)

# Expense patterns: "coffee 45", "45 coffee", "spent 200 on transport"
EXPENSE_PATTERN_NUM_FIRST = re.compile(
    r"^\s*(\d+[.,]?\d*)\s+(.+)$"
)
EXPENSE_PATTERN_NUM_LAST = re.compile(
    r"^\s*(.+?)\s+(\d+[.,]?\d*)\s*$"
)
EXPENSE_PATTERN_SPENT = re.compile(
    r"(?:spent|brugte|brugt)\s+(\d+[.,]?\d*)\s+(?:on|til|pa|p\u00e5)\s+(.+)",
    re.IGNORECASE,
)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle free-text messages: expense logging, questions, goal flows."""
    text = update.message.text.strip()
    chat_id = update.effective_chat.id

    # --- Deal flow: awaiting search query ---
    if context.user_data.get("deal_flow") == "awaiting_query":
        context.user_data.pop("deal_flow", None)
        await _send_deals(context.bot, chat_id, text)
        return

    # --- Goal flow: awaiting input ---
    goal_flow = context.user_data.get("goal_flow")
    if goal_flow == "awaiting_name":
        context.user_data["goal_name"] = text
        context.user_data["goal_flow"] = "awaiting_target"
        await update.message.reply_text(
            f"Goal: *{text}*\nWhat's the target amount? (DKK)",
            parse_mode="Markdown",
        )
        return

    if goal_flow == "awaiting_target":
        try:
            target = float(text.replace(",", ".").replace(" ", ""))
        except ValueError:
            await update.message.reply_text("Please enter a valid number.")
            return

        name = context.user_data.pop("goal_name", "Unnamed Goal")
        context.user_data.pop("goal_flow", None)

        try:
            conn = _get_conn()
            _save_goal(conn, name, target)
            conn.close()
        except Exception as exc:
            logger.error("Error saving goal: %s", exc)
            await update.message.reply_text("Error saving goal.")
            return

        await update.message.reply_text(
            f"\u2705 Goal created: *{name}* - Target: {_fmt(target)}",
            parse_mode="Markdown",
        )
        await _send_goals(context.bot, chat_id)
        return

    if goal_flow == "awaiting_add_amount":
        try:
            add_amount = float(text.replace(",", ".").replace(" ", ""))
        except ValueError:
            await update.message.reply_text("Please enter a valid number.")
            return

        goal_id = context.user_data.pop("goal_add_id", None)
        context.user_data.pop("goal_flow", None)

        if goal_id is None:
            await update.message.reply_text("Goal not found. Try /goals again.")
            return

        try:
            conn = _get_conn()
            row = conn.execute(
                "SELECT saved FROM savings_goals WHERE id = ?", (goal_id,)
            ).fetchone()
            if row is None:
                await update.message.reply_text("Goal not found.")
                conn.close()
                return
            new_saved = row["saved"] + add_amount
            _update_goal_progress(conn, goal_id, new_saved)
            conn.close()
        except Exception as exc:
            logger.error("Error updating goal: %s", exc)
            await update.message.reply_text("Error updating goal.")
            return

        await update.message.reply_text(
            f"\u2705 Added {_fmt(add_amount)} to goal!",
        )
        await _send_goals(context.bot, chat_id)
        return

    # --- Route questions to AI ---
    if QUESTION_PATTERN.match(text):
        if not AI_AVAILABLE:
            await update.message.reply_text(
                "AI features not available. Use /help for available commands."
            )
            return
        api_key = _resolve_api_key()
        if not api_key:
            # Not a question for us, ignore
            return

        os.environ["ANTHROPIC_API_KEY"] = api_key
        await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

        try:
            conn = _get_conn()
            df = _load_transactions_df(conn)
            budgets_df = _load_budgets_df(conn)
            conn.close()
        except Exception:
            return

        if df.empty:
            return

        if "monthly_limit" in budgets_df.columns:
            budgets_df = budgets_df.rename(columns={"monthly_limit": "budget"})

        try:
            response = get_financial_insights(df, budgets_df, text)
        except Exception:
            return

        await _send_long_message(
            context.bot, chat_id, f"\U0001F916 *AI Advisor*\n\n{response}"
        )
        return

    # --- Try parsing as an expense ---
    amount, desc = None, ""

    # "spent 200 on transport" / "brugte 150 til mad"
    m = EXPENSE_PATTERN_SPENT.match(text)
    if m:
        try:
            amount = float(m.group(1).replace(",", "."))
            desc = m.group(2).strip()
        except ValueError:
            pass

    # "45 coffee"
    if amount is None:
        m = EXPENSE_PATTERN_NUM_FIRST.match(text)
        if m:
            try:
                amount = float(m.group(1).replace(",", "."))
                desc = m.group(2).strip()
            except ValueError:
                pass

    # "coffee 45"
    if amount is None:
        m = EXPENSE_PATTERN_NUM_LAST.match(text)
        if m:
            try:
                amount = float(m.group(2).replace(",", "."))
                desc = m.group(1).strip()
            except ValueError:
                pass

    if amount is not None and desc:
        cat, subcat = _match_category(desc)
        context.user_data["pending_tx"] = {
            "desc": desc,
            "amount": -abs(amount),
            "cat": cat,
            "subcat": subcat,
            "time": _time.time(),
        }

        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("\u2705 Confirm", callback_data="tx:confirm"),
                InlineKeyboardButton("\u274C Cancel", callback_data="tx:cancel"),
            ],
            [
                InlineKeyboardButton("Edit Category", callback_data="tx:editcat"),
            ],
        ])

        await update.message.reply_text(
            f"*New Expense:*\n"
            f"  Amount: {_fmt(abs(amount))}\n"
            f"  Description: {desc}\n"
            f"  Category: {cat}" + (f" > {subcat}" if subcat else ""),
            parse_mode="Markdown",
            reply_markup=keyboard,
        )
        return

    # Message didn't match any pattern — silently ignore


# ---------------------------------------------------------------------------
# Scheduled jobs
# ---------------------------------------------------------------------------

async def _nightly_bank_sync(context: ContextTypes.DEFAULT_TYPE) -> None:
    """03:00 daily bank sync via GoCardless."""
    chat_id = _resolve_chat_id()
    try:
        from bank_sync import sync as bank_sync_run
        new_count = bank_sync_run()
        logger.info("Nightly bank sync: %d new transaction(s).", new_count)
        if chat_id and new_count > 0:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"\U0001f3e6 *Nightly Sync*\n{new_count} new transaction(s) imported from Danske Bank.",
                parse_mode="Markdown",
            )
    except Exception as exc:
        logger.error("Nightly bank sync failed: %s", exc)
        if chat_id:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"\U0001f3e6 *Sync Error*\n{exc}",
                parse_mode="Markdown",
            )


async def _morning_briefing(context: ContextTypes.DEFAULT_TYPE) -> None:
    """08:00 daily morning briefing."""
    chat_id = _resolve_chat_id()
    if chat_id is None:
        logger.warning("Morning briefing skipped: no chat ID registered.")
        return

    try:
        conn = _get_conn()
        _init_snus_table(conn)
        bal = _latest_balance(conn)
        yesterday = _yesterday_summary(conn)
        budgets = _budget_status(conn)
        ba = _balance_aware_status(conn)
        snus_info = _snus_streak(conn)
        df = _load_transactions_df(conn)
        budgets_df = _load_budgets_df(conn)
        shifts_df = fetch_shifts_standalone(conn)
        subs = _upcoming_subscriptions(conn)
        conn.close()
    except Exception as exc:
        logger.error("Morning briefing DB error: %s", exc)
        return

    lines = ["\U0001F305 *Good morning! Here's your day:*", ""]

    # Balance + daily allowance
    if bal is not None:
        bal_emoji = "\U0001F7E2" if bal >= 0 else "\U0001F534"
        lines.append(f"{bal_emoji} Balance: *{_fmt(bal)}*")
        lines.append(
            f"\U0001F4B5 You can spend *{_fmt(ba['daily_allowance'])}* today "
            f"({_fmt(ba['effective_remaining'])} over {ba['days_left']} days)"
        )
        if ba["is_constrained"]:
            lines.append(
                f"\u26A0\uFE0F Budget adjusted to balance "
                f"(bills coming: {_fmt(ba['upcoming_bills'])})"
            )

    # Yesterday
    if yesterday["count"] > 0:
        lines.append(
            f"\U0001F4CA Yesterday: {_fmt(yesterday['total'])} "
            f"({yesterday['count']} transaction{'s' if yesterday['count'] != 1 else ''})"
        )

    # Budget alerts (>60%)
    warnings = [b for b in budgets if b["pct"] >= 60]
    if warnings:
        lines.append("")
        lines.append("\u26A0\uFE0F *Budget alerts:*")
        for b in warnings:
            adj = ba["adjusted"].get(b["category"], b["remaining"])
            lines.append(
                f"  {b['category']}: {b['pct']:.0f}% used "
                f"({_fmt(b['spent'])}/{_fmt(b['limit'])}) — "
                f"can spend {_fmt(adj)}"
            )

    # Upcoming shifts
    if not shifts_df.empty:
        today_ts = pd.Timestamp.now().normalize()
        upcoming = shifts_df[
            (shifts_df["date"] >= today_ts)
            & (shifts_df["date"] <= today_ts + pd.Timedelta(days=3))
        ]
        if not upcoming.empty:
            lines.append("")
            lines.append("\U0001F4C5 *Upcoming:*")
            for _, s in upcoming.iterrows():
                d = s["date"].strftime("%a %b %d")
                lines.append(
                    f"  Shift {d} ({s['hours']}h, ~{_fmt(s['net'])} net)"
                )

    # Upcoming subscriptions
    if subs:
        if "\U0001F4C5 *Upcoming:*" not in lines:
            lines.append("")
            lines.append("\U0001F4C5 *Upcoming:*")
        for sub in subs[:3]:
            lines.append(
                f"  {sub['description'][:25]} in {sub['days_until']}d "
                f"(~{_fmt(sub['amount'])})"
            )

    # Grocery deals of the day (personalized)
    if DEALS_AVAILABLE:
        try:
            conn2 = _get_conn()
            deals_lat = float(_get_setting(conn2, "deals_lat", "55.786"))
            deals_lng = float(_get_setting(conn2, "deals_lng", "12.524"))
            deals_radius = int(_get_setting(conn2, "deals_radius", "10000"))

            # Use personalized items, fall back to defaults
            freq = get_frequent_searches(conn2, 5)
            staples = [f["name"] for f in freq] if freq else ["mælk", "brød", "kylling"]

            best = deals_cheapest(
                staples,
                lat=deals_lat, lng=deals_lng, radius=deals_radius,
            )
            # Record prices from briefing searches
            for item_offers in best.values():
                if item_offers:
                    record_prices(conn2, item_offers)

            deal_lines = []
            for item, item_offers in best.items():
                if item_offers:
                    o = item_offers[0]
                    price_str = f"{o['price']:,.2f}".replace(",", ".") if o["price"] else "?"
                    line = f"  {o['heading'][:30]} — {price_str} at {o['dealer']}"
                    if o["discount_pct"] > 0:
                        line += f" (-{o['discount_pct']:.0f}%)"
                    deal_lines.append(line)
            if deal_lines:
                lines.append("")
                lines.append("\U0001F6D2 *Today's grocery deals:*")
                lines.extend(deal_lines)

            # Price drop alerts
            drops = find_price_drops(conn2, threshold_pct=10)
            if drops:
                lines.append("")
                lines.append("\U0001F4C9 *Price drops:*")
                for d in drops[:3]:
                    lines.append(
                        f"  {d['product'][:25]} — {d['current_price']:.0f} DKK "
                        f"at {d['dealer']} ({d['drop_pct']:.0f}% below avg)"
                    )

            conn2.close()
        except Exception as exc:
            logger.warning("Morning briefing deals error: %s", exc)

    # Snus streak
    if snus_info["total_clean"] > 0 or snus_info["current"] > 0:
        lines.append("")
        lines.append(
            f"\U0001F6AD Snus streak: {snus_info['current']} days "
            f"(saved {_fmt(snus_info['saved'])})"
        )

    # XP/Level
    xp = compute_xp(df, budgets_df)
    level = get_level(xp)
    title = LEVELS.get(level, "Unknown")
    lines.append(f"\u2B50 Level {level}: {title} ({xp:,} XP)")

    try:
        await context.bot.send_message(
            chat_id=chat_id, text="\n".join(lines), parse_mode="Markdown"
        )
        logger.info("Morning briefing sent to chat %s.", chat_id)
    except Exception as exc:
        logger.error("Failed to send morning briefing: %s", exc)


async def _snus_daily_checkin(context: ContextTypes.DEFAULT_TYPE) -> None:
    """20:00 daily snus check-in push notification."""
    chat_id = _resolve_chat_id()
    if chat_id is None:
        return

    try:
        conn = _get_conn()
        _init_snus_table(conn)
        info = _snus_streak(conn)

        # Check if already checked in today
        today_str = datetime.now().strftime("%Y-%m-%d")
        row = conn.execute(
            "SELECT status FROM snus_checkins WHERE date = ?", (today_str,)
        ).fetchone()
        conn.close()

        if row:
            # Already checked in
            return
    except Exception as exc:
        logger.error("Snus check-in error: %s", exc)
        return

    lines = [
        "\U0001F6AD *Evening Snus Check-in*",
        "",
        f"Current streak: *{info['current']} days*",
        "",
        "How was today?",
    ]

    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text="\n".join(lines),
            parse_mode="Markdown",
            reply_markup=_snus_checkin_keyboard(),
        )
    except Exception as exc:
        logger.error("Failed to send snus check-in: %s", exc)


async def _price_drop_alert(context: ContextTypes.DEFAULT_TYPE) -> None:
    """10:00 daily price drop alert for tracked items."""
    if not DEALS_AVAILABLE:
        return
    chat_id = _resolve_chat_id()
    if chat_id is None:
        return

    try:
        conn = _get_conn()
        drops = find_price_drops(conn, threshold_pct=10)
        conn.close()
    except Exception as exc:
        logger.error("Price drop alert error: %s", exc)
        return

    if not drops:
        logger.info("Price drop check: no drops found.")
        return

    lines = ["\U0001F4C9 *Price Drop Alert!*", ""]
    for d in drops[:5]:
        lines.append(
            f"*{d['product'][:30].title()}*\n"
            f"  {d['current_price']:.0f} DKK at {d['dealer']} "
            f"({d['drop_pct']:.0f}% below avg {d['avg_price']:.0f} DKK)"
        )
        if d["current_price"] <= d["all_time_low"]:
            lines.append("  \U0001F525 All-time low!")
        lines.append("")

    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text="\n".join(lines),
            parse_mode="Markdown",
        )
        logger.info("Price drop alert sent to chat %s.", chat_id)
    except Exception as exc:
        logger.error("Failed to send price drop alert: %s", exc)


async def _daily_budget_alert(context: ContextTypes.DEFAULT_TYPE) -> None:
    """21:00 daily budget alert (budget >80% or low balance)."""
    chat_id = _resolve_chat_id()
    if chat_id is None:
        logger.warning("Daily alert skipped: no chat ID registered.")
        return

    try:
        conn = _get_conn()
        statuses = _budget_status(conn)
        ba = _balance_aware_status(conn)
        conn.close()
    except Exception as exc:
        logger.error("Daily alert DB error: %s", exc)
        return

    budget_warnings = [b for b in statuses if b["pct"] >= 80]
    low_balance = ba["is_constrained"]

    if not budget_warnings and not low_balance:
        logger.info("Daily check: all budgets under 80%%, balance OK. No alert sent.")
        return

    month_label = datetime.now().strftime("%B %Y")
    lines = [f"\U0001F514 *Budget Alert - {month_label}*", ""]

    # Low balance warning first
    if low_balance:
        if ba["available"] <= 0:
            lines.append(
                f"\U0001F534 *Balance critical: {_fmt(ba['balance'])}*\n"
                f"Upcoming bills ({_fmt(ba['upcoming_bills'])}) exceed your balance.\n"
                f"Avoid ALL non-essential spending!"
            )
        else:
            lines.append(
                f"\u26A0\uFE0F *Low balance: {_fmt(ba['balance'])}*\n"
                f"After bills: {_fmt(ba['available'])} available\n"
                f"Daily limit: *{_fmt(ba['daily_allowance'])}* "
                f"for {ba['days_left']} days"
            )
        lines.append("")

    for b in budget_warnings:
        adj = ba["adjusted"].get(b["category"], b["remaining"])
        lines.append(
            f"{b['emoji']} *{b['category']}*: "
            f"{_fmt(b['spent'])} / {_fmt(b['limit'])} ({b['pct']:.0f}%) "
            f"— can spend {_fmt(adj)}"
        )

    try:
        await context.bot.send_message(
            chat_id=chat_id, text="\n".join(lines), parse_mode="Markdown"
        )
        logger.info("Daily budget alert sent to chat %s.", chat_id)
    except Exception as exc:
        logger.error("Failed to send daily alert: %s", exc)


async def _weekly_digest(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sunday 20:00 enhanced weekly digest."""
    chat_id = _resolve_chat_id()
    if chat_id is None:
        logger.warning("Weekly digest skipped: no chat ID registered.")
        return

    try:
        conn = _get_conn()
        _init_snus_table(conn)
        week = _week_summary(conn)
        budgets = _budget_status(conn)
        bal = _latest_balance(conn)
        snus_info = _snus_streak(conn)
        goals = _load_goals(conn)
        df = _load_transactions_df(conn)
        budgets_df = _load_budgets_df(conn)
        conn.close()
    except Exception as exc:
        logger.error("Weekly digest DB error: %s", exc)
        return

    lines = [
        "\U0001F4CB *Weekly Digest*",
        f"{week['start']} to {week['end']}",
        "",
        f"Total spent: {_fmt(week['total_spent'])}",
        f"Daily average: {_fmt(week['daily_avg'])}",
        f"Transactions: {week['tx_count']}",
    ]

    if week["top_categories"]:
        lines.append("")
        lines.append("*Top categories:*")
        for i, (cat, amt) in enumerate(week["top_categories"].items(), 1):
            lines.append(f"  {i}. {cat}: {_fmt(amt)}")

    if budgets:
        lines.append("")
        lines.append("*Budget status:*")
        for b in budgets:
            lines.append(
                f"  {b['emoji']} {b['category']}: {b['pct']:.0f}% "
                f"({_fmt(b['spent'])} / {_fmt(b['limit'])})"
            )

    if bal is not None:
        lines.append("")
        bal_emoji = "\U0001F7E2" if bal >= 0 else "\U0001F534"
        lines.append(f"{bal_emoji} Balance: {_fmt(bal)}")

    # Snus status
    if snus_info["total_clean"] > 0:
        lines.append("")
        lines.append(
            f"\U0001F6AD Snus: {snus_info['current']}-day streak "
            f"(saved {_fmt(snus_info['saved'])} total)"
        )

    # Goals
    if goals:
        lines.append("")
        lines.append("*Goals:*")
        for g in goals:
            pct = (g["saved"] / g["target"] * 100) if g["target"] > 0 else 0
            lines.append(f"  {g['name']}: {_progress_bar(pct, 8)}")

    # XP
    xp = compute_xp(df, budgets_df)
    level = get_level(xp)
    title = LEVELS.get(level, "Unknown")
    lines.append("")
    lines.append(f"\u2B50 Level {level}: {title} ({xp:,} XP)")

    try:
        await context.bot.send_message(
            chat_id=chat_id, text="\n".join(lines), parse_mode="Markdown"
        )
        logger.info("Weekly digest sent to chat %s.", chat_id)
    except Exception as exc:
        logger.error("Failed to send weekly digest: %s", exc)


# ---------------------------------------------------------------------------
# Application bootstrap
# ---------------------------------------------------------------------------

def main() -> None:
    """Start the Telegram bot."""
    token = _resolve_token()

    # Initialize snus table on startup
    try:
        conn = _get_conn()
        _init_snus_table(conn)
        conn.close()
    except Exception as exc:
        logger.warning("Could not init snus table: %s", exc)

    app = Application.builder().token(token).build()

    # Register command handlers
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_menu))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("week", cmd_week))
    app.add_handler(CommandHandler("budget", cmd_budget))
    app.add_handler(CommandHandler("balance", cmd_balance))
    app.add_handler(CommandHandler("add", cmd_add))
    app.add_handler(CommandHandler("ask", cmd_ask))
    app.add_handler(CommandHandler("snus", cmd_snus))
    app.add_handler(CommandHandler("goals", cmd_goals))
    app.add_handler(CommandHandler("xp", cmd_xp))
    app.add_handler(CommandHandler("alerts", cmd_alerts))
    app.add_handler(CommandHandler("shifts", cmd_shifts))
    app.add_handler(CommandHandler("deals", cmd_deals))
    app.add_handler(CommandHandler("smartlist", cmd_smartlist))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("sync", _cmd_sync))

    # Callback query handler for inline keyboards
    app.add_handler(CallbackQueryHandler(handle_callback))

    # Free text message handler (lowest priority)
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, handle_text
    ))

    # Schedule recurring jobs
    job_queue = app.job_queue

    if job_queue is not None:
        # Morning briefing at 08:00
        job_queue.run_daily(
            _morning_briefing,
            time=datetime.now().replace(
                hour=MORNING_BRIEF_HOUR,
                minute=MORNING_BRIEF_MINUTE,
                second=0, microsecond=0,
            ).time(),
            name="morning_briefing",
        )

        # Snus check-in at 20:00
        job_queue.run_daily(
            _snus_daily_checkin,
            time=datetime.now().replace(
                hour=SNUS_CHECKIN_HOUR,
                minute=SNUS_CHECKIN_MINUTE,
                second=0, microsecond=0,
            ).time(),
            name="snus_checkin",
        )

        # Daily budget alert at 21:00
        job_queue.run_daily(
            _daily_budget_alert,
            time=datetime.now().replace(
                hour=DAILY_ALERT_HOUR,
                minute=DAILY_ALERT_MINUTE,
                second=0, microsecond=0,
            ).time(),
            name="daily_budget_alert",
        )

        # Weekly digest every Sunday at 20:00
        job_queue.run_daily(
            _weekly_digest,
            time=datetime.now().replace(
                hour=WEEKLY_DIGEST_HOUR,
                minute=WEEKLY_DIGEST_MINUTE,
                second=0, microsecond=0,
            ).time(),
            days=(WEEKLY_DIGEST_DAY,),
            name="weekly_digest",
        )

        # Price drop alert at 10:00
        job_queue.run_daily(
            _price_drop_alert,
            time=datetime.now().replace(
                hour=PRICE_DROP_HOUR,
                minute=PRICE_DROP_MINUTE,
                second=0, microsecond=0,
            ).time(),
            name="price_drop_alert",
        )

        # Nightly bank sync at 03:00
        job_queue.run_daily(
            _nightly_bank_sync,
            time=datetime.now().replace(
                hour=BANK_SYNC_HOUR,
                minute=BANK_SYNC_MINUTE,
                second=0, microsecond=0,
            ).time(),
            name="nightly_bank_sync",
        )

        logger.info(
            "Scheduled jobs: morning %02d:%02d, snus %02d:%02d, "
            "alert %02d:%02d, weekly Sun %02d:%02d, price_drops %02d:%02d, "
            "bank_sync %02d:%02d.",
            MORNING_BRIEF_HOUR, MORNING_BRIEF_MINUTE,
            SNUS_CHECKIN_HOUR, SNUS_CHECKIN_MINUTE,
            DAILY_ALERT_HOUR, DAILY_ALERT_MINUTE,
            WEEKLY_DIGEST_HOUR, WEEKLY_DIGEST_MINUTE,
            PRICE_DROP_HOUR, PRICE_DROP_MINUTE,
            BANK_SYNC_HOUR, BANK_SYNC_MINUTE,
        )
    else:
        logger.warning(
            "JobQueue not available. Install python-telegram-bot[job-queue] "
            "for scheduled alerts."
        )

    logger.info("Bot starting... (polling)")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
