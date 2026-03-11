import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime
import streamlit as st

DB_PATH = Path(__file__).parent / "budget.db"

@st.cache_resource
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            category TEXT NOT NULL,
            subcategory TEXT NOT NULL,
            description TEXT NOT NULL,
            amount REAL NOT NULL,
            balance REAL NOT NULL,
            import_hash TEXT UNIQUE NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_tx_date ON transactions(date);
        CREATE INDEX IF NOT EXISTS idx_tx_category ON transactions(category);
        CREATE INDEX IF NOT EXISTS idx_tx_hash ON transactions(import_hash);
        CREATE TABLE IF NOT EXISTS budgets (
            category TEXT PRIMARY KEY,
            monthly_limit REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS achievements (
            name TEXT PRIMARY KEY,
            unlocked_date TEXT,
            description TEXT
        );
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS savings_goals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            target REAL NOT NULL,
            saved REAL DEFAULT 0,
            deadline TEXT,
            created TEXT DEFAULT (date('now'))
        );

        CREATE TABLE IF NOT EXISTS deal_searches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'dashboard',
            result_count INTEGER DEFAULT 0,
            searched_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT NOT NULL,
            dealer TEXT NOT NULL,
            price REAL,
            pre_price REAL,
            quantity TEXT,
            observed_at TEXT DEFAULT (date('now')),
            UNIQUE(product_name, dealer, observed_at)
        );

        CREATE TABLE IF NOT EXISTS shopping_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            search_count INTEGER DEFAULT 1,
            last_searched TEXT,
            last_deal_price REAL,
            is_staple INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (date('now'))
        );

        CREATE TABLE IF NOT EXISTS purchase_patterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            merchant TEXT UNIQUE NOT NULL,
            avg_amount REAL,
            visit_count INTEGER DEFAULT 0,
            avg_days_between REAL,
            last_visit TEXT,
            preferred_store INTEGER DEFAULT 0,
            updated_at TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit()

@st.cache_data(ttl=60)
def load_transactions():
    conn = get_conn()
    df = pd.read_sql("SELECT * FROM transactions ORDER BY date", conn)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
        df["month"] = df["date"].dt.to_period("M").astype(str)
    return df

@st.cache_data(ttl=60)
def load_budgets():
    conn = get_conn()
    return pd.read_sql("SELECT * FROM budgets", conn)

def save_budget(conn, category, limit_val):
    conn.execute(
        "INSERT OR REPLACE INTO budgets (category, monthly_limit) VALUES (?, ?)",
        (category, limit_val),
    )
    conn.commit()
    load_budgets.clear()

@st.cache_data(ttl=60)
def load_achievements():
    conn = get_conn()
    return pd.read_sql("SELECT * FROM achievements", conn)

def unlock_achievement(conn, name, description):
    conn.execute(
        "INSERT OR IGNORE INTO achievements (name, unlocked_date, description) VALUES (?, ?, ?)",
        (name, datetime.now().strftime("%Y-%m-%d"), description),
    )
    conn.commit()
    load_achievements.clear()

def get_setting(conn, key, default=""):
    row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    return row[0] if row else default

def set_setting(conn, key, value):
    conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))
    conn.commit()


# ──────────────────────────── Savings Goals ────────────────────────────

@st.cache_data(ttl=60)
def load_goals():
    conn = get_conn()
    return pd.read_sql("SELECT * FROM savings_goals ORDER BY created", conn)


def save_goal(conn, name, target, deadline=None):
    conn.execute(
        "INSERT INTO savings_goals (name, target, deadline) VALUES (?, ?, ?)",
        (name, target, deadline),
    )
    conn.commit()
    load_goals.clear()


def update_goal_progress(conn, goal_id, saved):
    conn.execute("UPDATE savings_goals SET saved = ? WHERE id = ?", (saved, goal_id))
    conn.commit()
    load_goals.clear()


def delete_goal(conn, goal_id):
    conn.execute("DELETE FROM savings_goals WHERE id = ?", (goal_id,))
    conn.commit()
    load_goals.clear()


# ──────────────────────────── Transaction Insert ────────────────────────────

def insert_transactions(conn, rows):
    """Insert transaction dicts into the database with deduplication.

    Each row must have keys: date, category, subcategory, description, amount, balance.
    Returns (new_count, skip_count).
    """
    import hashlib

    new_count = skip_count = 0
    seen_counts = {}
    for row in rows:
        d = row["date"]          # already "YYYY-MM-DD"
        amt = row["amount"]
        desc = row["description"]
        base_key = f"{d}|{amt}|{desc}"
        seen_counts[base_key] = seen_counts.get(base_key, 0) + 1
        h = hashlib.sha256(
            f"{d}|{amt}|{desc}#{seen_counts[base_key]}".encode()
        ).hexdigest()
        balance = row.get("balance", 0)
        try:
            cur = conn.execute(
                "INSERT INTO transactions "
                "(date, category, subcategory, description, amount, balance, import_hash) "
                "VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(import_hash) DO UPDATE SET balance = excluded.balance "
                "WHERE excluded.balance != 0 AND transactions.balance = 0",
                (d, row["category"], row["subcategory"], desc, amt,
                 balance, h),
            )
            if cur.rowcount > 0:
                new_count += 1
            else:
                skip_count += 1
        except Exception:
            skip_count += 1
    conn.commit()
    load_transactions.clear()
    return new_count, skip_count
