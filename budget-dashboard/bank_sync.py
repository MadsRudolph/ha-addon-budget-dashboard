#!/usr/bin/env python3
"""Enable Banking API sync for Danske Bank.

Usage:
    python bank_sync.py --link   # One-time: authorise bank account via MitID
    python bank_sync.py          # Sync transactions to SQLite
"""

import argparse
import os
import re
import sqlite3
import time
import webbrowser
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import jwt
import requests
from dotenv import load_dotenv, set_key

# Load .env from same directory as this script
APP_DIR = Path(__file__).parent
ENV_PATH = APP_DIR / ".env"
load_dotenv(ENV_PATH)

BASE_URL = "https://api.enablebanking.com"
ASPSP_NAME = "Danske Bank"
ASPSP_COUNTRY = "DK"
DAYS_BACK = 365
TIMEOUT = 30
CALLBACK_PORT = 8321
REDIRECT_URI = f"https://localhost:{CALLBACK_PORT}/callback"

DB_PATH = APP_DIR / "budget.db"

# ── Category rules (keep in sync with dashboard.py CATEGORY_RULES) ──

CATEGORY_RULES = [
    (r"P\.?O\.?\s*Pedersen|Kollegiet", "Bolig", "Husleje"),
    (r"Norlys", "Bolig", "Forsyning"),
    (r"Rejsekort|DSB|MOB\.PAY\*DSB", "Transport", "Offentlig transport"),
    (r"Coop|REMA|F[Ø@]TEX|Føtex|Netto|Lidl|ALDI|Fakta|Meny|Irma|COOP365|Bilka|Spar\b",
     "Dagligvarer", "Supermarked"),
    (r"McDonalds|Burger King|Max Burgers|Sunset Boulevard", "Dagligvarer", "Fast food"),
    (r"Wolt|Just.?Eat|Hungry", "Fornøjelser og fritid", "Takeaway"),
    (r"Kaffestuen|Starbucks|Espresso|Cafe|Café", "Fornøjelser og fritid", "Café"),
    (r"Durumbar|Kebap|Shawarma|Pizza|Pakhus", "Fornøjelser og fritid", "Restaurant"),
    (r"WINTER|Bar |Bodega|Pub\b", "Fornøjelser og fritid", "Bar"),
    (r"TIDAL|Spotify|Netflix|Disney|HBO|YouTube|Viaplay", "Øvrige udgifter", "Streaming"),
    (r"Microsoft|OPENAI|CLAUDE|ANTHROPIC|Google\s*\*?Google\s*One|Google\s*Play",
     "Øvrige udgifter", "Abonnement"),
    (r"Carlsen|Barbershop|Frisør|frisør", "Tøj, sko og personlig pleje", "Frisør"),
    (r"Matas|Apotek|Normal\b|N.rrebro Apotek", "Tøj, sko og personlig pleje", "Personlig pleje"),
    (r"temashop|Brinck Elektronik|ESSENCEVAULT", "Øvrige udgifter", "Shopping"),
    (r"forsikri|Tryg\b", "Personforsikringer", "Forsikring"),
    (r"Kr.ftens Bek.mpelse|Red Barnet|UNICEF", "Øvrige udgifter", "Donation"),
    (r"Til Opsparing|Fra Opsparing", "Opsparing", "Overførsel"),
    (r"^MobilePay\s+[A-ZÆØÅ][a-zæøå]+\s+[A-ZÆØÅ]", "Øvrige udgifter", "MobilePay overførsel"),
]


def _categorize(description: str) -> tuple[str, str]:
    """Match description against category rules. Returns (category, subcategory)."""
    desc_stripped = re.sub(r"^MobilePay\s+", "", description)
    for pattern, cat, subcat in CATEGORY_RULES:
        if re.search(pattern, description, re.IGNORECASE) or \
           re.search(pattern, desc_stripped, re.IGNORECASE):
            return cat, subcat
    return "Ukategoriseret", ""


# ── Enable Banking Auth ──

def _get_app_id() -> str:
    """Get application ID from environment."""
    app_id = os.environ.get("ENABLEBANKING_APP_ID", "")
    if not app_id:
        raise RuntimeError(
            "Missing ENABLEBANKING_APP_ID. Set it in your .env file."
        )
    return app_id


def _load_private_key() -> str:
    """Load the RSA private key from the .pem file."""
    pem_name = os.environ.get("ENABLEBANKING_PEM_FILE", "enablebanking.pem")
    pem_path = APP_DIR / pem_name
    if not pem_path.exists():
        raise RuntimeError(
            f"Private key not found at {pem_path}. "
            "Download it from Enable Banking Control Panel and place it "
            "in the app directory."
        )
    return pem_path.read_text()


def _make_jwt() -> str:
    """Create a signed JWT for Enable Banking API authentication."""
    app_id = _get_app_id()
    private_key = _load_private_key()
    now = int(time.time())
    payload = {
        "iss": "enablebanking.com",
        "aud": "api.enablebanking.com",
        "iat": now,
        "exp": now + 3600,
    }
    return jwt.encode(payload, private_key, algorithm="RS256",
                      headers={"kid": app_id})


def _auth_headers() -> dict:
    """Return authorization headers for API calls."""
    return {"Authorization": f"Bearer {_make_jwt()}"}


# ── Enable Banking API ──

def start_auth() -> dict:
    """Start bank authorisation. Returns dict with 'url' and auth metadata."""
    valid_until = (datetime.now() + timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%SZ")
    resp = requests.post(
        f"{BASE_URL}/auth",
        headers=_auth_headers(),
        json={
            "access": {"valid_until": valid_until},
            "aspsp": {"name": ASPSP_NAME, "country": ASPSP_COUNTRY},
            "state": "budgetbot-link",
            "redirect_url": REDIRECT_URI,
            "psu_type": "personal",
        },
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def create_session(auth_code: str) -> dict:
    """Exchange auth code for a session. Returns dict with session_id and accounts."""
    resp = requests.post(
        f"{BASE_URL}/sessions",
        headers=_auth_headers(),
        json={"code": auth_code},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def get_session(session_id: str) -> dict:
    """Get session info including accounts.

    Raises RuntimeError if session is expired or invalid.
    """
    resp = requests.get(
        f"{BASE_URL}/sessions/{session_id}",
        headers=_auth_headers(),
        timeout=TIMEOUT,
    )
    if resp.status_code in (401, 403, 404):
        raise RuntimeError(
            "Bank session expired or invalid. "
            "Run 'python bank_sync.py --link' to re-authorise."
        )
    resp.raise_for_status()
    return resp.json()


def fetch_transactions(account_uid: str) -> list[dict]:
    """Fetch booked transactions for an account with pagination."""
    date_from = (datetime.now() - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%d")
    all_txns = []
    params = {"date_from": date_from}

    while True:
        resp = requests.get(
            f"{BASE_URL}/accounts/{account_uid}/transactions",
            headers=_auth_headers(),
            params=params,
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        all_txns.extend(data.get("transactions", []))

        # Pagination: continue until no continuation_key
        cont_key = data.get("continuation_key")
        if not cont_key:
            break
        params = {"continuation_key": cont_key}

    return all_txns


def normalize_transactions(raw: list[dict]) -> list[dict]:
    """Convert Enable Banking transaction format to our DB schema dicts."""
    rows = []
    for tx in raw:
        # Only include booked transactions
        if tx.get("status") not in ("BOOK", "BOOKD", None):
            continue

        # Date: prefer booking_date, fall back to value_date
        tx_date = tx.get("booking_date") or tx.get("value_date", "")
        if not tx_date:
            continue

        # Amount
        amount_info = tx.get("transaction_amount", {})
        try:
            amount = float(amount_info.get("amount", 0))
        except (ValueError, TypeError):
            continue

        # Make expenses negative if credit_debit_indicator says debit
        if tx.get("credit_debit_indicator") == "DBIT" and amount > 0:
            amount = -amount

        # Description: try multiple fields
        remittance = tx.get("remittance_information", [])
        description = (
            (remittance[0] if remittance else "")
            or (tx.get("creditor") or {}).get("name", "")
            or (tx.get("debtor") or {}).get("name", "")
            or "Unknown"
        )

        # Auto-categorize
        cat, subcat = _categorize(description)

        # Balance after transaction (if provided by API)
        bal_info = tx.get("balance_after_transaction", {})
        try:
            balance = float(bal_info.get("amount", 0))
        except (ValueError, TypeError):
            balance = 0

        rows.append({
            "date": tx_date,
            "category": cat,
            "subcategory": subcat,
            "description": description,
            "amount": amount,
            "balance": balance,
        })
    return rows


def sync() -> int:
    """Full sync: session -> accounts -> transactions -> SQLite.

    Returns count of newly inserted transactions.
    """
    session_id = os.environ.get("ENABLEBANKING_SESSION_ID", "")
    if not session_id:
        raise RuntimeError(
            "No ENABLEBANKING_SESSION_ID set. "
            "Run 'python bank_sync.py --link' to authorise your bank account first."
        )

    # Get accounts from session (raises RuntimeError if expired)
    session = get_session(session_id)
    # accounts is a list of UID strings; full data in accounts_data
    account_uids = session.get("accounts", [])
    if not account_uids:
        raise RuntimeError(
            "No accounts found in session. "
            "Re-run 'python bank_sync.py --link' to authorise again."
        )

    all_raw = []
    for uid in account_uids:
        if isinstance(uid, str):
            all_raw.extend(fetch_transactions(uid))
        elif isinstance(uid, dict):
            # Fallback: if accounts contains dicts (e.g. from create_session)
            acct_uid = uid.get("uid", "")
            if acct_uid:
                all_raw.extend(fetch_transactions(acct_uid))

    rows = normalize_transactions(all_raw)
    if not rows:
        return 0

    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    try:
        from db import init_db, insert_transactions
        init_db(conn)
        new_count, _ = insert_transactions(conn, rows)
    finally:
        conn.close()

    return new_count


# ── One-time link flow ──

def _extract_code_from_url(url: str) -> str | None:
    """Extract the 'code' query parameter from a callback URL."""
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    codes = params.get("code", [])
    return codes[0] if codes else None


def link() -> None:
    """One-time setup: authorise bank account via MitID."""
    _get_app_id()  # Validate credentials early

    print("\n  Starting bank authorisation...")
    auth = start_auth()
    auth_url = auth.get("url", "")

    print(f"\n  Opening browser for MitID login...")
    print(f"  (If browser doesn't open, visit: {auth_url})\n")
    webbrowser.open(auth_url)

    # After MitID login, the browser redirects to our callback URL.
    # Since we use HTTPS with a self-signed cert, the browser will show
    # an error page — but the URL bar will contain the auth code.
    print("  After completing MitID login, your browser will redirect to")
    print("  a page that won't load (this is normal).")
    print()
    print("  Copy the FULL URL from your browser's address bar and paste it here:")
    print()
    callback_url = input("  URL> ").strip()

    code = _extract_code_from_url(callback_url)
    if not code:
        print("\n  ERROR: No authorisation code found in URL.")
        print("  Expected URL like: https://localhost:8321/callback?code=...")
        return

    print("  Exchanging code for session...")
    session = create_session(code)
    session_id = session.get("session_id", "")
    accounts = session.get("accounts", [])

    print(f"\n  Session created! Found {len(accounts)} account(s).")
    for acct in accounts:
        iban = (acct.get("account_id") or {}).get("iban", "")
        name = acct.get("name", "")
        label = f"{name} ({iban})" if iban else name or acct.get("uid", "unknown")
        print(f"    - {label}")

    # Auto-save session ID to .env
    set_key(str(ENV_PATH), "ENABLEBANKING_SESSION_ID", session_id)
    os.environ["ENABLEBANKING_SESSION_ID"] = session_id
    print(f"\n  ENABLEBANKING_SESSION_ID saved to .env")
    print("  You can now run 'python bank_sync.py' to sync transactions.\n")


def main():
    parser = argparse.ArgumentParser(description="Danske Bank sync via Enable Banking")
    parser.add_argument("--link", action="store_true",
                        help="One-time: authorise bank account via MitID")
    args = parser.parse_args()

    if args.link:
        link()
    else:
        count = sync()
        print(f"Sync complete: {count} new transaction(s) inserted.")


if __name__ == "__main__":
    main()
