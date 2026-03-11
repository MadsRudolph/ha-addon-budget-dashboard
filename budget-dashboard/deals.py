"""eTilbudsavis (Tjek) grocery deals API module.

Standalone module with no Streamlit dependency — used by both
the dashboard and the Telegram bot.

The v2 API works without authentication for read-only offer searches.
Base URL: https://api.etilbudsavis.dk/v2
"""

import logging
import re
from datetime import datetime, date, timedelta

import requests

logger = logging.getLogger(__name__)

API_BASE = "https://api.etilbudsavis.dk/v2"

# Default location: DTU / Lyngby
DEFAULT_LAT = 55.786
DEFAULT_LNG = 12.524
DEFAULT_RADIUS = 10000  # meters


def search_offers(
    query: str,
    lat: float = DEFAULT_LAT,
    lng: float = DEFAULT_LNG,
    radius: int = DEFAULT_RADIUS,
    limit: int = 20,
) -> list[dict]:
    """Search eTilbudsavis for offers matching query near location.

    No API key required — the v2 endpoint is publicly accessible.

    Returns list of dicts with keys:
        heading, price, pre_price, discount_pct, dealer,
        valid_from, valid_until, quantity, image_url
    """
    resp = requests.get(
        f"{API_BASE}/offers/search",
        params={
            "query": query,
            "r_lat": lat,
            "r_lng": lng,
            "r_radius": radius,
            "r_locale": "da_DK",
            "limit": limit,
        },
        timeout=10,
    )
    resp.raise_for_status()
    raw_offers = resp.json()

    offers = []
    for o in raw_offers:
        pricing = o.get("pricing", {})
        price = pricing.get("price")
        pre_price = pricing.get("pre_price")

        # Calculate discount percentage
        discount_pct = 0.0
        if price is not None and pre_price is not None and pre_price > 0:
            discount_pct = round((1 - price / pre_price) * 100, 1)

        # Quantity info
        qty = o.get("quantity", {})
        pieces = qty.get("pieces", {}) or {}
        size = qty.get("size", {}) or {}
        qty_from = pieces.get("from")
        qty_to = pieces.get("to")
        quantity = None
        if qty_from and qty_to and qty_from == qty_to:
            quantity = str(qty_from)
        elif qty_from and qty_to:
            quantity = f"{qty_from}-{qty_to}"
        elif qty_from:
            quantity = str(qty_from)
        # Fall back to size info (e.g. "500 g")
        if not quantity and size.get("from"):
            unit = (qty.get("unit", {}) or {}).get("symbol", "")
            quantity = f"{size['from']}{' ' + unit if unit else ''}"

        # Images
        images = o.get("images", {})
        image_url = images.get("zoom") or images.get("view") or images.get("thumb")

        dealer = o.get("dealer", {}) or {}
        branding = o.get("branding", {}) or {}

        offers.append({
            "heading": o.get("heading", ""),
            "price": price,
            "pre_price": pre_price,
            "discount_pct": discount_pct,
            "dealer": dealer.get("name") or branding.get("name", ""),
            "valid_from": o.get("run_from", ""),
            "valid_until": o.get("run_till", ""),
            "quantity": quantity,
            "image_url": image_url,
        })

    # Sort by discount percentage (best deals first)
    offers.sort(key=lambda x: x["discount_pct"], reverse=True)
    return offers


def get_cheapest(
    items: list[str],
    **kwargs,
) -> dict[str, list[dict]]:
    """Search multiple items, return top 3 deals for each.

    Returns dict mapping item name -> list of top offers.
    """
    results = {}
    for item in items:
        try:
            offers = search_offers(item, limit=5, **kwargs)
            results[item] = offers[:3]
        except Exception as exc:
            logger.warning("Failed to search for '%s': %s", item, exc)
            results[item] = []
    return results


def test_connection(
    lat: float = DEFAULT_LAT,
    lng: float = DEFAULT_LNG,
) -> tuple[bool, str]:
    """Test API connectivity. Returns (success, message)."""
    try:
        resp = requests.get(
            f"{API_BASE}/offers/search",
            params={
                "query": "mælk",
                "r_lat": lat,
                "r_lng": lng,
                "r_radius": 5000,
                "r_locale": "da_DK",
                "limit": 1,
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        count = len(data)
        return True, f"Connected! Got {count} result(s) for test query."
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        return False, f"HTTP {status}: {exc}"
    except requests.exceptions.ConnectionError:
        return False, "Connection failed. Check your internet connection."
    except requests.exceptions.Timeout:
        return False, "Request timed out."
    except Exception as exc:
        return False, f"Error: {exc}"


def format_offer_text(offer: dict, index: int = 0) -> str:
    """Format a single offer for Telegram (plain text with Markdown)."""
    lines = []
    heading = offer["heading"] or "Unknown item"
    dealer = offer["dealer"] or "Unknown store"
    price = offer["price"]
    pre_price = offer["pre_price"]
    discount = offer["discount_pct"]

    # Title line
    if index > 0:
        lines.append(f"{index}. *{heading}*")
    else:
        lines.append(f"*{heading}*")

    # Price line
    price_str = f"{price:,.2f} DKK".replace(",", ".") if price else "?"
    if pre_price and discount > 0:
        pre_str = f"{pre_price:,.2f}".replace(",", ".")
        lines.append(f"   {dealer} — {price_str} (was {pre_str} — {discount:.0f}% off)")
    else:
        lines.append(f"   {dealer} — {price_str}")

    # Validity
    valid_from = offer.get("valid_from", "")
    valid_until = offer.get("valid_until", "")
    if valid_from and valid_until:
        try:
            vf = datetime.fromisoformat(valid_from.replace("Z", "+00:00"))
            vt = datetime.fromisoformat(valid_until.replace("Z", "+00:00"))
            lines.append(f"   Valid: {vf.strftime('%d %b')} - {vt.strftime('%d %b')}")
        except (ValueError, AttributeError):
            pass

    # All-time low badge
    if offer.get("is_all_time_low"):
        lines.append("   \U0001F525 ALL-TIME LOW!")

    return "\n".join(lines)


# ──────────────────────────── Recording ────────────────────────────

def record_prices(conn, offers):
    """Record observed prices into price_history (INSERT OR IGNORE)."""
    today = date.today().isoformat()
    for o in offers:
        heading = (o.get("heading") or "").lower().strip()
        dealer = o.get("dealer") or ""
        price = o.get("price")
        if not heading or price is None:
            continue
        try:
            conn.execute(
                "INSERT OR IGNORE INTO price_history "
                "(product_name, dealer, price, pre_price, quantity, observed_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (heading, dealer, price, o.get("pre_price"), o.get("quantity"), today),
            )
        except Exception as exc:
            logger.debug("record_prices skip: %s", exc)
    conn.commit()


def record_search(conn, query, source="dashboard", count=0):
    """Log a search and upsert the shopping_items vocabulary."""
    now = datetime.now().isoformat(timespec="seconds")
    conn.execute(
        "INSERT INTO deal_searches (query, source, result_count, searched_at) VALUES (?, ?, ?, ?)",
        (query.lower().strip(), source, count, now),
    )
    # Upsert shopping_items
    row = conn.execute(
        "SELECT id, search_count FROM shopping_items WHERE name = ?",
        (query.lower().strip(),),
    ).fetchone()
    if row:
        conn.execute(
            "UPDATE shopping_items SET search_count = search_count + 1, last_searched = ? WHERE id = ?",
            (now, row[0]),
        )
    else:
        conn.execute(
            "INSERT INTO shopping_items (name, last_searched) VALUES (?, ?)",
            (query.lower().strip(), now),
        )
    conn.commit()


def smart_search(conn, query, source="dashboard", **kwargs):
    """Wrapper: search_offers + record + annotate with all-time-low flag."""
    offers = search_offers(query, **kwargs)
    record_search(conn, query, source, len(offers))
    record_prices(conn, offers)

    # Annotate each offer with all-time-low status
    for o in offers:
        heading = (o.get("heading") or "").lower().strip()
        price = o.get("price")
        if heading and price is not None:
            atl = get_all_time_low(conn, heading)
            o["is_all_time_low"] = atl is not None and price <= atl["price"]
        else:
            o["is_all_time_low"] = False

    return offers


# ──────────────────────────── Price Intelligence ────────────────────────────

def get_price_history(conn, product, days=90):
    """Return list of {date, price, dealer} for a product over last N days."""
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    rows = conn.execute(
        "SELECT observed_at, price, dealer FROM price_history "
        "WHERE product_name = ? AND observed_at >= ? ORDER BY observed_at",
        (product.lower().strip(), cutoff),
    ).fetchall()
    return [{"date": r[0], "price": r[1], "dealer": r[2]} for r in rows]


def get_all_time_low(conn, product):
    """Return {price, dealer, date} of the lowest recorded price, or None."""
    row = conn.execute(
        "SELECT price, dealer, observed_at FROM price_history "
        "WHERE product_name = ? AND price IS NOT NULL "
        "ORDER BY price ASC LIMIT 1",
        (product.lower().strip(),),
    ).fetchone()
    if row:
        return {"price": row[0], "dealer": row[1], "date": row[2]}
    return None


def find_price_drops(conn, threshold_pct=10):
    """Find products currently at/near all-time low vs their history.

    Returns list of {product, current_price, dealer, all_time_low, avg_price, drop_pct}.
    """
    # Get today's prices (most recent observation per product)
    today = date.today().isoformat()
    week_ago = (date.today() - timedelta(days=7)).isoformat()
    recent = conn.execute(
        "SELECT product_name, price, dealer FROM price_history "
        "WHERE observed_at >= ? ORDER BY observed_at DESC",
        (week_ago,),
    ).fetchall()

    seen = {}
    for name, price, dealer in recent:
        if name not in seen and price is not None:
            seen[name] = {"price": price, "dealer": dealer}

    drops = []
    for product, current in seen.items():
        # Get average historical price
        row = conn.execute(
            "SELECT AVG(price), MIN(price) FROM price_history "
            "WHERE product_name = ? AND price IS NOT NULL",
            (product,),
        ).fetchone()
        if not row or row[0] is None:
            continue
        avg_price, min_price = row[0], row[1]
        if avg_price <= 0:
            continue
        drop_pct = (1 - current["price"] / avg_price) * 100
        if drop_pct >= threshold_pct:
            drops.append({
                "product": product,
                "current_price": current["price"],
                "dealer": current["dealer"],
                "all_time_low": min_price,
                "avg_price": round(avg_price, 2),
                "drop_pct": round(drop_pct, 1),
            })

    drops.sort(key=lambda x: x["drop_pct"], reverse=True)
    return drops


# ──────────────────────────── Habit Analysis ────────────────────────────

# Merchant name normalization patterns
_MERCHANT_PATTERNS = [
    (r"(?:MobilePay\s+)?Coop\s*(?:365|App)?", "Coop"),
    (r"(?:MobilePay\s+)?REMA\s*1000", "REMA 1000"),
    (r"(?:MobilePay\s+)?(?:Føtex|F[Ø@]TEX)", "Føtex"),
    (r"(?:MobilePay\s+)?Netto", "Netto"),
    (r"(?:MobilePay\s+)?Lidl", "Lidl"),
    (r"(?:MobilePay\s+)?ALDI", "ALDI"),
    (r"(?:MobilePay\s+)?Bilka", "Bilka"),
    (r"(?:MobilePay\s+)?Meny", "Meny"),
    (r"(?:MobilePay\s+)?Irma", "Irma"),
    (r"(?:MobilePay\s+)?Spar\b", "Spar"),
    (r"(?:MobilePay\s+)?Fakta", "Fakta"),
]


def extract_merchant(description):
    """Normalize a transaction description to a merchant name."""
    for pattern, name in _MERCHANT_PATTERNS:
        if re.search(pattern, description, re.IGNORECASE):
            return name
    return description.strip()[:30]


def analyze_grocery_habits(conn):
    """Scan Dagligvarer transactions, populate purchase_patterns.

    Returns {top_stores, avg_weekly_spend, shopping_days, avg_trip_amount}.
    """
    rows = conn.execute(
        "SELECT date, description, amount FROM transactions "
        "WHERE category = 'Dagligvarer' AND amount < 0 ORDER BY date"
    ).fetchall()

    if not rows:
        return {"top_stores": [], "avg_weekly_spend": 0, "shopping_days": {}, "avg_trip_amount": 0}

    # Aggregate by merchant
    merchant_visits = {}
    day_counts = {}
    total_spend = 0.0

    for dt_str, desc, amount in rows:
        merchant = extract_merchant(desc)
        abs_amount = abs(amount)
        total_spend += abs_amount

        if merchant not in merchant_visits:
            merchant_visits[merchant] = {"amounts": [], "dates": []}
        merchant_visits[merchant]["amounts"].append(abs_amount)
        merchant_visits[merchant]["dates"].append(dt_str)

        # Day-of-week counting
        try:
            dow = datetime.strptime(dt_str, "%Y-%m-%d").strftime("%A")
            day_counts[dow] = day_counts.get(dow, 0) + 1
        except ValueError:
            pass

    # Populate purchase_patterns table
    now = datetime.now().isoformat(timespec="seconds")
    for merchant, data in merchant_visits.items():
        visit_count = len(data["amounts"])
        avg_amount = sum(data["amounts"]) / visit_count
        dates_sorted = sorted(data["dates"])
        if len(dates_sorted) > 1:
            deltas = []
            for i in range(1, len(dates_sorted)):
                try:
                    d1 = datetime.strptime(dates_sorted[i - 1], "%Y-%m-%d")
                    d2 = datetime.strptime(dates_sorted[i], "%Y-%m-%d")
                    deltas.append((d2 - d1).days)
                except ValueError:
                    pass
            avg_days = sum(deltas) / len(deltas) if deltas else 0
        else:
            avg_days = 0

        conn.execute(
            "INSERT OR REPLACE INTO purchase_patterns "
            "(merchant, avg_amount, visit_count, avg_days_between, last_visit, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (merchant, round(avg_amount, 2), visit_count, round(avg_days, 1),
             dates_sorted[-1] if dates_sorted else None, now),
        )
    conn.commit()

    # Build top stores
    top_stores = sorted(
        [{"name": m, "visits": len(d["amounts"]), "total": sum(d["amounts"])}
         for m, d in merchant_visits.items()],
        key=lambda x: x["total"], reverse=True,
    )

    # Weekly spend
    if rows:
        first_date = datetime.strptime(rows[0][0], "%Y-%m-%d")
        last_date = datetime.strptime(rows[-1][0], "%Y-%m-%d")
        weeks = max(1, (last_date - first_date).days / 7)
        avg_weekly = total_spend / weeks
    else:
        avg_weekly = 0

    return {
        "top_stores": top_stores[:10],
        "avg_weekly_spend": round(avg_weekly, 0),
        "shopping_days": day_counts,
        "avg_trip_amount": round(total_spend / len(rows), 0) if rows else 0,
    }


def get_grocery_budget_status(conn):
    """Return {limit, spent, remaining, pct_used, days_left, daily_remaining}."""
    # Get budget limit
    row = conn.execute(
        "SELECT monthly_limit FROM budgets WHERE category = 'Dagligvarer'"
    ).fetchone()
    limit = row[0] if row else 0

    # Current month spend
    today = date.today()
    month_start = today.replace(day=1).isoformat()
    row = conn.execute(
        "SELECT COALESCE(SUM(ABS(amount)), 0) FROM transactions "
        "WHERE category = 'Dagligvarer' AND amount < 0 AND date >= ?",
        (month_start,),
    ).fetchone()
    spent = row[0] if row else 0

    remaining = max(0, limit - spent)
    pct_used = (spent / limit * 100) if limit > 0 else 0

    # Days left in month
    if today.month == 12:
        next_month = today.replace(year=today.year + 1, month=1, day=1)
    else:
        next_month = today.replace(month=today.month + 1, day=1)
    days_left = (next_month - today).days

    daily_remaining = remaining / days_left if days_left > 0 else 0

    return {
        "limit": limit,
        "spent": round(spent, 0),
        "remaining": round(remaining, 0),
        "pct_used": round(pct_used, 1),
        "days_left": days_left,
        "daily_remaining": round(daily_remaining, 0),
    }


# ──────────────────────────── Smart List ────────────────────────────

def get_frequent_searches(conn, top_n=10):
    """Return most-searched items from shopping_items."""
    rows = conn.execute(
        "SELECT name, search_count, is_staple, last_deal_price FROM shopping_items "
        "ORDER BY search_count DESC LIMIT ?",
        (top_n,),
    ).fetchall()
    return [
        {"name": r[0], "search_count": r[1], "is_staple": bool(r[2]), "last_deal_price": r[3]}
        for r in rows
    ]


def set_staple(conn, item_name, is_staple=True):
    """Mark/unmark an item as a staple."""
    conn.execute(
        "UPDATE shopping_items SET is_staple = ? WHERE name = ?",
        (1 if is_staple else 0, item_name.lower().strip()),
    )
    conn.commit()


def remove_shopping_item(conn, item_name):
    """Remove an item from shopping_items and its search/price history."""
    name = item_name.lower().strip()
    conn.execute("DELETE FROM shopping_items WHERE name = ?", (name,))
    conn.execute("DELETE FROM deal_searches WHERE query = ?", (name,))
    conn.execute("DELETE FROM price_history WHERE product_name = ?", (name,))
    conn.commit()


def clear_all_shopping_data(conn):
    """Wipe all search history, shopping items, and price history."""
    conn.execute("DELETE FROM shopping_items")
    conn.execute("DELETE FROM deal_searches")
    conn.execute("DELETE FROM price_history")
    conn.commit()


def add_to_shopping_list(conn, item_name, price=None, as_staple=False):
    """Add an item to shopping_items (upsert). Optionally mark as staple."""
    name = item_name.lower().strip()
    now = datetime.now().isoformat(timespec="seconds")
    row = conn.execute("SELECT id FROM shopping_items WHERE name = ?", (name,)).fetchone()
    if row:
        conn.execute(
            "UPDATE shopping_items SET search_count = search_count + 1, "
            "last_searched = ?, last_deal_price = COALESCE(?, last_deal_price), "
            "is_staple = MAX(is_staple, ?) WHERE id = ?",
            (now, price, 1 if as_staple else 0, row[0]),
        )
    else:
        conn.execute(
            "INSERT INTO shopping_items (name, last_searched, last_deal_price, is_staple) "
            "VALUES (?, ?, ?, ?)",
            (name, now, price, 1 if as_staple else 0),
        )
    conn.commit()


def generate_smart_list(conn, **api_kwargs):
    """Auto-generate a weekly shopping list from staples + frequent items.

    Returns {items: [{name, best_offer, reason}], estimated_total, budget_remaining, savings_vs_regular}.
    """
    # Collect staples + frequent items
    staples = conn.execute(
        "SELECT name FROM shopping_items WHERE is_staple = 1"
    ).fetchall()
    frequent = conn.execute(
        "SELECT name FROM shopping_items WHERE search_count >= 3 AND is_staple = 0 "
        "ORDER BY search_count DESC LIMIT 10"
    ).fetchall()

    candidates = []
    for r in staples:
        candidates.append({"name": r[0], "reason": "Staple item"})
    for r in frequent:
        candidates.append({"name": r[0], "reason": f"Frequently searched"})

    if not candidates:
        return {"items": [], "estimated_total": 0, "budget_remaining": 0, "savings_vs_regular": 0}

    # Deduplicate
    seen = set()
    unique = []
    for c in candidates:
        if c["name"] not in seen:
            seen.add(c["name"])
            unique.append(c)
    candidates = unique[:10]  # Max 10 API calls

    # Search current deals for each
    items = []
    estimated_total = 0
    savings = 0
    for c in candidates:
        try:
            offers = search_offers(c["name"], limit=3, **api_kwargs)
            record_prices(conn, offers)
        except Exception:
            offers = []

        if offers:
            best = offers[0]
            price = best.get("price") or 0
            estimated_total += price
            if best.get("pre_price") and best["pre_price"] > price:
                savings += best["pre_price"] - price

            # Score for sorting
            disc = best.get("discount_pct", 0)
            freq_row = conn.execute(
                "SELECT search_count, is_staple FROM shopping_items WHERE name = ?",
                (c["name"],),
            ).fetchone()
            freq_score = min(10, (freq_row[0] if freq_row else 1)) / 10
            staple_bonus = 0.2 if (freq_row and freq_row[1]) else 0
            score = disc * 0.5 + freq_score * 30 + staple_bonus * 100

            items.append({
                "name": c["name"],
                "best_offer": best,
                "reason": c["reason"],
                "score": score,
            })
            # Update last_deal_price
            conn.execute(
                "UPDATE shopping_items SET last_deal_price = ? WHERE name = ?",
                (price, c["name"]),
            )
        else:
            items.append({"name": c["name"], "best_offer": None, "reason": c["reason"], "score": 0})

    conn.commit()
    items.sort(key=lambda x: x["score"], reverse=True)

    budget = get_grocery_budget_status(conn)
    return {
        "items": items,
        "estimated_total": round(estimated_total, 2),
        "budget_remaining": budget["remaining"],
        "savings_vs_regular": round(savings, 2),
    }
