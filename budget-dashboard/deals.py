"""eTilbudsavis (Tjek) grocery deals API module.

Standalone module with no Streamlit dependency — used by both
the dashboard and the Telegram bot.

The v2 API works without authentication for read-only offer searches.
Base URL: https://api.etilbudsavis.dk/v2
"""

import logging
from datetime import datetime

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

    return "\n".join(lines)
