# Changelog

## [1.0.13] - 2026-03-11

### Added
- Realistic budget plan with saving targets (12,000 DKK/month total, ~19% savings rate)
- Expanded category rules: tandlæge, Klarna, kortgebyr, Kaffebønnen, S-Huset, SILVAN, GoodNotes, etc.
- Re-categorized 356 previously uncategorized transactions (rent, MobilePay supermarkets, subscriptions)

---

## [1.0.12] - 2026-03-11

### Fixed
- Income/spending KPIs now show last complete month instead of current incomplete month (salary arrives end of month)
- Added current month progress caption below KPIs

---

## [1.0.11] - 2026-03-11

### Added
- CSV upload button in Danske Bank Sync sidebar — imports bank CSV with real balances, then API sync handles new transactions going forward

---

## [1.0.10] - 2026-03-11

### Fixed
- Use ITBD (booked) balance instead of ITAV (available) for accurate balance matching

---

## [1.0.9] - 2026-03-11

### Fixed
- Net worth now uses real account balance from Enable Banking API instead of unreliable per-transaction balance
- Balance recalculation works backwards from real API balance for accurate history
- Sync starts from latest DB date to prevent CSV/API duplicate transactions

---

## [1.0.8] - 2026-03-11

### Changed
- Transaction history extended from 90 to 365 days
- Upsert logic: re-syncing now fixes balance=0 for previously imported transactions

---

## [1.0.7] - 2026-03-11

### Fixed
- Reverted to HA base image (Alpine + S6 overlay) — Debian python image lacked S6, preventing container startup

---

## [1.0.6] - 2026-03-11

### Fixed
- Net worth display showing 0 — now uses real balance from Enable Banking API

---

## [1.0.5] - 2026-03-10

### Added
- Enable Banking integration for Danske Bank auto-sync (free Restricted Mode)
- `bank_sync.py` module with JWT (RS256) authentication and MitID consent flow
- Enable Banking config options (`enablebanking_app_id`, `enablebanking_session_id`)
- PEM file auto-copy from `/share/enablebanking.pem` at container startup
- Nightly auto-sync at 03:00 and `/sync` Telegram command
- SHA-256 transaction deduplication

### Fixed
- Database initialization before transaction insert (was silently failing)

---

## [1.0.4] - 2026-02-16

### Fixed
- PDF export on Home Assistant (added `report_generator.py` and `fpdf2`)
- Budget categories adjusted to realistic 8,300 DKK/mo target

---

## [1.0.3] - 2026-02-16

### Added
- Telegram bot as smart financial companion (14 commands)
- AI-powered financial advisor via Anthropic API
- PDF monthly report export
- Bill calendar with upcoming payment tracking
- Quick-add transactions from Telegram
- Scheduled notifications (morning briefing, snus check-in, budget alerts, weekly digest)

---

## [1.0.2] - 2026-02-16

### Added
- Auto-categorization of transactions (18 Danish merchant patterns)
- Budget alerts and notifications
- Spending forecast
- Subscription detection
- Savings goals tracker
- Monthly report card
- XP/gamification system

---

## [1.0.1] - 2026-02-16

### Added
- Monthly budget tracker on Overview page
- Tax (Restskat) tab on Income & Loan page
- Google Calendar integration for shift tracking

---

## [1.0.0] - 2026-02-16

### Added
- Initial release: Streamlit budget dashboard
- SQLite database with transaction import
- S6 overlay services for Streamlit + Telegram bot
