# Changelog

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
