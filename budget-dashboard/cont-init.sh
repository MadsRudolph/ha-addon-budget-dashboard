#!/usr/bin/with-contenv bash
# S6 init script — runs once at container startup

echo "=== Budget Dashboard: Initializing ==="

# Persistent database in /data/
# If /share/budget.db is newer (or /data/ doesn't exist), use /share/ copy
if [ ! -f /data/budget.db ]; then
    if [ -f /share/budget.db ]; then
        echo "Copying budget.db from /share/ to /data/ ..."
        cp /share/budget.db /data/budget.db
    else
        echo "No existing database found. A fresh one will be created."
    fi
elif [ -f /share/budget.db ] && [ /share/budget.db -nt /data/budget.db ]; then
    echo "Updating budget.db from /share/ (newer) ..."
    cp /share/budget.db /data/budget.db
fi

# Symlink so Python code finds it at /app/budget.db
ln -sf /data/budget.db /app/budget.db

# Read add-on options from /data/options.json
OPTIONS="/data/options.json"
if [ -f "$OPTIONS" ]; then
    TOKEN=$(jq -r '.telegram_bot_token // empty' "$OPTIONS")
    API_KEY=$(jq -r '.anthropic_api_key // empty' "$OPTIONS")
    GCAL_URL=$(jq -r '.gcal_ics_url // empty' "$OPTIONS")
    GCAL_KW=$(jq -r '.gcal_shift_keyword // empty' "$OPTIONS")
    EB_APP_ID=$(jq -r '.enablebanking_app_id // empty' "$OPTIONS")
    EB_SESSION=$(jq -r '.enablebanking_session_id // empty' "$OPTIONS")

    # Write env vars for the services to pick up
    {
        [ -n "$TOKEN" ]     && echo "TELEGRAM_BOT_TOKEN=$TOKEN"
        [ -n "$API_KEY" ]   && echo "ANTHROPIC_API_KEY=$API_KEY"
        [ -n "$EB_APP_ID" ] && echo "ENABLEBANKING_APP_ID=$EB_APP_ID"
        [ -n "$EB_SESSION" ] && echo "ENABLEBANKING_SESSION_ID=$EB_SESSION"
    } > /var/run/s6/container_environment/BUDGET_ENV 2>/dev/null || true

    # Also write to a file the services can source
    {
        [ -n "$TOKEN" ]     && echo "export TELEGRAM_BOT_TOKEN=\"$TOKEN\""
        [ -n "$API_KEY" ]   && echo "export ANTHROPIC_API_KEY=\"$API_KEY\""
        [ -n "$EB_APP_ID" ] && echo "export ENABLEBANKING_APP_ID=\"$EB_APP_ID\""
        [ -n "$EB_SESSION" ] && echo "export ENABLEBANKING_SESSION_ID=\"$EB_SESSION\""
    } > /app/.env

    # Copy Enable Banking PEM file from /share if available
    if [ -f /share/enablebanking.pem ]; then
        cp /share/enablebanking.pem /app/enablebanking.pem
        echo "Copied enablebanking.pem from /share/"
    fi

    # Write calendar settings to DB if provided
    if [ -n "$GCAL_URL" ] || [ -n "$GCAL_KW" ]; then
        python3 -c "
import sqlite3, os
conn = sqlite3.connect('/data/budget.db')
conn.execute('CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
gcal_url = os.environ.get('GCAL_URL', '$GCAL_URL')
gcal_kw = os.environ.get('GCAL_KW', '$GCAL_KW')
if gcal_url:
    conn.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', ('gcal_ics_url', gcal_url))
if gcal_kw:
    conn.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', ('gcal_shift_keyword', gcal_kw))
conn.commit()
conn.close()
" 2>/dev/null || true
    fi
else
    touch /app/.env
fi

echo "=== Budget Dashboard: Init complete ==="
