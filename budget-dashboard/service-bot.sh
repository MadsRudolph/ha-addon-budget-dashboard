#!/usr/bin/with-contenv bash
# S6 service: Telegram bot
source /app/.env 2>/dev/null || true
cd /app

# Trim whitespace from token
TELEGRAM_BOT_TOKEN="$(echo -n "$TELEGRAM_BOT_TOKEN" | xargs)"

if [ -z "$TELEGRAM_BOT_TOKEN" ]; then
    echo "No Telegram bot token configured — bot disabled."
    echo "Set it in the add-on Configuration tab."
    # Sleep forever so S6 doesn't restart endlessly
    exec sleep infinity
fi

export TELEGRAM_BOT_TOKEN

# Run with backoff to avoid crash loops
FAILURES=0
MAX_BACKOFF=300  # 5 minutes max

while true; do
    echo "Starting Telegram bot..."
    python3 telegram_bot.py
    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        # Clean exit — restart normally
        FAILURES=0
        sleep 5
    else
        FAILURES=$((FAILURES + 1))
        BACKOFF=$((FAILURES * 10))
        if [ $BACKOFF -gt $MAX_BACKOFF ]; then
            BACKOFF=$MAX_BACKOFF
        fi
        echo "Telegram bot exited with code $EXIT_CODE (failure #${FAILURES}). Retrying in ${BACKOFF}s..."
        sleep $BACKOFF
    fi
done
