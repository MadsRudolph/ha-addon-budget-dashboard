#!/usr/bin/with-contenv bash
# S6 service: Telegram bot
source /app/.env 2>/dev/null || true
cd /app

if [ -z "$TELEGRAM_BOT_TOKEN" ]; then
    echo "No Telegram bot token configured — bot disabled."
    echo "Set it in the add-on Configuration tab."
    # Sleep forever so S6 doesn't restart endlessly
    exec sleep infinity
fi

exec python3 telegram_bot.py
