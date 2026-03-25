ARG BUILD_FROM=ghcr.io/home-assistant/aarch64-base:3.21
FROM ${BUILD_FROM}

# Use http for apk repos (workaround for stale CA certs in base image)
RUN sed -i 's|https://|http://|g' /etc/apk/repositories && \
    apk add --no-cache \
        ca-certificates python3 py3-pip gcc g++ musl-dev python3-dev jq bash && \
    sed -i 's|http://|https://|g' /etc/apk/repositories

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --break-system-packages --prefer-binary -r requirements.txt

# Copy application code
COPY dashboard.py telegram_bot.py ai_insights.py db.py deals.py bank_sync.py report_generator.py style.css ./

# --- S6 overlay init script (runs once at startup) ---
COPY cont-init.sh /etc/cont-init.d/00-budget-init.sh
RUN chmod +x /etc/cont-init.d/00-budget-init.sh && \
    sed -i 's/\r$//' /etc/cont-init.d/00-budget-init.sh

# --- S6 service: Streamlit dashboard ---
RUN mkdir -p /etc/services.d/streamlit
COPY service-streamlit.sh /etc/services.d/streamlit/run
RUN chmod +x /etc/services.d/streamlit/run && \
    sed -i 's/\r$//' /etc/services.d/streamlit/run

# --- S6 service: Telegram bot ---
RUN mkdir -p /etc/services.d/telegram-bot
COPY service-bot.sh /etc/services.d/telegram-bot/run
RUN chmod +x /etc/services.d/telegram-bot/run && \
    sed -i 's/\r$//' /etc/services.d/telegram-bot/run

# Fix line endings on Python files
RUN sed -i 's/\r$//' /app/*.py
