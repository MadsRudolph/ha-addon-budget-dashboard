ARG BUILD_FROM=ghcr.io/home-assistant/aarch64-base:latest
FROM ${BUILD_FROM}

# Install build deps (Debian-based python image)
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc g++ jq bash && \
    rm -rf /var/lib/apt/lists/*

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
