ARG BUILD_FROM
FROM ${BUILD_FROM}

# Detect the base OS and install dependencies accordingly
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Install system dependencies
# Debian-based images (amd64) use apt-get; Alpine images (aarch64/armv7) use apk
RUN if command -v apt-get >/dev/null 2>&1; then \
        apt-get update && \
        apt-get install -y --no-install-recommends \
            python3 python3-pip python3-venv python3-dev \
            gcc g++ jq bash ca-certificates && \
        rm -rf /var/lib/apt/lists/*; \
    else \
        sed -i 's|https://|http://|g' /etc/apk/repositories && \
        apk add --no-cache \
            ca-certificates python3 py3-pip gcc g++ musl-dev python3-dev jq bash && \
        sed -i 's|http://|https://|g' /etc/apk/repositories; \
    fi

WORKDIR /app

COPY requirements.txt .

# Install Python packages
# On Alpine: remove system numpy/pandas to avoid X86_V2 issues, install from pip
# On Debian: pip install works natively with glibc wheels
RUN if command -v apk >/dev/null 2>&1; then \
        apk del py3-numpy py3-pandas 2>/dev/null; \
        rm -rf /usr/lib/python3.*/site-packages/numpy* \
               /usr/lib/python3.*/site-packages/pandas*; \
    fi && \
    pip install --no-cache-dir --break-system-packages \
        numpy pandas && \
    pip install --no-cache-dir --break-system-packages \
        -r requirements.txt

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
