#!/usr/bin/with-contenv bash
# S6 service: Streamlit dashboard
source /app/.env 2>/dev/null || true
cd /app
exec streamlit run dashboard.py \
    --server.port=8501 \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --browser.gatherUsageStats=false \
    --server.fileWatcherType=none
