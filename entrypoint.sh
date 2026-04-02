#!/bin/sh
exec /app/.venv/bin/streamlit run streamlit_app.py \
  --server.port="${PORT:-8501}" \
  --server.address=0.0.0.0 \
  --server.headless=true
