#!/bin/bash
# 启动 Market Data Service

VENV="/vol1/@apphome/trim.openclaw/data/workspace/akshare_venv"
PROJECT_ROOT="/vol1/@apphome/trim.openclaw/data/workspace/market_service"

cd "$PROJECT_ROOT" || exit 1

# 设置Python路径
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

# 启动服务
exec "$VENV/bin/uvicorn" \
    --host 127.0.0.1 \
    --port 8080 \
    --app-dir "$PROJECT_ROOT" \
    "api:app"
