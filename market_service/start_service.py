#!/usr/bin/env python3
"""
启动 Market Data Service
"""

import sys
from pathlib import Path

# 确保 marketplace_service 可导入
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "market_service.api:app",
        host="127.0.0.1",
        port=8080,
        reload=False,
        log_level="info"
    )
