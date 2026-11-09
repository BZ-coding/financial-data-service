"""第三方采集源凭据的读取层。

凭据真源：`~/.config/financial-data-service-credentials/collectors-credentials.json`
（0600，不入 git），结构 {"massive.api_key": ..., "tushare.token": ...}。
config.yaml 里只保留 enabled / rate_limit 等非敏感项；api_key/token 字段已从 git 移除。
也可用环境变量 FPS_COLLECTOR_CREDENTIALS 指向其它路径（本地调试）。
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

DEFAULT_PATH = Path.home() / ".config" / "financial-data-service-credentials" / "collectors-credentials.json"


def _load() -> dict:
    path = Path(os.environ.get("FPS_COLLECTOR_CREDENTIALS") or DEFAULT_PATH)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def get(key: str) -> Optional[str]:
    """按 'collector.field' 取凭据，如 'massive.api_key'、'tushare.token'。"""
    return _load().get(key)
