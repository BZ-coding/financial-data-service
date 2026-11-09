#!/usr/bin/env python3
"""
测试调度器（单次tick）
"""

import asyncio
import logging
from pathlib import Path

# 添加项目路径
#
# ⚠️ 2026-10-30 修正：**差了一层**。
#   原来插的是 `Path(__file__).parent.parent` = `market_service/`，
#   但下面 `from market_service.database import ...` 需要的是**仓库根**
#   （`market_service` 包的父目录）→ 直接跑本脚本必然
#   ModuleNotFoundError: No module named 'market_service'。
#   两个路径都插：仓库根（给 `market_service.*` 包导入用）+
#   market_service/（给 database.py 的平铺回退分支用）。
import sys
_project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_project_root))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from market_service.database import Database
from market_service.scheduler import Scheduler

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def main():
    db = Database()
    sched = Scheduler(db)

    print("=" * 60)
    print("调度器单次tick测试")
    print("=" * 60)

    # 执行一次tick
    await sched._tick()

    print("\n采集日志（最近5条）:")
    cur = db.conn.execute("SELECT * FROM collection_log ORDER BY id DESC LIMIT 5")
    for row in cur.fetchall():
        print(f"  [{row['status']}] {row['source']} {row['symbol']} {row['data_type']} - {row['items_stored']}条")

    db.close()

if __name__ == "__main__":
    asyncio.run(main())
