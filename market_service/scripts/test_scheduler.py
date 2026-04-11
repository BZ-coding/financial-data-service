#!/usr/bin/env python3
"""
测试调度器（单次tick）
"""

import asyncio
import logging
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent.parent
import sys
sys.path.insert(0, str(project_root))

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
