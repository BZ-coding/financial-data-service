#!/usr/bin/env python3
"""
初始化默认订阅
"""

import sys
import json
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from database import Database

def init_subscriptions(db: Database):
    """创建初始订阅配置"""

    # 1. 008114基金数据（净值+新闻）
    sub_id = db.add_subscription(
        name="008114天弘红利低波动",
        type_="akshare",
        symbol="008114",
        config={"module": "fund_open_fund_info_em"},
        data_types=["nav", "news"],
        frequency_min=60
    )
    print(f"✅ 创建订阅 #{sub_id}: 008114基金数据")

    # 2. RSS财经新闻（所有源）
    # 从config/rss_config.json读取source列表，通配符"*"表示所有源
    sub_id = db.add_subscription(
        name="RSS财经新闻",
        type_="rss",
        symbol="*",  # 通配符：所有源
        config={},
        data_types=["news"],
        frequency_min=30
    )
    print(f"✅ 创建订阅 #{sub_id}: RSS财经新闻")

    # 3. 600519贵州茅台实时行情
    sub_id = db.add_subscription(
        name="600519贵州茅台行情",
        type_="akshare",
        symbol="600519",
        config={},
        data_types=["price"],
        frequency_min=5
    )
    print(f"✅ 创建订阅 #{sub_id}: 600519实时行情")

    print("\n📋 所有订阅列表:")
    subs = db.get_all_subscriptions()
    for sub in subs:
        print(f"  ID={sub['id']} | {sub['name']} | type={sub['type']} | symbol={sub.get('symbol','-')} | enabled={sub['enabled']}")

if __name__ == "__main__":
    db = Database()
    init_subscriptions(db)
    db.close()
