#!/usr/bin/env python3
"""
调度器：监控订阅并触发采集（支持多级降级）
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
from pathlib import Path
import yaml
import json

from .database import Database, now_tz
from .collectors.akshare import AKShareFetcher
from .collectors.rss import RSSCollector
from .collectors.massive import MassiveCollector
from .collectors.tickflow import TickFlowCollector
from .collectors.tushare import TushareCollector
from .collectors.news_aggregator import NewsAggregatorCollector
from .collectors.router import CollectorRouter
from .collectors.base import CollectResult

logger = logging.getLogger(__name__)

class Scheduler:
    """调度器（支持多级降级）"""

    def __init__(self, db: Database, config_path: str = None):
        self.db = db
        if config_path is None:
            config_path = Path(__file__).parent / "config" / "config.yaml"
        self.config = self._load_config(config_path)
        self.running = False
        self._task = None

        # 初始化各采集器
        self.collectors = {
            'akshare': AKShareFetcher(rate_limit_per_minute=self.config['collectors']['akshare']['rate_limit_per_minute']),
            'rss': RSSCollector(
                config_path=Path(__file__).parent / "config" / "rss_config.json",
                rate_limit_per_second=self.config['collectors']['rss']['rate_limit_per_second']
            ),
            'massive': MassiveCollector(
                api_key=self.config['collectors']['massive']['api_key'],
                rate_limit_per_minute=self.config['collectors']['massive']['rate_limit_per_minute']
            ),
            'tickflow': TickFlowCollector(
                rate_limit_per_minute=self.config['collectors'].get('tickflow', {}).get('rate_limit_per_minute', 30)
            ),
            'tushare': TushareCollector(
                api_token=self.config['collectors']['tushare'].get('token'),
                rate_limit_per_minute=self.config['collectors']['tushare']['rate_limit_per_minute']
            ),
            'news_aggregator': NewsAggregatorCollector(
                rate_limit_per_minute=self.config['collectors'].get('news_aggregator', {}).get('rate_limit_per_minute', 10)
            ),
        }

        # 初始化路由器（用于 price/nav/minute 等数据的智能降级）
        self.router = CollectorRouter(self.collectors)

    async def _store_result(self, result: 'CollectResult') -> int:
        """将采集结果存入数据库"""
        if not result.success or not result.data:
            return 0
        
        data_type = result.data_type
        # 单条 or 列表
        items = result.data if isinstance(result.data, list) else [result.data]
        stored = 0
        
        for item in items:
            try:
                if data_type == 'price':
                    self.db.insert_price(item)
                    stored += 1
                elif data_type == 'nav':
                    self.db.insert_nav(item)
                    stored += 1
                elif data_type == 'news':
                    self.db.insert_news(item)
                    stored += 1
                elif data_type == 'fundamental':
                    self.db.save_fundamental(item)
                    stored += 1
                elif data_type == 'index':
                    self.db.insert_index(item)
                    stored += 1
                elif data_type == 'announcement':
                    self.db.insert_announcement(item)
                    stored += 1
                elif data_type == 'community':
                    self.db.insert_community(item)
                    stored += 1
                elif data_type == 'minute':
                    # minute类型返回的是List[List[Dict]]（每只股票一个列表），或直接是List[Dict]
                    bars = item if isinstance(item, list) else [item]
                    if bars and isinstance(bars[0], list):
                        bars = bars[0]  # 解包两层
                    stored += self.db.insert_minute_bars(bars)
                elif data_type == 'estimate_nav':
                    self.db.insert_estimate_nav(item)
                    stored += 1
                elif data_type == 'daily':
                    self.db.insert_daily(item)
                    stored += 1
                elif data_type == 'hsgt':
                    self.db.insert_hsgt(item)
                    stored += 1
                elif data_type == 'stock_basic':
                    self.db.insert_stock_basic(item)
                    stored += 1
                elif data_type == 'news_aggregator':
                    # 聚合新闻是单一 dict，直接存 whole_result
                    self.db.insert_news_aggregator(result.data)
                    stored += 1
                else:
                    logger.debug(f"未知数据类型: {data_type}, 跳过入库")
            except Exception as e:
                logger.error(f"入库失败 {data_type}: {e}")
        
        return stored

    def _load_config(self, path: str) -> Dict[str, Any]:
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def _is_trading_hours(self, dt: datetime) -> bool:
        """判断是否在A股交易时段（9:30-11:30, 13:00-15:00）"""
        hour = dt.hour
        minute = dt.minute
        return (
            (hour == 9 and minute >= 30) or
            (10 <= hour < 11) or
            (hour == 11 and minute < 30) or
            (13 <= hour < 15)
        )

    async def start(self):
        """启动后台调度循环"""
        self.running = True
        logger.info("调度器启动，检查间隔: %d秒", self.config['scheduler']['check_interval_seconds'])
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self):
        """停止调度器"""
        self.running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("调度器已停止")

    async def _run_loop(self):
        """主循环"""
        check_interval = self.config['scheduler']['check_interval_seconds']

        while self.running:
            try:
                await self._tick()
            except Exception as e:
                logger.exception(f"调度器tick异常: {e}")

            # 等待下一次检查
            await asyncio.sleep(check_interval)

    async def _tick(self):
        """单次检查"""
        now = now_tz()
        due_subs = self.db.get_due_subscriptions(now)

        # 过滤：如果订阅设置了 trading_hours_only，检查是否在交易时段
        filtered_subs = []
        for sub in due_subs:
            if sub.get('trading_hours_only'):
                if not self._is_trading_hours(now):
                    logger.debug(f"订阅 {sub['name']} 仅交易时段采集，跳过（非交易时段）")
                    continue
            filtered_subs.append(sub)

        logger.debug(f"需要处理的订阅数: {len(filtered_subs)}（筛选后）")

        for sub in filtered_subs:
            await self._process_subscription(sub, now)

    async def _process_subscription(self, sub: Dict[str, Any], now: datetime):
        """处理单个订阅（支持多级降级）"""
        sub_id = sub['id']
        name = sub['name']
        source_type = sub['type']
        symbol = sub.get('symbol')
        data_types = json.loads(sub['data_types']) if isinstance(sub['data_types'], str) else sub['data_types']
        config = json.loads(sub['config']) if isinstance(sub['config'], str) else sub['config']

        # 注入backup_sources（从订阅字段到config，供路由器使用）
        backup_sources = None
        if sub.get('backup_sources'):
            raw_backup = sub['backup_sources']
            if isinstance(raw_backup, str):
                backup_sources = [s.strip() for s in raw_backup.split(',') if s.strip()]
            else:
                backup_sources = list(raw_backup) if raw_backup else None
            config['backup_sources'] = backup_sources

        logger.info(f"处理订阅: {name} (ID={sub_id}, source={source_type}, symbol={symbol})")

        # 对于 price/nav/minute/estimate_nav 类型，使用路由器进行多级降级
        # daily 走 router：akshare 不支持 → 降级到 tushare（tushare 有 daily）
        ROUTABLE_TYPES = {"price", "nav", "minute", "estimate_nav", "daily"}

        # 对每个data_type执行采集
        for data_type in data_types:
            start_time = now_tz()
            use_router = data_type in ROUTABLE_TYPES and source_type == 'akshare'

            try:
                if use_router:
                    # 使用路由器智能路由
                    # 注意：config['backup_sources'] 已注入，直接用 **config 即可
                    result = await self.router.collect(
                        symbol=symbol or "*",
                        data_type=data_type,
                        max_retries=3,
                        **config
                    )
                else:
                    # 使用原始采集器
                    if source_type not in self.collectors:
                        raise ValueError(f"未知的采集器类型: {source_type}")
                    collector = self.collectors[source_type]
                    result = await collector.collect_with_retry(
                        symbol=symbol or "*",
                        data_type=data_type,
                        max_retries=3,
                        **config
                    )

                if result.success and result.data:
                    items_stored = await self._store_result(result)
                else:
                    items_stored = 0

                # 记录日志
                self.db.log_collection(
                    subscription_id=sub_id,
                    source=result.source,
                    symbol=result.symbol,
                    data_type=result.data_type,
                    status="success" if result.success else "failed",
                    items_fetched=len(result.data) if result.data and isinstance(result.data, list) else (1 if result.data else 0),
                    items_stored=items_stored,
                    error_message=result.error,
                    started_at=start_time,
                    finished_at=now_tz()
                )

                if result.success:
                    logger.info(f"  订阅[{name}]采集成功: {result.data_type}, {items_stored}条")
                else:
                    logger.warning(f"  订阅[{name}]采集失败: {result.error}")

            except Exception as e:
                logger.exception(f"采集异常 {name}/{data_type}: {e}")
                self.db.log_collection(
                    subscription_id=sub_id,
                    source=source_type,
                    symbol=symbol,
                    data_type=data_type,
                    status="failed",
                    error_message=str(e),
                    started_at=start_time,
                    finished_at=now_tz()
                )

        # 更新订阅最后运行时间（无论单个data_type是否都成功）
        self.db.update_subscription_last_run(sub_id, now)

    def get_status(self) -> Dict[str, Any]:
        """获取调度器状态"""
        return {
            "running": self.running,
            "check_interval": self.config['scheduler']['check_interval_seconds'],
            "collectors": list(self.collectors.keys()),
            "router_status": self.router.get_status(),
            "active_subscriptions": self.db.get_stats().get('active_subscriptions', 0)
        }

# 便捷函数
async def run_scheduler_once():
    """单次运行调度器（用于调试）"""
    db = Database()
    sched = Scheduler(db)
    await sched._tick()
    db.close()

if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_scheduler_once())
