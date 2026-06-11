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
from .collectors.mmx_search import MmxSearchCollector
from .collectors.sina import SinaCollector
from .collectors.eastmoney import EastMoneyCollector
from .collectors.community_enhanced import CommunityEnhancedCollector
from .collectors.router import CollectorRouter
from .collectors.chain import ChainCollector
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
            'mmx_search': MmxSearchCollector(
                rate_limit_per_minute=self.config['collectors'].get('mmx_search', {}).get('rate_limit_per_minute', 10),
                default_queries=self.config['collectors'].get('mmx_search', {}).get('default_queries', [
                    "A股今日行情", "基金净值", "美股行情", "全球市场"
                ]),
            ),
            'sina': SinaCollector(
                rate_limit_per_minute=self.config['collectors'].get('sina', {}).get('rate_limit_per_minute', 60)
            ),
            'community_enhanced': CommunityEnhancedCollector(
                rate_limit_per_minute=self.config['collectors'].get('community_enhanced', {}).get('rate_limit_per_minute', 20)
            ),
            'eastmoney': EastMoneyCollector(
                rate_limit_per_minute=self.config['collectors'].get('eastmoney', {}).get('rate_limit_per_minute', 30)
            ),
        }

        # 初始化路由器（用于 price/nav/minute 等数据的智能降级）
        self.router = CollectorRouter(self.collectors)
        # 初始化责任链采集器（Phase 1: nav 类型走新链式架构）
        self.chain_collector = ChainCollector(self.db, self.collectors, self.router)

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
                elif data_type == 'stock_news':
                    # 个股新闻（东方财富搜索API），复用 news_data 表
                    news_item = {
                        'source': item.get('source', 'em_search'),
                        'title': item.get('title', ''),
                        'summary': item.get('summary', ''),
                        'link': item.get('url', ''),
                        'symbol': result.symbol,
                        'published': item.get('published', ''),
                    }
                    self.db.insert_news(news_item)
                    stored += 1
                elif data_type == 'xueqiu_hot':
                    # 雪球热帖，存入 community_data 表
                    # 注意：follow_count 可能是 NaN/None，int() 会抛 ValueError
                    follow_count_raw = item.get('follow_count', 0)
                    try:
                        follow_count_int = int(follow_count_raw) if follow_count_raw is not None and not (isinstance(follow_count_raw, float) and follow_count_raw != follow_count_raw) else 0
                    except (ValueError, TypeError):
                        follow_count_int = 0
                    community_item = {
                        'source': item.get('source', 'xueqiu'),
                        'symbol': item.get('symbol', ''),
                        'title': f"{item.get('name', '')} 关注:{follow_count_raw or 0} 价格:{item.get('price', 0)}",
                        'author': 'xueqiu',
                        'reply_count': follow_count_int,
                        'click_count': 0,
                        'published': now_tz().strftime('%Y-%m-%d'),
                        'link': f"https://xueqiu.com/S/{item.get('symbol', '')}",
                    }
                    self.db.insert_community(community_item)
                    stored += 1
                else:
                    logger.debug(f"未知数据类型: {data_type}, 跳过入库")
            except Exception as e:
                logger.error(f"入库失败 {data_type}: {e}")
        
        return stored

    def _load_config(self, path: str) -> Dict[str, Any]:
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    async def _consume_dead_letters(self, limit: int = 3, now=None):
        """
        消费死信队列：每分钟最多处理N条。
        能自动修复的自动修复，不能修的保留等待人工介入。
        """
        if now is None:
            now = now_tz()
        letters = self.db.get_unresolved_transforms(limit=limit)
        if not letters:
            return

        logger.info(f"死信队列：{len(letters)} 条待处理")
        for letter in letters:
            await self._process_dead_letter(letter)

    async def _process_dead_letter(self, letter: Dict[str, Any]):
        """
        处理单条死信。
        当前策略：
        - KeyError（缺字段）：记录 + 标记 resolved，等待人工分析
        - 其他错误：直接标记 resolved（可扩展为自动重试逻辑）
        未来可扩展为：自动识别接口变更并修补转换函数。
        """
        letter_id = letter['id']
        data_type = letter['data_type']
        error_msg = letter.get('transform_error', '')

        # 当前实现：只记录，不自动修复（等我来看）
        # 扩展点：未来在这里加 pattern matching：
        #   - "字段 xxx 不存在" → 尝试动态适配新字段名
        #   - "类型错误" → 尝试类型转换
        logger.info(
            f"  💀 死信 ID={letter_id}: {letter['source']}/{data_type}/{letter['symbol']} "
            f"| 错误: {error_msg[:80]}"
        )

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

    # ==================== 限频检测 & 冷却策略 ====================

    _RATE_LIMIT_KEYWORDS = (
        "rate limit", "频率", "每分钟最多", "每小时最多",
        "权限的具体详情", "access limit", "too many requests",
        "请求过于频繁", "配额", "quota"
    )

    def _is_rate_limit_error(self, error: str) -> bool:
        if not error:
            return False
        err_lower = error.lower()
        return any(kw in err_lower for kw in self._RATE_LIMIT_KEYWORDS)

    def _cooldown_minutes(self, error: str, attempt: int = 1) -> int:
        """从错误信息推断冷却时间（分钟）。"""
        import re
        e = error or ""
        # 尝试匹配 "每分钟最多访问该接口1次" → 1分钟
        m = re.search(r"每[时分]钟最多.{0,10}?(\d+)次", e)
        if m:
            return int(m.group(1)) + 1
        # 通用：当前重试次数越大，冷却越长（1/2/4分钟）
        return min(60, 2 ** max(0, attempt - 1))
    # ======================================================

    async def _tick(self):
        """单次检查"""
        now = now_tz()

        # ---- [NEW] 死信队列消费（每分钟最多处理3条）----
        await self._consume_dead_letters(limit=3, now=now)

        # ---- 优先：处理限频冷却到期的重试队列 ----
        due_fails = self.db.get_due_failed_collections(now)
        if due_fails:
            logger.info(f"限频重试队列：{len(due_fails)} 条待处理")
            for fail in due_fails:
                await self._process_failed_collection(fail, now)

        # ---- 普通订阅采集 ----
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

    async def _process_failed_collection(self, fail: Dict[str, Any], now: datetime):
        """处理冷却到期的限频重试记录。成功则删除记录；再次失败则更新冷却时间。"""
        fail_id = fail['id']
        sub_id = fail['subscription_id']
        source = fail['source']
        symbol = fail['symbol']
        data_type = fail['data_type']
        config = json.loads(fail['config']) if fail.get('config') else {}
        fail_error = fail.get('error_message', '')

        logger.info(f"  重试限频记录 ID={fail_id}: {source}/{data_type}/{symbol}")

        # 重建订阅上下文（仅用于采集，不更新 last_run）
        sub = {
            'id': sub_id,
            'name': f"重试#{fail_id}",
            'type': source,
            'symbol': symbol,
            'data_types': [data_type],
            'config': config,
        }
        # 注入 backup_sources
        if fail.get('backup_sources'):
            raw_backup = fail['backup_sources']
            backup_sources = [s.strip() for s in raw_backup.split(',')] if isinstance(raw_backup, str) else list(raw_backup)
            config['backup_sources'] = backup_sources

        start_time = now_tz()
        try:
            collector = self.collectors.get(source)
            if not collector:
                logger.warning(f"  采集器不存在: {source}，删除重试记录")
                self.db.remove_failed_collection(fail_id)
                return

            result = await collector.collect(symbol=symbol or "*", data_type=data_type, **config)

            if result.success and result.data:
                items_stored = await self._store_result(result)
                self.db.remove_failed_collection(fail_id)
                logger.info(f"  重试成功！已入库 {items_stored} 条，删除重试记录")
            else:
                if self._is_rate_limit_error(result.error):
                    # 再次限频：延长冷却时间
                    cooldown = self._cooldown_minutes(result.error, attempt=2)
                    retry_after = now + timedelta(minutes=cooldown)
                    self.db.update_failed_collection_retry_after(fail_id, retry_after)
                    logger.warning(f"  重试再次限频，更新冷却至 {cooldown} 分钟后")
                else:
                    # 非限频错误：删除重试记录，避免无限重试
                    self.db.remove_failed_collection(fail_id)
                    logger.warning(f"  重试非限频失败，删除重试记录: {result.error}")
        except Exception as e:
            logger.exception(f"  重试异常 ID={fail_id}: {e}")
            self.db.remove_failed_collection(fail_id)

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

        # 对于 price/nav/minute/estimate_nav 类型，使用路由器进行多级降级
        # daily 走 router：akshare 不支持 → 降级到 tushare（tushare 有 daily）
        ROUTABLE_TYPES = {"price", "nav", "minute", "estimate_nav", "daily"}
        # nav 类型走 ChainCollector（Phase 1: 责任链+死信队列）
        # 迁移范围：除 daily(已废弃)、news_aggregator(独立采集)、mmx_search(特殊) 外全部迁移
        CHAIN_TYPES = {
            "nav", "estimate_nav", "price", "minute",
            "fundamental", "news", "index", "community", "announcement",
            "hsgt", "stock_basic",
            "stock_news",   # 个股新闻（东方财富搜索API / 雪球）—— 走 community_enhanced
            "xueqiu_hot",   # 雪球热帖 —— 走 community_enhanced
        }

        # 对每个data_type执行采集
        for data_type in data_types:
            start_time = now_tz()
            use_router = data_type in ROUTABLE_TYPES and source_type == 'akshare'
            use_chain = data_type in CHAIN_TYPES and source_type == 'akshare'

            try:
                if use_chain:
                    # Phase 1: nav 类型走责任链（自动容灾 + 死信队列）
                    result = await self.chain_collector.collect(
                        data_type=data_type,
                        symbol=symbol or "*",
                        backup_sources=backup_sources,
                        config=config,
                    )
                elif use_router:
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
                    # ---- 限频检测：写入重试队列 ----
                    if self._is_rate_limit_error(result.error):
                        cooldown = self._cooldown_minutes(result.error)
                        retry_after = datetime.now(TZ).astimezone(TZ).astimezone().astimezone() + timedelta(minutes=cooldown)
                        retry_after = datetime.now(TZ) + timedelta(minutes=cooldown)
                        self.db.insert_failed_collection(
                            subscription_id=sub_id,
                            source=result.source,
                            symbol=result.symbol or symbol,
                            data_type=data_type,
                            config=config,
                            error_message=result.error,
                            retry_after=retry_after
                        )
                        logger.info(f"  限频记录已入队，冷却 {cooldown} 分钟后重试（订阅={name}）")

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
