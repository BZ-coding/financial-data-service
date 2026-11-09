#!/usr/bin/env python3
"""
采集器路由器：智能多级降级系统
根据标的类型、数据类型自动选择最优采集器，失败时降级到备选源
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta

from .base import BaseCollector, CollectResult

TZ = timezone(timedelta(hours=8))
logger = logging.getLogger(__name__)


@dataclass
class CollectorInfo:
    """采集器信息"""
    name: str                          # 采集器标识：akshare, massive, tickflow
    collector: BaseCollector           # 采集器实例
    priority: int                      # 优先级（数字越小越高）
    supported_markets: List[str]       # 支持的市场，如 ["SH", "SZ", "US", "HK"]
    supported_data_types: List[str]    # 支持的数据类型，如 ["price", "nav", "minute"]
    is_premium: bool = False           # 是否付费数据源


class CollectorRouter:
    """
    多级降级采集路由器

    设计原则：
    1. 优先使用高质量数据源（付费 > 免费）
    2. 按市场匹配最优采集器
    3. 主源失败时自动降级到备选源
    4. 跨源去重（同一标的同一时间只存一条）
    """

    # 市场 -> 采集器优先级配置
    # 格式：(采集器名, 是否付费, 支持的数据类型)
    MARKET_PRIORITY = {
        "SH": [("akshare", False, ["price", "nav", "minute", "fundamental", "index", "news", "announcement", "community", "daily", "estimate_nav"]),
               ("tushare", False, ["daily", "hsgt", "stock_basic"]),
               ("tickflow", False, ["price", "daily"]),
               ("massive", False, ["price"])],  # massive的SH股票数据实际来自美股指数映射
        "SZ": [("akshare", False, ["price", "nav", "minute", "fundamental", "index", "news", "announcement", "community", "daily", "estimate_nav"]),
               ("tushare", False, ["daily", "hsgt", "stock_basic"]),
               ("tickflow", False, ["price", "daily"]),
               ("massive", False, ["price"])],
        "BJ": [("akshare", False, ["price", "nav", "fundamental", "news", "announcement", "community"]),
               ("tickflow", False, ["price"])],
        "US": [("massive", True, ["price", "minute"]),
               ("tickflow", False, ["price", "daily"]),
               ("akshare", False, ["price"])],
        "HK": [("tickflow", False, ["price"]),
               ("akshare", False, ["price"])],
        # 期货
        "SHF": [("akshare", False, ["price"]),
                ("tickflow", False, ["price"])],
        "DCE": [("akshare", False, ["price"]),
                ("tickflow", False, ["price"])],
        "ZCE": [("akshare", False, ["price"]),
                ("tickflow", False, ["price"])],
        "CFX": [("akshare", False, ["price"]),
                ("tickflow", False, ["price"])],
        "INE": [("akshare", False, ["price"]),
                ("tickflow", False, ["price"])],
        "GFE": [("akshare", False, ["price"]),
                ("tickflow", False, ["price"])],
    }

    # 数据类型 -> 采集器能力映射
    DATA_TYPE_MARKET_FILTER = {
        "price": ["SH", "SZ", "BJ", "US", "HK", "SHF", "DCE", "ZCE", "CFX", "INE", "GFE"],
        "nav": ["SH", "SZ", "BJ"],
        "fundamental": ["SH", "SZ", "BJ"],
        "index": ["SH", "SZ"],
        "news": ["SH", "SZ", "BJ"],
        "announcement": ["SH", "SZ", "BJ"],
        "community": ["SH", "SZ", "BJ"],
        "minute": ["SH", "SZ", "US"],
        "daily": ["SH", "SZ"],
        "estimate_nav": ["SH", "SZ"],
        "hsgt": ["SH", "SZ"],
        "stock_basic": ["SH", "SZ", "BJ"],
    }

    def __init__(self, collectors: Dict[str, BaseCollector]):
        """
        初始化路由器

        Args:
            collectors: {采集器名: 采集器实例}
                       如 {"akshare": AKShareFetcher(), "massive": MassiveCollector(), "tickflow": TickFlowCollector()}
        """
        self.collectors = collectors
        self._build_priority_map()

    def _build_priority_map(self):
        """构建优先级映射表"""
        self._priority_map: Dict[Tuple[str, str], List[Tuple[str, bool]]] = {}
        # _priority_map[("US", "price")] = [("massive", True), ("tickflow", False), ("akshare", False)]
        for market, sources in self.MARKET_PRIORITY.items():
            for data_type in self.DATA_TYPE_MARKET_FILTER.keys():
                key = (market, data_type)
                self._priority_map[key] = [
                    (name, is_premium)
                    for name, is_premium, supported_types in sources
                    if data_type in supported_types and name in self.collectors
                ]
                # 按优先级排序（付费优先）
                self._priority_map[key].sort(key=lambda x: (not x[1], 0))  # 付费的排前面

    def _detect_market(self, symbol: str) -> Optional[str]:
        """检测标的所属市场"""
        if "." in symbol:
            _, suffix = symbol.rsplit(".", 1)
            return suffix
        # A股推断
        if symbol.startswith(("6", "9")) and len(symbol) == 6:
            return "SH"
        elif symbol.startswith(("0", "1", "2", "3")) and len(symbol) == 6:
            return "SZ"
        elif symbol.startswith(("4", "8")) and len(symbol) == 6:
            return "BJ"
        return None

    def _get_collector_chain(self, market: str, data_type: str) -> List[str]:
        """获取采集器链路（按优先级排序）"""
        key = (market, data_type)
        chain = self._priority_map.get(key, [])
        return [name for name, _ in chain]

    async def collect(
        self,
        symbol: str,
        data_type: str,
        backup_sources: List[str] = None,
        max_retries: int = 3,
        **kwargs
    ) -> CollectResult:
        """
        智能采集（带多级降级）

        Args:
            symbol: 标的代码（如 "600519.SH", "TSLA.US"）
            data_type: 数据类型（price, nav, minute 等）
            backup_sources: 手动指定的备选源列表（会插入到自动链路之后）
            max_retries: 每个采集器的最大重试次数
            **kwargs: 传递给采集器的额外参数

        Returns:
            CollectResult: 成功时包含数据，失败时包含所有尝试的错误
        """
        start_time = datetime.now(TZ)
        market = self._detect_market(symbol)

        if not market:
            return CollectResult(
                success=False,
                data=None,
                source="router",
                symbol=symbol,
                data_type=data_type,
                error=f"无法识别市场: {symbol}",
                duration_seconds=0
            )

        # 获取采集链路
        chain = self._get_collector_chain(market, data_type)

        # 追加手动指定的备选源
        if backup_sources:
            for src in backup_sources:
                if src not in chain and src in self.collectors:
                    chain.append(src)

        if not chain:
            return CollectResult(
                success=False,
                data=None,
                source="router",
                symbol=symbol,
                data_type=data_type,
                error=f"没有可用的采集器: market={market}, data_type={data_type}",
                duration_seconds=0
            )

        # 依次尝试每个采集器
        errors = []
        for collector_name in chain:
            collector = self.collectors[collector_name]

            try:
                result = await collector.collect_with_retry(
                    symbol=symbol,
                    data_type=data_type,
                    max_retries=max_retries,
                    **kwargs
                )

                if result.success and result.data:
                    result.source = f"{collector_name}({market})"  # 标记实际来源
                    duration = (datetime.now(TZ) - start_time).total_seconds()
                    result.duration_seconds = duration
                    logger.info(f"✅ [{collector_name}] {symbol}/{data_type} 成功 (耗时{duration:.1f}s)")
                    return result
                else:
                    errors.append(f"{collector_name}: {result.error}")

            except Exception as e:
                errors.append(f"{collector_name}: {str(e)}")
                logger.warning(f"❌ [{collector_name}] {symbol}/{data_type} 异常: {e}")

        # 所有采集器都失败
        duration = (datetime.now(TZ) - start_time).total_seconds()
        logger.error(f"❌ 全链路失败 {symbol}/{data_type}: {'; '.join(errors)}")
        return CollectResult(
            success=False,
            data=None,
            source="router",
            symbol=symbol,
            data_type=data_type,
            error=f"全链路失败: {'; '.join(errors)}",
            duration_seconds=duration
        )

    async def collect_batch(
        self,
        symbols: List[str],
        data_type: str,
        **kwargs
    ) -> Dict[str, CollectResult]:
        """批量采集，返回 {symbol: result}"""
        tasks = [
            self.collect(symbol, data_type, **kwargs)
            for symbol in symbols
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        output = {}
        for symbol, result in zip(symbols, results):
            if isinstance(result, Exception):
                output[symbol] = CollectResult(
                    success=False,
                    data=None,
                    source="router",
                    symbol=symbol,
                    data_type=data_type,
                    error=str(result)
                )
            else:
                output[symbol] = result
        return output

    def get_collector_chain_str(self, symbol: str, data_type: str) -> str:
        """获取采集链路描述（用于调试/展示）"""
        market = self._detect_market(symbol)
        if not market:
            return "无法识别市场"
        chain = self._get_collector_chain(market, data_type)
        return " -> ".join(chain) if chain else "无可用采集器"

    def get_status(self) -> Dict[str, Any]:
        """获取路由器状态"""
        return {
            "available_collectors": list(self.collectors.keys()),
            "market_priority": {
                market: tuple(name for name, _ in sources)
                for market, sources in self._priority_map.items()
                if sources
            }
        }