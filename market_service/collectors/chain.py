#!/usr/bin/env python3
"""
责任链采集器（ChainCollector）
- 订阅只声明 data_type，信源由链动态排序决定
- 链式遍历：第一个成功返回的信源停止传播
- 采集失败自动切下一个信源；转换失败进入死信队列
- 每次采集后更新 source_metrics，用于动态排名
"""

import asyncio
import logging
import traceback
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field

from .base import BaseCollector, CollectResult

TZ = timezone(timedelta(hours=8))
logger = logging.getLogger(__name__)


def now_tz() -> datetime:
    return datetime.now(TZ)


@dataclass
class TransformResult:
    """转换结果"""
    ok: bool
    data: Any = None
    error: str = ""
    stack: str = ""


class SoftTransformError(Exception):
    """软转换失败：数据无效（如price=None），不记死信，切下一个源"""
    pass


@dataclass
class ChainConfig:
    """链配置（对应 TaskQueue 中的单条任务）"""
    data_type: str
    symbol: str
    required_fields: List[str] = field(default_factory=list)
    max_attempts: int = 3
    timeout_per_source: int = 15


class ChainCollector:
    """
    责任链采集器

    使用方式（示例）：
        chain = ChainCollector(db, collectors, router)
        result = await chain.collect(data_type='nav', symbol='270023')
    """

    # 每个 data_type 默认的链顺序（可被 source_metrics 动态调整）
    DEFAULT_CHAINS: Dict[str, List[str]] = {
        "nav":          ["akshare"],         # tushare 不支持 nav，只用 akshare
        "estimate_nav": ["akshare"],          # 只有 fundgz 支持
        "price":        ["akshare", "sina", "tickflow", "massive"],
        "minute":       ["akshare", "tickflow"],
        "daily":        ["tushare", "akshare"],
        "fundamental":  ["akshare"],
        "news":         ["akshare"],
        "index":        ["akshare"],
        "community":    ["akshare"],
        "announcement": ["akshare"],
        "hsgt":         ["tushare"],
        "stock_basic":  ["tushare", "akshare"],
        "stock_news":   ["community_enhanced"],  # 个股新闻（东方财富搜索API / 雪球）
        "xueqiu_hot":   ["community_enhanced"],  # 雪球热帖
    }

    # 连续失败 N 次触发降级
    DEGRADE_THRESHOLD = 5
    # 成功率低于此值触发降级（需同时满足 total > 10）
    DEGRADE_SUCCESS_RATE = 0.80

    def __init__(
        self,
        db: 'Database',
        collectors: Dict[str, BaseCollector],
        router: 'CollectorRouter' = None,
    ):
        self.db = db
        self.collectors = collectors
        self.router = router

    # ==================== 公开 API ====================

    async def collect(
        self,
        data_type: str,
        symbol: str,
        backup_sources: List[str] = None,
        config: Dict[str, Any] = None,
    ) -> CollectResult:
        """
        入口：采集单条数据，走责任链。

        Args:
            data_type: 数据类型（nav, price, estimate_nav ...）
            symbol: 标的代码
            backup_sources: 手动指定的备选信源（追加到链尾）
            config: 透传给采集器的额外参数

        Returns:
            CollectResult: 第一个成功返回的信源数据，或全链路失败
        """
        cfg = config or {}
        chain = self._build_chain(data_type, symbol, backup_sources)
        start_time = now_tz()

        last_error = ""
        for attempt_idx, source in enumerate(chain):
            collector = self.collectors.get(source)
            if not collector:
                last_error = f"信源不存在: {source}"
                continue

            # ---- Phase 1: 采集 ----
            raw_result = await self._try_collect(
                collector, source, data_type, symbol, cfg
            )

            if not raw_result.success:
                last_error = raw_result.error or "采集返回空"
                self._record_metric(source, data_type, success=False, duration=raw_result.duration_seconds)
                logger.warning(f"  [{source}] 采集失败: {last_error}，尝试链下一个")
                continue

            # ---- Phase 2: 转换 ----
            try:
                transform_result = self._try_transform(data_type, source, raw_result.data)
            except SoftTransformError as e:
                # 软转换失败（price=None等脏数据）：不记死信，记录失败，切下一个源
                last_error = str(e)
                self._record_metric(source, data_type, success=False, is_transform_error=True, duration=raw_result.duration_seconds)
                logger.warning(f"  ⚠️ [{source}] {symbol}/{data_type} 软转换失败: {e}，切换下一源")
                continue

            if transform_result.ok:
                self._record_metric(source, data_type, success=True, duration=raw_result.duration_seconds)
                duration = (now_tz() - start_time).total_seconds()
                logger.info(f"  ✅ [{source}] {symbol}/{data_type} 成功 (耗时{duration:.1f}s)")
                return CollectResult(
                    success=True,
                    data=transform_result.data,
                    source=source,
                    symbol=symbol,
                    data_type=data_type,
                    duration_seconds=duration,
                )
            else:
                # 采集成功，转换失败 → 死信队列
                self._record_metric(source, data_type, success=False, is_transform_error=True, duration=raw_result.duration_seconds)
                self._enqueue_dead_letter(
                    data_type=data_type,
                    symbol=symbol,
                    source=source,
                    raw_sample=transform_result.data,  # 这里传原始raw
                    error_msg=transform_result.error,
                    stack=transform_result.stack,
                )
                last_error = f"转换失败({source}): {transform_result.error}"
                logger.error(f"  💀 [{source}] 转换失败，已入死信队列: {transform_result.error}")
                # 转换失败也切下一个信源尝试，不困在这里
                continue

        # 全链路失败
        duration = (now_tz() - start_time).total_seconds()
        logger.error(f"  ❌ 全链路失败 {symbol}/{data_type}: {last_error}")
        return CollectResult(
            success=False,
            data=None,
            source="chain",
            symbol=symbol,
            data_type=data_type,
            error=f"全链路失败: {last_error}",
            duration_seconds=duration,
        )

    # ==================== 链构建 ====================

    def _build_chain(self, data_type: str, symbol: str, backup_sources: List[str] = None) -> List[str]:
        """
        根据 data_type 和 source_metrics 构建动态链。
        成功率高的信源排前面；已降级的信源排到最后。
        """
        default = self.DEFAULT_CHAINS.get(data_type, [])
        if not default:
            return list(backup_sources) if backup_sources else []

        # 从数据库读 source_metrics，按 success_rate 降序排
        ranked = self._get_ranked_sources(data_type, default)

        # 追加手动指定的备选（未在默认链里出现过的）
        if backup_sources:
            for src in backup_sources:
                if src not in ranked:
                    ranked.append(src)

        return ranked

    def _get_ranked_sources(self, data_type: str, default_chain: List[str]) -> List[str]:
        """从 source_metrics 读取质量排名，重排链顺序"""
        rows = self.db.get_source_metrics(data_type)
        if not rows:
            return default_chain

        # 建立 metrics 查表
        metrics_map = {row['source']: row for row in rows}

        # 降级信源分离：未降级的按 success_rate 降序；降级的排最后；不在 metrics 的保留在链尾
        active = []
        degraded = []
        unranked = []  # 新信源，尚未积累 metrics，排在链尾

        for src in default_chain:
            if src not in metrics_map:
                # 新信源，尚未积累成功率记录，保留在链尾
                unranked.append(src)
                continue

            row = metrics_map[src]
            rate = row['success_rate'] or 1.0
            total = row['total'] or 0
            is_deg = row['is_degraded']

            if is_deg and total >= 5:
                degraded.append(src)
            else:
                active.append((src, rate, total))

        # 按 success_rate 降序排（成功率高的在前）
        active.sort(key=lambda x: (-x[1], -x[2]))
        return [src for src, _, _ in active] + degraded + unranked

    # ==================== 采集尝试 ====================

    async def _try_collect(
        self,
        collector: BaseCollector,
        source: str,
        data_type: str,
        symbol: str,
        config: Dict[str, Any],
    ) -> CollectResult:
        """对单个信源尝试采集，带超时"""
        try:
            async with asyncio.timeout(15):
                result = await collector.collect_with_retry(
                    symbol=symbol,
                    data_type=data_type,
                    max_retries=1,  # 重试在链级别做，不在单个采集器内做
                    **config,
                )
                return result
        except asyncio.TimeoutError:
            return CollectResult(
                success=False,
                data=None,
                source=source,
                symbol=symbol,
                data_type=data_type,
                error=f"[{source}] 采集超时(15s)",
                duration_seconds=15.0,
            )
        except Exception as e:
            return CollectResult(
                success=False,
                data=None,
                source=source,
                symbol=symbol,
                data_type=data_type,
                error=f"[{source}] 异常: {e}",
                duration_seconds=0.0,
            )

    # ==================== 数据转换 ====================

    def _try_transform(self, data_type: str, source: str, raw_data: Any) -> TransformResult:
        """
        尝试将采集器原始输出转换为标准格式。
        转换失败 → 死信，不阻塞链。
        """
        try:
            if data_type == "nav":
                data = self._transform_nav(raw_data, source)
            elif data_type == "estimate_nav":
                data = self._transform_estimate_nav(raw_data, source)
            elif data_type == "price":
                data = self._transform_price(raw_data, source)
            elif data_type == "minute":
                data = self._transform_minute(raw_data, source)
            elif data_type == "daily":
                data = self._transform_daily(raw_data, source)
            elif data_type == "fundamental":
                data = raw_data  # 透传
            elif data_type == "news":
                data = self._transform_news(raw_data, source)
            elif data_type == "index":
                data = self._transform_index(raw_data, source)
            elif data_type == "community":
                data = self._transform_community(raw_data, source)
            elif data_type == "announcement":
                data = raw_data
            elif data_type == "hsgt":
                data = raw_data
            elif data_type == "stock_basic":
                data = raw_data
            elif data_type == "news_aggregator":
                data = raw_data
            else:
                data = raw_data

            # 基础字段检查
            if data is None:
                return TransformResult(ok=False, error="转换结果为 None")
            return TransformResult(ok=True, data=data)

        except SoftTransformError:
            # 软失败：数据无效，不记死信，抛出去让调用方切下一个源
            raise
        except KeyError as e:
            return TransformResult(ok=False, error=f"缺少字段: {e}", stack=traceback.format_exc())
        except (TypeError, ValueError) as e:
            return TransformResult(ok=False, error=f"类型/值错误: {e}", stack=traceback.format_exc())
        except Exception as e:
            return TransformResult(ok=False, error=f"转换异常: {e}", stack=traceback.format_exc())

    def _transform_nav(self, raw, source: str) -> Dict[str, Any]:
        """标准化 nav 数据"""
        item = raw[0] if isinstance(raw, list) else raw
        return {
            "source": source,
            "symbol": item["symbol"],
            "nav_date": item["nav_date"],
            "nav": item["nav"],
            "acc_nav": item.get("acc_nav"),
            "change_pct": item.get("change_pct"),
            "raw": item,
        }

    def _transform_estimate_nav(self, raw, source: str) -> Dict[str, Any]:
        """标准化盘中估算净值"""
        return {
            "source": source,
            "symbol": raw["symbol"],
            "fund_name": raw.get("fund_name"),
            "nav_date": raw.get("nav_date"),
            "dwjz": raw.get("dwjz"),
            "estimate_nav": raw["estimate_nav"],
            "estimate_growth": raw.get("estimate_growth"),
            "estimate_time": raw["estimate_time"],
            "raw": raw,
        }

    def _transform_price(self, raw, source: str) -> Dict[str, Any]:
        """标准化 price 数据"""
        item = raw[0] if isinstance(raw, list) else raw
        # 软校验：price 为 None 或 0 → 抛 SoftTransformError，不记死信，直接切下一个源
        if item.get("price") is None or item.get("price") == 0:
            raise SoftTransformError(f"[{source}] 价格无效: price={item.get('price')}, symbol={item.get('symbol')}")
        return {
            "source": source,
            "symbol": item["symbol"],
            "trade_time": item["trade_time"],
            "price": item["price"],
            "volume": item.get("volume"),
            "amount": item.get("amount"),
            "high": item.get("high"),
            "low": item.get("low"),
            "open": item.get("open"),
            "prev_close": item.get("prev_close"),
            "raw": item,
        }

    def _transform_minute(self, raw, source: str) -> List[Dict[str, Any]]:
        """标准化 minute 数据（返回列表）"""
        bars = raw if isinstance(raw, list) else [raw]
        if bars and isinstance(bars[0], list):
            bars = bars[0]
        return [
            {
                "source": source,
                "symbol": b["symbol"],
                "trade_date": b.get("trade_date"),
                "trade_time": b["trade_time"],
                "open": b.get("open"),
                "high": b.get("high"),
                "low": b.get("low"),
                "close": b["close"],
                "volume": b.get("volume"),
                "amount": b.get("amount"),
                "raw": b,
            }
            for b in bars
        ]

    def _transform_daily(self, raw, source: str) -> Dict[str, Any]:
        """标准化 daily 数据"""
        item = raw[0] if isinstance(raw, list) else raw
        return {
            "source": source,
            "symbol": item["symbol"],
            "trade_date": item["trade_date"],
            "open": item["open"],
            "high": item["high"],
            "low": item["low"],
            "close": item["close"],
            "pre_close": item.get("pre_close"),
            "change": item.get("change"),
            "pct_chg": item.get("pct_chg"),
            "vol": item.get("vol"),
            "amount": item.get("amount"),
            "raw": item,
        }

    def _transform_news(self, raw, source: str) -> List[Dict[str, Any]]:
        """标准化 news 数据"""
        items = raw if isinstance(raw, list) else [raw]
        return [
            {
                "source": source,
                "title": item.get("title", ""),
                "summary": item.get("summary") or item.get("content", ""),
                "link": item["link"],
                "symbol": item.get("symbol"),
                "published": item.get("published") or item.get("publish_time", ""),
                "raw": item,
            }
            for item in items
        ]

    def _transform_index(self, raw, source: str) -> Dict[str, Any]:
        """标准化 index 数据"""
        item = raw[0] if isinstance(raw, list) else raw
        return {
            "source": source,
            "code": item["code"],
            "name": item.get("name", ""),
            "price": item.get("price"),
            "change_pct": item.get("change_pct"),
            "change_amount": item.get("change_amount"),
            "volume": item.get("volume"),
            "amount": item.get("amount"),
            "high": item.get("high"),
            "low": item.get("low"),
            "open": item.get("open"),
            "prev_close": item.get("prev_close"),
            "trade_date": item.get("trade_date"),
            "trade_time": item.get("trade_time"),
            "raw": item,
        }

    def _transform_community(self, raw, source: str) -> List[Dict[str, Any]]:
        """标准化 community 数据"""
        items = raw if isinstance(raw, list) else [raw]
        return [
            {
                "source": source,
                "symbol": item.get("symbol"),
                "title": item.get("title", ""),
                "author": item.get("author"),
                "reply_count": item.get("reply_count", 0),
                "click_count": item.get("click_count", 0),
                "published": item.get("published") or item.get("publish_time", ""),
                "link": item["link"],
                "raw": item,
            }
            for item in items
        ]

    # ==================== 指标记录 ====================

    def _record_metric(
        self,
        source: str,
        data_type: str,
        success: bool,
        is_transform_error: bool = False,
        duration: float = 0.0,
    ):
        """更新 source_metrics"""
        try:
            self.db.upsert_source_metric(
                source=source,
                data_type=data_type,
                success=success,
                is_transform_error=is_transform_error,
                duration=duration,
            )
            # 检查是否需要降级
            self._check_degrade(source, data_type)
        except Exception as e:
            logger.warning(f"_record_metric failed: {e}")

    # 连续成功多少次自动解除降级
    RECOVER_CONSECUTIVE_SUCCESS = 10

    def _check_degrade(self, source: str, data_type: str):
        """检查是否触发降级或恢复条件"""
        row = self.db.get_source_metric(source, data_type)
        if not row:
            return
        total = row['total'] or 0
        rate = row['success_rate'] or 1.0
        consec = row.get('consecutive_success') or 0
        is_deg = row['is_degraded']

        if is_deg:
            # 已降级：连续成功 >= RECOVER_CONSECUTIVE_SUCCESS 则自动恢复
            if consec >= self.RECOVER_CONSECUTIVE_SUCCESS:
                self.db.set_source_degraded(source, data_type, False)
                logger.warning(f"✅ 信源恢复: {source}/{data_type} (连续{consec}次成功)")
            return

        # 未降级：total > 10 且 success_rate < 80% 则降级
        if total > 10 and rate < self.DEGRADE_SUCCESS_RATE:
            self.db.set_source_degraded(source, data_type, True)
            logger.warning(f"⚠️ 信源降级: {source}/{data_type} (total={total}, rate={rate:.1%})")

    # ==================== 死信队列 ====================

    def _enqueue_dead_letter(
        self,
        data_type: str,
        symbol: str,
        source: str,
        raw_sample: Any,
        error_msg: str,
        stack: str = "",
    ):
        """将转换失败写入死信队列"""
        # 截取原始数据样例（最多500字符）
        raw_str = ""
        try:
            raw_str = json.dumps(raw_sample, ensure_ascii=False)
            if len(raw_str) > 500:
                raw_str = raw_str[:500] + "...[truncated]"
        except Exception:
            raw_str = str(raw_sample)[:500]

        try:
            self.db.insert_failed_transform(
                data_type=data_type,
                symbol=symbol,
                source=source,
                raw_sample=raw_str,
                error_message=error_msg,
                stack_trace=stack[:2000] if stack else "",
            )
            logger.info(f"  💀 死信已入队: {source}/{data_type}/{symbol} -> {error_msg}")
        except Exception as e:
            logger.error(f"  💀 死信入队失败: {e}")
