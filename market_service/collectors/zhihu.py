#!/usr/bin/env python3
"""
知乎数据采集器 — 包装知乎数据开放平台 4 个 MCP 接口
- zhihu_search: 知乎站内搜索（按 query）
- zhihu_global_search: 全网搜索（知乎+全网权威源）
- zhihu_hot_list: 实时热榜
- zhihu_zhida: 知乎直答 LLM 问答（每天限 10 次，慎用）
"""

import sys
import os
import asyncio
import json
import time
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta

from .base import BaseCollector, CollectResult

# 让 /home/zsd/zhihu_mcp.py 可被 import
_ZHIHU_LIB_PATH = '/home/zsd'
if _ZHIHU_LIB_PATH not in sys.path:
    sys.path.insert(0, _ZHIHU_LIB_PATH)

logger = logging.getLogger(__name__)
TZ = timezone(timedelta(hours=8))


class ZhihuCollector(BaseCollector):
    """知乎数据开放平台采集器"""

    def __init__(
        self,
        rate_limit_per_minute: int = 30,
        default_search_queries: List[str] = None,
        default_global_queries: List[str] = None,
    ):
        super().__init__(rate_limit_per_minute)
        self.default_search_queries = default_search_queries or [
            "易方达蓝筹精选",
            "中欧医疗健康",
            "金风科技 002202",
            "半导体 国产替代 2026",
            "美联储 降息",
        ]
        self.default_global_queries = default_global_queries or [
            "A股 今日行情",
            "新基金发行 2026",
            "新能源 政策",
        ]
        self.logger = logging.getLogger(__name__)
        # 延迟 import — 避免 collector 加载时 token 问题
        try:
            from zhihu_mcp import zhihu_search, global_search, hot_list, ask_zhida
            self._zhihu_search = zhihu_search
            self._global_search = global_search
            self._hot_list = hot_list
            self._ask_zhida = ask_zhida
            self._available = True
            logger.info("ZhihuCollector 初始化成功，知乎 4 个工具已加载")
        except Exception as e:
            logger.warning(f"ZhihuCollector 加载 zhihu_mcp 失败: {e}")
            self._available = False

    def _search_sync(self, query: str, count: int, search_kind: str = 'site') -> List[Dict[str, Any]]:
        """同步调知乎搜索/全网搜索。search_kind: 'site' / 'global'"""
        fn = self._zhihu_search if search_kind == 'site' else self._global_search
        items = fn(query, count=count)
        # 转换: 知乎 MCP 返回 dict，标注 query 字段方便聚合
        return [{'query': query, **item} for item in items]

    def _hot_list_sync(self, limit: int) -> List[Dict[str, Any]]:
        """同步调知乎热榜"""
        return self._hot_list(limit=limit)

    def _zhida_sync(self, question: str, model: str) -> str:
        """同步调知乎直答"""
        return self._ask_zhida(question, model=model)

    async def collect(
        self,
        symbol: str = "__zhihu__",
        data_type: str = "zhihu_search",
        **kwargs,
    ) -> CollectResult:
        """单次采集入口。
        data_type:
          - zhihu_search: 按 query 查站内（kwargs.query 优先，否则 symbol 当 query）
          - zhihu_global_search: 按 query 查全网（同上）
          - zhihu_hot_list: 拉热榜（symbol 无意义，kwargs.limit 默认 30）
          - zhihu_zhida: 问直答（symbol 当 question，kwargs.model 默认 fast）
        """
        start = time.time()
        if not self._available:
            return CollectResult(
                success=False, data=None, source="zhihu", symbol=symbol,
                data_type=data_type, error="zhihu_mcp 不可用",
                duration_seconds=time.time() - start,
            )

        loop = asyncio.get_event_loop()

        try:
            if data_type == "zhihu_search":
                # 优先用 config.queries 列表（scheduler 自动从 subscription.config 传过来）
                queries = kwargs.get("queries")
                count = int(kwargs.get("count", 5))
                if queries:
                    # 批量模式：跑多个 query
                    all_items = []
                    for q in queries:
                        items = await loop.run_in_executor(
                            None, self._search_sync, q, count, 'site'
                        )
                        all_items.extend(items)
                        await asyncio.sleep(0.3)
                    return CollectResult(
                        success=True, data=all_items, source="zhihu",
                        symbol="|".join(queries)[:80], data_type=data_type,
                        duration_seconds=time.time() - start,
                    )
                # 单 query 模式
                query = kwargs.get("query") or symbol
                items = await loop.run_in_executor(
                    None, self._search_sync, query, count, 'site'
                )
                return CollectResult(
                    success=True, data=items, source="zhihu",
                    symbol=query, data_type=data_type,
                    duration_seconds=time.time() - start,
                )

            elif data_type == "zhihu_global_search":
                queries = kwargs.get("queries")
                count = int(kwargs.get("count", 5))
                if queries:
                    all_items = []
                    for q in queries:
                        items = await loop.run_in_executor(
                            None, self._search_sync, q, count, 'global'
                        )
                        all_items.extend(items)
                        await asyncio.sleep(0.3)
                    return CollectResult(
                        success=True, data=all_items, source="zhihu",
                        symbol="|".join(queries)[:80], data_type=data_type,
                        duration_seconds=time.time() - start,
                    )
                query = kwargs.get("query") or symbol
                items = await loop.run_in_executor(
                    None, self._search_sync, query, count, 'global'
                )
                return CollectResult(
                    success=True, data=items, source="zhihu",
                    symbol=query, data_type=data_type,
                    duration_seconds=time.time() - start,
                )

            elif data_type == "zhihu_hot_list":
                limit = int(kwargs.get("limit", 30))
                items = await loop.run_in_executor(None, self._hot_list_sync, limit)
                return CollectResult(
                    success=True, data=items, source="zhihu",
                    symbol="__hot_list__", data_type=data_type,
                    duration_seconds=time.time() - start,
                )

            elif data_type == "zhihu_zhida":
                question = kwargs.get("question") or symbol
                model = kwargs.get("model", "zhida-fast-1p5")
                answer = await loop.run_in_executor(
                    None, self._zhida_sync, question, model
                )
                return CollectResult(
                    success=True,
                    data={'question': question, 'model': model, 'answer': answer},
                    source="zhihu", symbol=question, data_type=data_type,
                    duration_seconds=time.time() - start,
                )

            else:
                return CollectResult(
                    success=False, data=None, source="zhihu", symbol=symbol,
                    data_type=data_type, error=f"不支持的 data_type: {data_type}",
                    duration_seconds=time.time() - start,
                )

        except Exception as e:
            self.logger.warning(f"ZhihuCollector.collect({data_type}, {symbol}) 异常: {e}")
            return CollectResult(
                success=False, data=None, source="zhihu", symbol=symbol,
                data_type=data_type, error=str(e),
                duration_seconds=time.time() - start,
            )

    async def collect_batch(
        self,
        symbols: List[str],
        data_type: str = "zhihu_search",
        **kwargs,
    ) -> List[CollectResult]:
        """批量采集 — 一次跑多个 query
        对 zhihu_search / zhihu_global_search: 把 symbols 列表当 query 列表
        对其他: 单次跑
        """
        if data_type in ("zhihu_search", "zhihu_global_search") and symbols:
            # symbols 当 query 列表
            queries = kwargs.get("queries") or symbols
            count = int(kwargs.get("count", 5))
            results = []
            for q in queries:
                r = await self.collect(q, data_type, query=q, count=count)
                results.append(r)
                await asyncio.sleep(0.5)
            return results
        # 其他类型走单条
        sym = symbols[0] if symbols else "__zhihu__"
        return [await self.collect(sym, data_type, **kwargs)]
