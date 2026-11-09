#!/usr/bin/env python3
"""
MiniMax Search 采集器
使用 mmx CLI 获取搜索结果，补充传统 RSS/API 无法覆盖的内容
"""
import asyncio
import json
import logging
import os
import subprocess
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from .base import BaseCollector, CollectResult

logger = logging.getLogger(__name__)


class MmxSearchCollector(BaseCollector):
    """MiniMax Search 采集器"""

    def __init__(
        self,
        rate_limit_per_minute: int = 10,
        default_queries: List[str] = None,
        max_results_per_query: int = 10,
    ):
        super().__init__(rate_limit_per_minute)
        self.default_queries = default_queries or [
            "A股今日行情",
            "基金净值",
            "美股行情",
        ]
        self.max_results_per_query = max_results_per_query
        # 启动时主动检测 mmx 可用性（系统 PATH 必须含 mmx；systemd --user 实例
        # 默认不带 .npm-global/bin，所以 unit file 必须显式 Environment="PATH=..."）
        from shutil import which
        self._mmx_available = which("mmx") is not None
        if not self._mmx_available:
            logger.warning(
                "mmx CLI 未在 PATH 中找到（尝试 which mmx 失败），"
                "mmx_search 采集器不可用；如已安装请检查 systemd unit 的 Environment PATH"
            )

    def _run_mmx(self, query: str) -> Optional[Dict[str, Any]]:
        """同步调用 mmx search，返回原始 JSON。
        _mmx_available 由 __init__ 设置，这里直接信任。
        """
        if not getattr(self, "_mmx_available", False):
            return None

        try:
            result = subprocess.run(
                ["mmx", "search", "query", "--q", query, "--output", "json"],
                capture_output=True,
                text=True,
                timeout=30,
                env={**os.environ, "PATH": os.environ.get("PATH", "")},
            )
            if result.returncode == 0 and result.stdout:
                return json.loads(result.stdout)
        except subprocess.TimeoutExpired:
            logger.warning(f"mmx search 超时: {query}")
        except Exception as e:
            logger.warning(f"mmx search 失败: {e}")
        return None

    async def search(self, query: str) -> List[Dict[str, Any]]:
        """搜索单个 query"""
        await self._acquire_token()
        loop = asyncio.get_event_loop()

        def _sync():
            return self._run_mmx(query)

        raw = await loop.run_in_executor(None, _sync)
        if not raw:
            return []

        # 统一解析格式（mmx search 输出结构）
        results = []
        items = raw.get("organic", raw.get("results", raw.get("data", [])))
        if isinstance(items, list):
            for item in items[: self.max_results_per_query]:
                results.append({
                    "query": query,
                    "title": item.get("title", ""),
                    "link": item.get("link", item.get("url", "")),
                    "snippet": item.get("snippet", item.get("description", "")),
                    "date": item.get("date", ""),
                })

        return results

    async def collect(
        self, symbol: str = "__search__", data_type: str = "mmx_search", **kwargs
    ) -> CollectResult:
        """采集：执行所有默认 queries"""
        start = time.time()
        query = kwargs.get("query")
        queries = [query] if query else self.default_queries

        # mmx CLI 不可用时直接失败（避免 scheduler 把"空结果"当成功）
        if not getattr(self, "_mmx_available", False):
            return CollectResult(
                success=False,
                data=None,
                source="mmx_search",
                symbol="__search__",
                data_type="mmx_search",
                error="mmx CLI 未安装，mmx_search 采集器不可用",
                duration_seconds=time.time() - start,
            )

        all_results = []
        for q in queries:
            items = await self.search(q)
            all_results.extend(items)
            await asyncio.sleep(0.5)  # 避免太频繁

        collected_at = datetime.now(timezone.utc).isoformat()

        return CollectResult(
            success=True,
            data={
                "collected_at": collected_at,
                "query_count": len(queries),
                "total_results": len(all_results),
                "items": all_results,
            },
            source="mmx_search",
            symbol="__search__",
            data_type="mmx_search",
            duration_seconds=time.time() - start,
        )

    async def collect_batch(
        self, symbols: List[str], data_type: str = "mmx_search", **kwargs
    ) -> List[CollectResult]:
        """批量搜索"""
        results = []
        for sym in symbols:
            r = await self.collect(sym, data_type, **kwargs)
            results.append(r)
            await asyncio.sleep(1)
        return results
