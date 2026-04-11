#!/usr/bin/env python3
"""
News Aggregator - 全网新闻聚合采集器
数据源：
  - HackerNews Top Stories（Firebase API，无认证）
  - GitHub Trending（GitHub API，public）
  - 同花顺问财（财经新闻，requests）

功能：
  - 每日生成市场情绪早报
  - 追踪科技/AI/金融热点
  - 为深度报告提供背景素材
"""

import asyncio
import logging
import re
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

import requests

from .base import BaseCollector, CollectResult

logger = logging.getLogger(__name__)


class NewsAggregatorCollector(BaseCollector):
    """全网新闻聚合采集器"""

    def __init__(
        self,
        rate_limit_per_minute: int = 30,
        hn_count: int = 10,
        github_count: int = 5,
        ths_count: int = 10,
    ):
        super().__init__(rate_limit_per_minute)
        self.hn_count = hn_count
        self.github_count = github_count
        self.ths_count = ths_count

    # ==================== HackerNews ====================

    async def fetch_hackernews(self) -> List[Dict[str, Any]]:
        """获取 HackerNews Top Stories"""
        loop = asyncio.get_event_loop()
        try:
            def _get_ids():
                r = requests.get(
                    "https://hacker-news.firebaseio.com/v0/topstories.json",
                    timeout=10,
                )
                return r.json()

            ids = await loop.run_in_executor(None, _get_ids)
            top_ids = ids[: self.hn_count]

            async def fetch_item(item_id: int) -> Optional[Dict]:
                def _get():
                    r = requests.get(
                        f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json",
                        timeout=10,
                    )
                    return r.json()

                try:
                    item = await loop.run_in_executor(None, _get)
                    if item and item.get("type") == "story":
                        return {
                            "source": "hackernews",
                            "id": item["id"],
                            "title": item.get("title", ""),
                            "url": item.get("url", f"https://news.ycombinator.com/item?id={item['id']}"),
                            "score": item.get("score", 0),
                            "by": item.get("by", ""),
                            "descendants": item.get("descendants", 0),
                            "time": datetime.fromtimestamp(item.get("time", 0), tz=timezone.utc).isoformat(),
                        }
                except Exception:
                    pass
                return None

            tasks = [fetch_item(i) for i in top_ids]
            results = await asyncio.gather(*tasks)
            return [r for r in results if r is not None]

        except Exception as e:
            logger.warning(f"HackerNews fetch failed: {e}")
            return []

    # ==================== GitHub Trending ====================

    async def fetch_github_trending(self) -> List[Dict[str, Any]]:
        """获取 GitHub Trending（最近30天新晋热门仓库）"""
        loop = asyncio.get_event_loop()
        try:
            date_to = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            date_from = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")

            params = {
                "q": f"created:{date_from}..{date_to}",
                "sort": "stars",
                "order": "desc",
                "per_page": self.github_count,
            }
            headers = {
                "Accept": "application/vnd.github.v3+json",
                "User-Agent": "Hermes-News-Aggregator/1.0",
            }

            def _fetch():
                return requests.get(
                    "https://api.github.com/search/repositories",
                    params=params,
                    headers=headers,
                    timeout=15,
                )

            r = await loop.run_in_executor(None, _fetch)
            if r.status_code == 403:
                logger.warning("GitHub API rate limited, falling back to page scrape")
                return await self._fetch_github_page()
            if r.status_code != 200:
                logger.warning(f"GitHub API error: {r.status_code}")
                return []

            data = r.json()
            return [
                {
                    "source": "github",
                    "name": item.get("full_name", ""),
                    "description": item.get("description", "") or "",
                    "stars": item.get("stargazers_count", 0),
                    "language": item.get("language", "") or "",
                    "url": item.get("html_url", ""),
                    "created_at": item.get("created_at", "")[:10],
                }
                for item in data.get("items", [])[: self.github_count]
            ]
        except Exception as e:
            logger.warning(f"GitHub trending failed: {e}")
            return []

    async def _fetch_github_page(self) -> List[Dict[str, Any]]:
        """GitHub API 限流时：直接爬取 Trending 页面"""
        loop = asyncio.get_event_loop()
        try:

            def _fetch():
                return requests.get(
                    "https://github.com/trending",
                    headers={
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                        "Accept": "text/html,application/xhtml+xml",
                    },
                    timeout=10,
                )

            r = await loop.run_in_executor(None, _fetch)
            if r.status_code != 200:
                return []

            # 解析 <article class="Box-row"> ... </article>
            articles = re.findall(r'<article class="Box-row">(.*?)</article>', r.text, re.DOTALL)
            results = []
            for article in articles[: self.github_count]:
                name_match = re.search(r'<a href="/([^"]+?)"', article)
                desc_match = re.search(r'<p[^>]*class="col-9[^"]*"[^>]*>(.*?)</p>', article, re.DOTALL)
                stars_match = re.search(r'(\d[\d,\.]*)\s*stars?', article)
                lang_match = re.search(r'<span[^>]*itemprop="name"[^>]*>(.*?)</span>', article, re.DOTALL)

                name = name_match.group(1).strip() if name_match else ""
                desc = re.sub(r"<[^>]+>", "", desc_match.group(1)).strip() if desc_match else ""
                stars_str = stars_match.group(1).strip() if stars_match else "0"
                stars = int(stars_str.replace(",", ""))
                lang = lang_match.group(1).strip() if lang_match else ""

                results.append({
                    "source": "github",
                    "name": name,
                    "description": desc,
                    "stars": stars,
                    "language": lang,
                    "url": f"https://github.com/{name}",
                })
            return results
        except Exception as e:
            logger.warning(f"GitHub page scrape failed: {e}")
            return []

    # ==================== 同花顺问财 ====================

    async def fetch_ths_news(self) -> List[Dict[str, Any]]:
        """获取同花顺问财财经新闻"""
        loop = asyncio.get_event_loop()
        try:

            def _fetch():
                return requests.get(
                    "https://news.10jqka.com.cn/tapp/news/push/stock/",
                    params={"page": 1, "tag": "", "track": "website", "pagesize": self.ths_count},
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=10,
                )

            r = await loop.run_in_executor(None, _fetch)
            if r.status_code != 200:
                return []

            data = r.json()
            items = data.get("data", {}).get("list", [])
            results = []
            for item in items:
                ctime = item.get("ctime", "")
                if ctime and ctime.isdigit():
                    ts = int(ctime)
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                    dt_str = dt.astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
                else:
                    dt_str = ctime or ""

                results.append({
                    "source": "ths",
                    "title": item.get("title", ""),
                    "ctime": dt_str,
                    "column": item.get("column", ""),
                    "url": item.get("articlUrl", ""),
                })
            return results
        except Exception as e:
            logger.warning(f"同花顺 news fetch failed: {e}")
            return []

    # ==================== 主采集入口 ====================

    async def collect(
        self,
        symbol: str,
        data_type: str = "news",
        **kwargs,
    ) -> CollectResult:
        """并发采集三路新闻，返回聚合结果"""
        start = datetime.now(timezone(timedelta(hours=8)))

        hn_task = asyncio.create_task(self.fetch_hackernews())
        gh_task = asyncio.create_task(self.fetch_github_trending())
        ths_task = asyncio.create_task(self.fetch_ths_news())

        hn_results, gh_results, ths_results = await asyncio.gather(
            hn_task, gh_task, ths_task, return_exceptions=True
        )

        if isinstance(hn_results, Exception):
            logger.warning(f"HN exception: {hn_results}")
            hn_results = []
        if isinstance(gh_results, Exception):
            logger.warning(f"GitHub exception: {gh_results}")
            gh_results = []
        if isinstance(ths_results, Exception):
            logger.warning(f"THS exception: {ths_results}")
            ths_results = []

        now_iso = datetime.now(timezone(timedelta(hours=8))).isoformat()

        combined = {
            "collected_at": now_iso,
            "hackernews": {"count": len(hn_results), "items": hn_results},
            "github_trending": {"count": len(gh_results), "items": gh_results},
            "ths_news": {"count": len(ths_results), "items": ths_results},
            "total_sources": sum([1 if hn_results else 0, 1 if gh_results else 0, 1 if ths_results else 0]),
        }

        return CollectResult(
            success=True,
            data=combined,
            source="news_aggregator",
            symbol="*",
            data_type="news_aggregator",
            duration_seconds=(datetime.now(timezone(timedelta(hours=8))) - start).total_seconds(),
        )

    async def collect_batch(
        self, symbols: List[str], data_type: str = "news", **kwargs
    ) -> List[CollectResult]:
        """批量采集"""
        r = await self.collect(symbols[0] if symbols else "*", data_type, **kwargs)
        return [r]
