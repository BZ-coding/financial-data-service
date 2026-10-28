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
import json
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

    # ==================== 财经快讯四源（2026-10-28 恢复）====================
    #
    # 这四个源在 **2026-06-11 06:35:57 同时停止**（库里最后写入时间一致），
    # 采集器实现被移除，但 news_aggregator_data 表里**列还在**（caijing_items 等），
    # 且有 12,748 行历史数据。
    # ⚠️ 后果：Hermes 的 cron prompt **明确依赖 `caijing_items`（财经网快讯）**，
    #    但 API 的 get_latest_news_aggregator 根本不 SELECT 这些列
    #    → agent 拿到的是"没有这些字段"→ **把"源挂了"误读成"今天没快讯"**。
    #
    # 现在恢复采集，并给每个源加**显式状态标记**（ok / empty / source_unavailable），
    # **绝不静默缺字段**。

    _FIN_SOURCES = ("caijing", "eeo", "sina_live", "ths_hk")

    def _http_get(self, url: str, timeout: int = 12, encoding: str = None) -> Optional[str]:
        """同步 GET，失败返回 None（调用方据此标 source_unavailable）"""
        try:
            resp = requests.get(url, timeout=timeout, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
                "Referer": "https://www.google.com/",
            })
            if resp.status_code != 200:
                logger.warning(f"GET {url} → HTTP {resp.status_code}")
                return None
            # 编码：这两个站点是 UTF-8，但 requests 按 header 猜可能猜成 ISO-8859-1
            # → 中文乱码。显式用 UTF-8 解码（errors='replace' 防个别坏字节）。
            raw = resp.content
            for enc in ([encoding] if encoding else []) + ["utf-8", "gbk"]:
                try:
                    txt = raw.decode(enc)
                    # 简单校验：解码结果里若几乎没有中文，多半选错编码
                    if enc != "gbk" and ("\u4e00" <= txt[:4000] <= "\u9fff") is False and txt.count("\ufffd") > 0:
                        continue
                    return txt
                except (UnicodeDecodeError, LookupError):
                    continue
            return raw.decode("utf-8", errors="replace")
        except Exception as e:
            logger.warning(f"GET {url} 失败: {type(e).__name__}: {e}")
            return None

    def _fetch_caijing_sync(self, limit: int = 20) -> Optional[List[Dict[str, Any]]]:
        """财经网快讯（finance.caijing.com.cn 首页快讯链接解析）"""
        html = self._http_get("http://finance.caijing.com.cn/")
        if not html:
            return None
        # 注意分组：1=url 2=日期 3=文章号 4=标题（标题在最后）
        pat = re.compile(
            r'<a[^>]+href="(http://(?:finance|economy)\.caijing\.com\.cn/(20\d{6})/(\d+)\.shtml)"[^>]*>\s*([^<]{6,60}?)\s*</a>')
        items, seen = [], set()
        for m in pat.finditer(html):
            url, ymd, title = m.group(1), m.group(2), m.group(4).strip()
            if url in seen or not title:
                continue
            seen.add(url)
            items.append({
                "source": "caijing",
                "title": title,
                "url": url,
                "published": f"{ymd[4:6]}-{ymd[6:8]} {ymd[8:10] if len(ymd)>8 else ''}".strip(),
                "cat": "快讯",
                "caturl": "http://finance.caijing.com.cn/",
            })
            if len(items) >= limit:
                break
        return items

    def _fetch_eeo_sync(self, limit: int = 10) -> Optional[List[Dict[str, Any]]]:
        """经济观察网快讯（www.eeo.com.cn 首页链接解析）"""
        html = self._http_get("http://www.eeo.com.cn/")
        if not html:
            return None
        # 注意分组：1=url 2=年 3=月日 4=文章号 5=标题（标题在最后）
        pat = re.compile(
            r'<a[^>]+href="(http://www\.eeo\.com\.cn/(20\d{2})/(\d{4})/(\d+)\.shtml)"[^>]*>\s*([^<]{6,60}?)\s*</a>')
        items, seen = [], set()
        for m in pat.finditer(html):
            url, y, md, title = m.group(1), m.group(2), m.group(3), m.group(5).strip()
            if url in seen or not title:
                continue
            seen.add(url)
            items.append({
                "source": "eeo",
                "title": title,
                "link": url,
                "category": "快讯",
                "pub_date": f"{y}-{md}",
            })
            if len(items) >= limit:
                break
        return items

    def _fetch_sina_live_sync(self, limit: int = 30) -> Optional[List[Dict[str, Any]]]:
        """新浪财经直播（zhibo_id=152）"""
        url = ("https://zhibo.sina.com.cn/api/zhibo/feed?zhibo_id=152&page=1"
               f"&page_size={limit}&zhibo_feed_content_type=102")
        txt = self._http_get(url)
        if not txt:
            return None
        try:
            d = json.loads(txt)
            lst = d.get("result", {}).get("data", {}).get("feed", {}).get("list", []) or []
        except Exception as e:
            logger.warning(f"sina_live 解析失败: {e}")
            return None
        items = []
        for it in lst:
            items.append({
                "source": "sina_live",
                "content": (it.get("rich_text") or "").strip(),
                "create_time": it.get("create_time") or "",
                "tag": (it.get("tag") or [{}])[0].get("tag_name", "") if isinstance(it.get("tag"), list) else "",
            })
        return items

    def _fetch_ths_hk_sync(self, limit: int = 20) -> Optional[List[Dict[str, Any]]]:
        """同花顺港股快讯（10jqka push API）"""
        url = (f"https://news.10jqka.com.cn/tapp/news/push/stock/?page=1&tag=&track=website&pagesize={limit}")
        txt = self._http_get(url)
        if not txt:
            return None
        try:
            d = json.loads(txt)
            lst = d.get("data", {}).get("list", []) or []
        except Exception as e:
            logger.warning(f"ths_hk 解析失败: {e}")
            return None
        items = []
        for it in lst:
            items.append({
                "source": "ths_hk",
                "title": it.get("title", ""),
                "digest": it.get("digest", ""),
                "ctime": it.get("ctime") or it.get("seq") or "",
                "tags": it.get("tag", "") or "",
                "url": it.get("url", "") or "",
            })
        return items

    async def fetch_fin_sources(self) -> Dict[str, Any]:
        """并发采集四个财经快讯源，返回 {源名: {"items": [...], "status": str}}"""
        fns = {
            "caijing":   self._fetch_caijing_sync,
            "eeo":       self._fetch_eeo_sync,
            "sina_live": self._fetch_sina_live_sync,
            "ths_hk":    self._fetch_ths_hk_sync,
        }

        async def one(name):
            f = fns[name]
            try:
                items = await asyncio.to_thread(f)
            except Exception as e:
                logger.warning(f"{name} 采集异常: {type(e).__name__}: {e}")
                items = None
            if items is None:
                return name, {"items": [], "status": "source_unavailable"}
            if not items:
                return name, {"items": [], "status": "empty"}
            return name, {"items": items, "status": "ok"}

        results = await asyncio.gather(*[one(n) for n in fns], return_exceptions=True)
        out = {}
        for r in results:
            if isinstance(r, Exception):
                continue
            out[r[0]] = r[1]
        return out

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
        # ★ 2026-10-28：恢复四个财经快讯源（caijing/eeo/sina_live/ths_hk）
        #   —— Hermes 的 cron prompt 明确依赖 caijing_items（财经网快讯）
        fin_task = asyncio.create_task(self.fetch_fin_sources())

        hn_results, gh_results, ths_results, fin_results = await asyncio.gather(
            hn_task, gh_task, ths_task, fin_task, return_exceptions=True
        )
        if isinstance(fin_results, Exception):
            logger.warning(f"fin sources exception: {fin_results}")
            fin_results = {}

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
        # ★ 四个财经快讯源 + **显式状态标记**（绝不静默缺字段）
        #   status: ok / empty / source_unavailable
        #   → agent 能分辨"源挂了"和"今天没快讯"
        for name in ("caijing", "eeo", "sina_live", "ths_hk"):
            info = (fin_results or {}).get(name) or {"items": [], "status": "source_unavailable"}
            combined[name] = {"count": len(info.get("items", [])), "items": info.get("items", [])}
            combined[f"{name}_status"] = info.get("status", "source_unavailable")

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
