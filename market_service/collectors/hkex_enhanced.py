#!/usr/bin/env python3
"""
HKEX 港交所披露易公告采集器 (2026-07-06 沉淀)

替代 stock_news fallback 给港股标的，提供**标的专属公告**（HKEX 披露易 PDF 公告 + 标题 + 类型 + 日期）。

数据源: https://www1.hkexnews.hk/search/titleSearchServlet.do (HKEX 官方 title search XHR)
        https://www1.hkexnews.hk/search/prefix.do (autocomplete: code → stockId 映射)
        https://www1.hkexnews.hk/listedco/listconews/sehk/<YYYY>/<MMDD>/<file>.pdf (公告 PDF 直链)

设计原则:
- 复用 BaseCollector 接口，跟 community_enhanced 同款
- stockId 映射本地 JSON 缓存 (避免每次重查 prefix.do)
- 单次请求 100 条上限 + hasNextRow 分页
- 重试用指数退避 (HKEX 慢，13-25s 一次 + SSL handshake 偶超时)
- 入库用 insert_news() 复用 news_data 表 + UNIQUE(link) 去重
- 失败 fallback: 返回 success=False, scheduler 会切下一个源 (stock_news)
"""

import asyncio
import html as _html
import json
import logging
import os
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional

import requests

from .base import BaseCollector, CollectResult

logger = logging.getLogger(__name__)

TZ = timezone(timedelta(hours=8))

# HKEX 端点 (从浏览器开发者工具实测)
HKEX_TITLE_SEARCH_URL = "https://www1.hkexnews.hk/search/titleSearchServlet.do"
HKEX_PREFIX_URL = "https://www1.hkexnews.hk/search/prefix.do"
HKEX_BASE_URL = "https://www1.hkexnews.hk"

# stockId 缓存文件路径 (项目内, 不放 /tmp 因为跨重启要保留)
STOCKID_CACHE = Path("/home/zsd/financial-data-service/market_service/config/hkex_stockid_cache.json")


def _now_tz() -> datetime:
    return datetime.now(TZ)


class HkexEnhancedCollector(BaseCollector):
    """
    HKEX 港交所披露易公告采集器

    data_type: "hkex_announcement"  (新增, 跟 stock_news 平级)
    symbol: hk 开头的港股代码 (如 "hk03690" / "hk09988")
    返回: List[{source, title, summary, url/link, published, news_id, long_text}]
    """

    def __init__(self, rate_limit_per_minute: int = 10):
        # HKEX 慢 + 容易被封, 给低 rate_limit
        super().__init__(rate_limit_per_minute=rate_limit_per_minute)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0",
            "Accept": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://www1.hkexnews.hk/search/titlesearch.xhtml",
        })
        self._stockid_cache: Dict[str, int] = {}
        self._load_stockid_cache()

    # ==================== stockId 缓存 ====================

    def _load_stockid_cache(self):
        """启动时读 stockId 缓存 (避免重复调 prefix.do)"""
        try:
            if STOCKID_CACHE.exists():
                with STOCKID_CACHE.open("r", encoding="utf-8") as f:
                    self._stockid_cache = json.load(f)
                logger.info(f"HKEX stockId 缓存加载: {len(self._stockid_cache)} 条")
            else:
                self._stockid_cache = {}
        except Exception as e:
            logger.warning(f"HKEX stockId 缓存加载失败: {e}")
            self._stockid_cache = {}

    def _save_stockid_cache(self):
        """持久化 stockId 缓存"""
        try:
            STOCKID_CACHE.parent.mkdir(parents=True, exist_ok=True)
            with STOCKID_CACHE.open("w", encoding="utf-8") as f:
                json.dump(self._stockid_cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"HKEX stockId 缓存保存失败: {e}")

    def _strip_hk_prefix(self, symbol: str) -> str:
        """'hk03690' → '03690'"""
        if symbol.startswith("hk"):
            return symbol[2:]
        return symbol

    def _add_hk_prefix(self, code: str) -> str:
        """'03690' → 'hk03690'"""
        if not code.startswith("hk"):
            return f"hk{code}"
        return code

    def _get_stock_id(self, symbol: str) -> Optional[int]:
        """
        HKEX code → stockId 映射
        - 先查缓存
        - 没命中调 prefix.do
        """
        code = self._strip_hk_prefix(symbol)
        if code in self._stockid_cache:
            return self._stockid_cache[code]

        # 没命中, 调 prefix.do 查询
        for attempt in range(3):
            try:
                resp = self.session.get(
                    HKEX_PREFIX_URL,
                    params={
                        "callback": "cb",
                        "lang": "EN",
                        "type": "A",
                        "name": code,
                        "market": "SEHK",
                    },
                    timeout=30,   # 2026-10-26: 45→30，配合应用层单次重试，最坏 60s = tick 边界内
                )
                if resp.status_code != 200:
                    logger.warning(f"prefix.do HTTP {resp.status_code}, retry")
                    time.sleep(2 ** attempt)
                    continue

                # JSONP 解析: callback({...}); 或 callback({...})
                # 实测响应末尾可能是 ; 或裸 JSON, regex 容错
                m = re.search(r"callback\s*\(\s*(\{.*?\})\s*\)\s*;?\s*$", resp.text.strip(), re.S)
                if not m:
                    logger.warning(f"prefix.do 响应非 JSONP: {resp.text[:200]}")
                    return None
                data = json.loads(m.group(1))
                stock_info = data.get("stockInfo") or []
                if not stock_info:
                    logger.warning(f"prefix.do 未找到 {code}: {data}")
                    return None
                stock_id = int(stock_info[0]["stockId"])
                # 缓存
                self._stockid_cache[code] = stock_id
                self._save_stockid_cache()
                logger.info(f"HKEX stockId 查询: {code} → {stock_id}")
                return stock_id
            except Exception as e:
                logger.warning(f"prefix.do 异常 (attempt {attempt+1}): {e}")
                time.sleep(2 ** attempt)
        return None

    # ==================== HKEX title search ====================

    def _fetch_announcements_sync(
        self,
        stock_id: int,
        code: str,
        from_date: str,
        to_date: str,
        row_range: int = 100,
        lang: str = "EN",
    ) -> Optional[List[Dict[str, Any]]]:
        """
        单次 titleSearchServlet 查询 (同步版, BaseCollector.collect 用 async 包)
        from_date / to_date: "YYYYMMDD" 格式 (HKEX 真实格式, 不是 DD/MM/YYYY — 2026-07-06 实测踩坑)
        """
        params = {
            "sortDir": "0",
            "sortByOptions": "DateTime",
            "category": "0",
            "market": "SEHK",
            "stockId": str(stock_id),
            "documentType": "-1",
            "fromDate": from_date,
            "toDate": to_date,
            "t1code": "-2",
            "t2Gcode": "-2",
            "t2code": "-2",
            "rowRange": str(row_range),
            "lang": lang,
        }
        resp = self.session.get(HKEX_TITLE_SEARCH_URL, params=params, timeout=30)  # 2026-10-26: 60→30，配合单次重试最坏 60s
        if resp.status_code != 200:
            raise Exception(f"HTTP {resp.status_code}")
        data = resp.json()
        result_str = data.get("result", "[]")
        if not result_str:
            return []
        items = json.loads(result_str)
        return items if isinstance(items, list) else []

    def _parse_hkex_item(self, item: Dict[str, Any], symbol: str) -> Dict[str, Any]:
        """
        HKEX API 返的 raw item → 通用 news schema
        HKEX fields: NEWS_ID, TITLE, LONG_TEXT, DATE_TIME (DD/MM/YYYY HH:MM),
                     FILE_LINK, FILE_TYPE, STOCK_CODE, STOCK_NAME, FILE_INFO, SHORT_TEXT
        注意: SHORT_TEXT/LONG_TEXT/TITLE 含 HTML entities (e.g. &#x2f; = '/'), 用 html.unescape 解码
        """
        date_time = item.get("DATE_TIME", "")  # "03/07/2026 18:51"
        published_iso = self._parse_hkex_datetime(date_time)

        file_link = item.get("FILE_LINK", "")
        # 加 https:// 前缀 (HKEX 返的是相对路径)
        full_link = f"{HKEX_BASE_URL}{file_link}" if file_link.startswith("/") else file_link

        # 解码 HTML entities (HKEX 用 &#x2f; 这种)
        title_text = _html.unescape(item.get("TITLE", "").strip())
        long_text_raw = (item.get("LONG_TEXT") or "").replace("<br/>", " ").strip()
        long_text = _html.unescape(long_text_raw)
        short_text_raw = (item.get("SHORT_TEXT") or "").replace("<br/>", " ").strip()
        short_text = _html.unescape(short_text_raw)

        # title 用 LONG_TEXT (类型) + TITLE 拼接, 信息量更大
        title = f"{title_text} - {long_text}" if long_text and long_text != title_text else title_text

        return {
            "source": "hkex",
            "title": title,
            "summary": short_text,
            "url": full_link,
            "link": full_link,
            "published": published_iso,
            "news_id": item.get("NEWS_ID"),
            "stock_code": (item.get("STOCK_CODE") or "").split("<br/>")[0],  # "09988<br/>89988" 取前
            "stock_name": (item.get("STOCK_NAME") or "").split("<br/>")[0],
            "long_text": long_text,
            "file_info": item.get("FILE_INFO"),  # "88KB"
        }

    @staticmethod
    def _parse_hkex_datetime(dt_str: str) -> str:
        """
        "03/07/2026 18:51" (HKEX DD/MM/YYYY HH:MM) → "2026-07-03T18:51:00+08:00"
        注意: DATE_TIME 字段是 DD/MM/YYYY 格式 (历史原因, 英国日期), fromDate/toDate 参数是 YYYYMMDD
        """
        if not dt_str:
            return ""
        try:
            # HKEX 时区是 HKT (UTC+8)
            dt = datetime.strptime(dt_str, "%d/%m/%Y %H:%M")
            dt = dt.replace(tzinfo=TZ)
            return dt.isoformat()
        except ValueError:
            return dt_str

    # ==================== BaseCollector 接口 ====================

    async def collect(self, symbol: str, data_type: str, **kwargs) -> CollectResult:
        """
        异步采集入口
        kwargs:
          - days: int, 查询最近 N 天 (默认 90)
          - max_items: int, 最多返回多少条 (默认 100)
        """
        start = _now_tz()

        if data_type != "hkex_announcement":
            return CollectResult(
                success=False, data=None, source="hkex",
                symbol=symbol, data_type=data_type,
                error=f"hkex_collector 不支持 data_type={data_type}",
                duration_seconds=(_now_tz() - start).total_seconds(),
            )

        days = int(kwargs.get("days", 90))
        max_items = int(kwargs.get("max_items", 100))

        # 1. 查 stockId
        stock_id = self._get_stock_id(symbol)
        if stock_id is None:
            return CollectResult(
                success=False, data=None, source="hkex",
                symbol=symbol, data_type=data_type,
                error=f"无法获取 HKEX stockId (symbol={symbol})",
                duration_seconds=(_now_tz() - start).total_seconds(),
            )

        # 2. 算日期范围 (YYYYMMDD 格式 — HKEX 实际要求, 2026-07-06 实测验证)
        to_dt = _now_tz()
        from_dt = to_dt - timedelta(days=days)
        from_date = from_dt.strftime("%Y%m%d")
        to_date = to_dt.strftime("%Y%m%d")

        # 3. 查询 (run_in_executor 避免阻塞 event loop)
        #
        # ⚠️ 2026-10-26：加**应用层重试**（1 次），并把单次 timeout 从 60s 降到 30s。
        #
        # 【为什么 hkex 不能像其它源那样挂 urllib3 Retry(read=N)】
        #   本 collector 的 timeout 是 45~60s（港交所 SSL 慢，chain.py 的 SLOW_SOURCES
        #   也给它 60s）。若挂 urllib3 的 read 重试，总耗时 = timeout x (1+重试次数)：
        #       read=2 → 60 x 3 = 180s
        #       read=3 → 60 x 4 = 240s
        #   而 8084 的 scheduler tick 只有 **60s** —— 单次采集超过 60s 会**拖垮调度**
        #   （这正是 2026-07-29 那次 8084 死锁的同类问题）。
        #   → **timeout 大的源不能用"多层重试"，只能用"单层 + 降 timeout"**。
        #
        # 【为什么仍值得加这一层重试】
        #   hkex_announcement 的三条订阅（id 47/48/45）是 **1440 分钟（每天仅 1 次）**
        #   —— 全项目**空闲最久**的采集，最容易命中"连接池里的连接被服务端静默关闭"。
        #   实测也确实出现过 1 次 RemoteDisconnected（"HKEX 请求失败: Connection aborted"）。
        #   单次重试（最多 30s + 30s = 60s，正好卡在 tick 边界内）足以覆盖瞬时断连。
        loop = asyncio.get_event_loop()
        items = None
        last_err = ""
        for attempt in range(2):          # 最多 2 次尝试（1 次重试）
            try:
                items = await loop.run_in_executor(
                    None,
                    lambda: self._fetch_announcements_sync(
                        stock_id, self._strip_hk_prefix(symbol),
                        from_date=from_date, to_date=to_date,
                        row_range=min(max_items, 100), lang="EN",
                    )
                )
                last_err = ""
                break
            except Exception as e:
                last_err = str(e)
                logger.warning(
                    f"_fetch_announcements_sync({symbol}) 异常 "
                    f"(attempt {attempt+1}/2): {e}"
                )
                if attempt == 0:
                    await asyncio.sleep(1)   # 退避 1s 后重试
        if items is None:
            return CollectResult(
                success=False, data=None, source="hkex",
                symbol=symbol, data_type=data_type,
                error=f"HKEX 请求失败: {last_err}",
                duration_seconds=(_now_tz() - start).total_seconds(),
            )

        if items is None:
            return CollectResult(
                success=False, data=None, source="hkex",
                symbol=symbol, data_type=data_type,
                error="HKEX 返 None",
                duration_seconds=(_now_tz() - start).total_seconds(),
            )

        # 4. 解析 + 标准化
        parsed = [self._parse_hkex_item(it, symbol) for it in items[:max_items]]
        logger.info(f"hkex_collector({symbol} stockId={stock_id}) 拉取 {len(parsed)} 条公告")

        return CollectResult(
            success=True, data=parsed, source="hkex",
            symbol=symbol, data_type=data_type,
            duration_seconds=(_now_tz() - start).total_seconds(),
        )

    async def collect_batch(self, symbols: List[str], data_type: str, **kwargs) -> List[CollectResult]:
        """批量采集 — 串行 (HKEX 单次慢, 并发会触发 rate limit)"""
        results = []
        for sym in symbols:
            res = await self.collect(sym, data_type, **kwargs)
            results.append(res)
            # 串行防 rate limit: 每次间隔 1s
            await asyncio.sleep(1)
        return results