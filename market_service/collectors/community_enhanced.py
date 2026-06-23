#!/usr/bin/env python3
"""
增强型社区/新闻采集器
- 东方财富搜索API：个股新闻（支持港股/A股/美股）
- 雪球热帖：via akshare stock_hot_tweet_xq / stock_hot_follow_xq
- 东方财富股吧：via akshare stock_comment_em（A股评级）
"""

import sys
import os
import json
import asyncio
import re
import requests
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone, timedelta
import logging

TZ = timezone(timedelta(hours=8))

def now_tz():
    return datetime.now(TZ)


from .base import BaseCollector, CollectResult

logger = logging.getLogger(__name__)


class CommunityEnhancedCollector(BaseCollector):
    """增强型社区/新闻采集器"""

    def __init__(self, rate_limit_per_minute: int = 30):
        BaseCollector.__init__(self, rate_limit_per_minute)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9",
        })
        self.logger = logging.getLogger(__name__)

    # ========== 东方财富搜索API ==========
    
    def _fetch_em_stock_news(self, symbol: str, page_size: int = 10) -> Optional[List[Dict[str, Any]]]:
        """
        通过东方财富搜索API获取个股新闻
        支持：A股代码(600519)、港股代码(03690)
        """
        try:
            # 港股代码去掉 hk 前缀
            code = symbol.upper().replace("HK", "")
            
            url = "https://search-api-web.eastmoney.com/search/jsonp"
            param_obj = {
                "uid": "",
                "keyword": code,
                "type": ["cmsArticleWebOld"],
                "client": "web",
                "clientType": "web",
                "clientVersion": "curr",
                "param": {
                    "cmsArticleWebOld": {
                        "searchScope": "default",
                        "sort": "default",
                        "pageIndex": 1,
                        "pageSize": page_size,
                        "preTag": "",
                        "postTag": ""
                    }
                }
            }
            params = {
                "cb": "jQuery",
                "param": json.dumps(param_obj, ensure_ascii=False)
            }
            
            resp = self.session.get(
                url, params=params,
                headers={"Referer": "https://so.eastmoney.com/"},
                timeout=10
            )
            if resp.status_code != 200:
                self.logger.warning(f"_fetch_em_stock_news({symbol}) HTTP {resp.status_code}")
                return None
            
            # 解析 JSONP
            m = re.search(r'jQuery\((.*)\)', resp.text, re.DOTALL)
            if not m:
                self.logger.warning(f"_fetch_em_stock_news({symbol}) JSONP解析失败")
                return None
            
            data = json.loads(m.group(1))
            articles = data.get("result", {}).get("cmsArticleWebOld", [])
            
            if not articles:
                self.logger.info(f"_fetch_em_stock_news({symbol}) 无新闻结果")
                return []
            
            news_list = []
            for item in articles:
                # 清理 HTML 标签
                title = re.sub(r'<[^>]+>', '', item.get("title", ""))
                content = re.sub(r'<[^>]+>', '', item.get("content", ""))
                news_list.append({
                    "title": title,
                    "summary": content[:200],
                    "url": item.get("url", ""),
                    "source": item.get("mediaName", ""),
                    "published": item.get("date", ""),
                    "em_code": item.get("code", ""),
                })
            
            self.logger.info(f"_fetch_em_stock_news({symbol}) 获取 {len(news_list)} 条新闻")
            return news_list
            
        except Exception as e:
            self.logger.warning(f"_fetch_em_stock_news({symbol}) 异常: {e}")
            return None

    # ========== 雪球热帖 ==========

    def _fetch_xueqiu_hot_tweets(self) -> Optional[List[Dict[str, Any]]]:
        """通过 akshare 获取雪球热帖（不需要 token）"""
        try:
            import akshare as ak
            df = ak.stock_hot_tweet_xq()
            if df.empty:
                return []
            
            tweets = []
            for _, row in df.iterrows():
                tweets.append({
                    "symbol": str(row.get("股票代码", "")),
                    "name": str(row.get("股票简称", "")),
                    "follow_count": float(row.get("关注", 0)),
                    "price": float(row.get("最新价", 0)),
                    "source": "xueqiu_hot_tweet",
                })
            
            self.logger.info(f"_fetch_xueqiu_hot_tweets 获取 {len(tweets)} 条热帖")
            return tweets
            
        except Exception as e:
            self.logger.warning(f"_fetch_xueqiu_hot_tweets 异常: {e}")
            return None

    def _fetch_xueqiu_hot_follow(self) -> Optional[List[Dict[str, Any]]]:
        """通过 akshare 获取雪球热关注"""
        try:
            import akshare as ak
            df = ak.stock_hot_follow_xq()
            if df.empty:
                return []
            
            items = []
            for _, row in df.iterrows():
                items.append({
                    "symbol": str(row.get("股票代码", "")),
                    "name": str(row.get("股票简称", "")),
                    "follow_count": float(row.get("关注", 0)),
                    "price": float(row.get("最新价", 0)),
                    "source": "xueqiu_hot_follow",
                })
            
            self.logger.info(f"_fetch_xueqiu_hot_follow 获取 {len(items)} 条热关注")
            return items
            
        except Exception as e:
            self.logger.warning(f"_fetch_xueqiu_hot_follow 异常: {e}")
            return None

    def _fetch_xueqiu_hot_deal(self) -> Optional[List[Dict[str, Any]]]:
        """通过 akshare 获取雪球热交易"""
        try:
            import akshare as ak
            df = ak.stock_hot_deal_xq()
            if df.empty:
                return []

            items = []
            for _, row in df.iterrows():
                items.append({
                    "symbol": str(row.get("股票代码", "")),
                    "name": str(row.get("股票简称", "")),
                    "follow_count": float(row.get("关注", 0)),
                    "price": float(row.get("最新价", 0)),
                    "source": "xueqiu_hot_deal",
                })

            self.logger.info(f"_fetch_xueqiu_hot_deal 获取 {len(items)} 条热交易")
            return items

        except Exception as e:
            self.logger.warning(f"_fetch_xueqiu_hot_deal 异常: {e}")
            return None

    # ========== 主力资金榜 / 板块资金流 (mcp-eastmoney 同源 API) ==========

    def _get_with_backoff(self, url: str, headers=None, timeout=10, max_retries=3):
        """GET 请求 + 指数退避重试 (针对 EM push2 IP 封禁)"""
        import time as _time
        last_err = None
        for attempt in range(max_retries):
            try:
                resp = self.session.get(url, headers=headers or {}, timeout=timeout)
                if resp.status_code == 200:
                    return resp
                last_err = f"HTTP {resp.status_code}"
            except Exception as e:
                last_err = str(e)
            wait = min(2 ** attempt, 8)
            self.logger.warning(f"GET 失败 (attempt {attempt+1}/{max_retries}), {wait}s 后重试: {last_err}")
            _time.sleep(wait)
        raise Exception(f"重试 {max_retries} 次仍失败: {last_err}")

    def _fetch_main_fund_rank(self, top_n: int = 20) -> Optional[List[Dict[str, Any]]]:
        """
        主力资金净流入 Top N (东方财富 push2delay 接口, mcp-eastmoney 同源)
        fields: f12=code, f14=name, f2=price, f3=change_pct×100,
                f62=main_net_inflow(元), f184=main_net_pct×100,
                f66=super_large, f70=large, f76=medium, f78=small
        """
        try:
            import time as _time
            fs = "m:0+t:6,m:0+t:13,m:0+t:80,m:1+t:2,m:1+t:23,m:1+t:8"
            ts = int(_time.time() * 1000)
            # 用 push2delay (mcp-eastmoney 同款), push2 经常被封
            url = (
                f"https://push2delay.eastmoney.com/api/qt/clist/get"
                f"?pn=1&pz={top_n}&po=1&np=1&fltt=2&invt=2"
                f"&fid=f62&fs={fs}"
                f"&fields=f12,f14,f2,f3,f62,f184,f66,f70,f76,f78"
                f"&req_trace={ts}&_={ts}"
            )
            resp = self._get_with_backoff(url, headers={"Referer": "https://quote.eastmoney.com/"})
            data = resp.json()
            diff = (data.get("data") or {}).get("diff") or []
            items = []
            for i, r in enumerate(diff, start=1):
                # f2=价格(元), f3=涨跌幅×100(%), f62=主力净流入(元), f184=主力净流入占比×100(%)
                # f66/f70/f76/f78=超大/大/中/小单净额(元)
                price_raw = r.get("f2")
                price = float(price_raw) if isinstance(price_raw, (int, float)) else None
                items.append({
                    "rank_no": i,
                    "symbol": r.get("f12"),
                    "name": r.get("f14"),
                    "price": price,
                    "change_pct": (r.get("f3") or 0) / 100 if isinstance(r.get("f3"), (int, float)) else None,
                    "main_net_inflow": r.get("f62"),
                    "main_net_pct": (r.get("f184") or 0) / 100 if isinstance(r.get("f184"), (int, float)) else None,
                    "super_large_net": r.get("f66"),
                    "large_net": r.get("f70"),
                    "medium_net": r.get("f76"),
                    "small_net": r.get("f78"),
                })
            self.logger.info(f"_fetch_main_fund_rank 获取 {len(items)} 条")
            return items
        except Exception as e:
            self.logger.warning(f"_fetch_main_fund_rank 异常: {e}")
            return None

    def _fetch_sector_fund_flow(self, kind: str = "industry", top_n: int = 15) -> Optional[List[Dict[str, Any]]]:
        """
        板块资金流 (industry/concept)
        fields: f12=code, f14=name, f3=change_pct×100,
                f62=main_net_inflow(元), f184=main_net_pct×100,
                f128=leading_stock, f136=leading_change_pct×100
        """
        try:
            import time as _time
            fs = "m:90+t:2" if kind == "industry" else "m:90+t:3"
            ts = int(_time.time() * 1000)
            url = (
                f"https://push2delay.eastmoney.com/api/qt/clist/get"
                f"?pn=1&pz={top_n}&po=1&np=1&fltt=2&invt=2"
                f"&fid=f62&fs={fs}"
                f"&fields=f12,f14,f2,f3,f62,f184,f128,f136"
                f"&req_trace={ts}&_={ts}"
            )
            resp = self._get_with_backoff(url, headers={"Referer": "https://quote.eastmoney.com/"})
            data = resp.json()
            diff = (data.get("data") or {}).get("diff") or []
            items = []
            for i, r in enumerate(diff, start=1):
                items.append({
                    "rank_no": i,
                    "sector_kind": kind,
                    "symbol": r.get("f12"),
                    "name": r.get("f14"),
                    "change_pct": (r.get("f3") or 0) / 100 if isinstance(r.get("f3"), (int, float)) else None,
                    "main_net_inflow": r.get("f62"),
                    "main_net_pct": (r.get("f184") or 0) / 100 if isinstance(r.get("f184"), (int, float)) else None,
                    "leading_stock": r.get("f128") or "-",
                    "leading_change_pct": (r.get("f136") or 0) / 100 if isinstance(r.get("f136"), (int, float)) else None,
                })
            self.logger.info(f"_fetch_sector_fund_flow({kind}) 获取 {len(items)} 条")
            return items
        except Exception as e:
            self.logger.warning(f"_fetch_sector_fund_flow({kind}) 异常: {e}")
            return None

    # ========== collect 接口 ==========

    async def collect(self, symbol: str, data_type: str, **kwargs) -> CollectResult:
        """
        采集入口
        data_type:
          - "stock_news": 个股新闻（东方财富搜索API）
          - "xueqiu_hot": 雪球热帖+热关注+热交易
          - "main_fund_rank": 主力资金净流入 Top 20
          - "sector_fund_flow": 板块资金流 (industry/concept)
        """
        start = now_tz()
        from datetime import datetime as _dt
        rank_date = _dt.now(TZ).date().isoformat()
        
        if data_type == "stock_news":
            news_list = self._fetch_em_stock_news(symbol)
            if news_list is None:
                return CollectResult(
                    success=False, data=None, source="em_search", symbol=symbol,
                    data_type=data_type, error="东方财富搜索API请求失败",
                    duration_seconds=(now_tz() - start).total_seconds()
                )
            return CollectResult(
                success=True, data=news_list, source="em_search",
                symbol=symbol, data_type=data_type,
                duration_seconds=(now_tz() - start).total_seconds()
            )

        elif data_type == "xueqiu_hot":
            all_items = []

            tweets = self._fetch_xueqiu_hot_tweets()
            if tweets:
                all_items.extend(tweets)

            # 雪球热关注/热交易太慢（每类~20s），暂不采集
            # follows = self._fetch_xueqiu_hot_follow()
            # deals = self._fetch_xueqiu_hot_deal()

            if not all_items:
                return CollectResult(
                    success=False, data=None, source="xueqiu", symbol="__all__",
                    data_type=data_type, error="雪球数据全部获取失败",
                    duration_seconds=(now_tz() - start).total_seconds()
                )

            return CollectResult(
                success=True, data=all_items, source="xueqiu",
                symbol="__all__", data_type=data_type,
                duration_seconds=(now_tz() - start).total_seconds()
            )

        elif data_type == "main_fund_rank":
            top_n = kwargs.get("top_n", 20)
            items = self._fetch_main_fund_rank(top_n=top_n)
            if items is None:
                return CollectResult(
                    success=False, data=None, source="eastmoney_em",
                    symbol="__main_fund__", data_type=data_type,
                    error="主力资金榜请求失败", duration_seconds=(now_tz() - start).total_seconds()
                )
            # 给每条加 flow_type 和 rank_date
            for it in items:
                it["flow_type"] = "main_fund_rank"
                it["rank_date"] = rank_date
                it["source"] = "eastmoney_em"
                # raw 备份去掉自己引用, 避免 json.dumps 报 Circular reference
                raw_copy = {k: v for k, v in it.items() if k != "raw"}
                it["raw"] = raw_copy
            return CollectResult(
                success=True, data=items, source="eastmoney_em",
                symbol="__main_fund__", data_type=data_type,
                duration_seconds=(now_tz() - start).total_seconds()
            )

        elif data_type == "sector_fund_flow":
            kind = kwargs.get("kind", "industry")
            top_n = kwargs.get("top_n", 15)
            items = self._fetch_sector_fund_flow(kind=kind, top_n=top_n)
            if items is None:
                return CollectResult(
                    success=False, data=None, source="eastmoney_em",
                    symbol=f"__sector_{kind}__", data_type=data_type,
                    error=f"板块资金流({kind})请求失败", duration_seconds=(now_tz() - start).total_seconds()
                )
            for it in items:
                it["flow_type"] = "sector_fund_flow"
                it["rank_date"] = rank_date
                it["source"] = "eastmoney_em"
                raw_copy = {k: v for k, v in it.items() if k != "raw"}
                it["raw"] = raw_copy
            return CollectResult(
                success=True, data=items, source="eastmoney_em",
                symbol=f"__sector_{kind}__", data_type=data_type,
                duration_seconds=(now_tz() - start).total_seconds()
            )

        return CollectResult(
            success=False, data=None, source="community_enhanced", symbol=symbol,
            data_type=data_type, error=f"不支持的 data_type: {data_type}",
            duration_seconds=(now_tz() - start).total_seconds()
        )

    async def collect_batch(self, symbols: List[str], data_type: str, **kwargs) -> List[CollectResult]:
        """
        批量采集
        """
        results = []
        for symbol in symbols:
            result = await self.collect(symbol, data_type, **kwargs)
            results.append(result)
            await asyncio.sleep(0.5)  # 限频
        return results
