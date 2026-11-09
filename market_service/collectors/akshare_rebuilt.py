#!/usr/bin/env python3
"""
AkShare 采集器
支持：基金净值、股票行情、财经新闻、基本面数据
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
import akshare as ak
import requests

from .base import BaseCollector, CollectResult

logger = logging.getLogger(__name__)

# 时区：东8区
TZ = timezone(timedelta(hours=8))

def now_tz():
    return datetime.now(TZ)

class AKShareFetcher(BaseCollector):
    def __init__(self, rate_limit_per_minute: int = 30):
        BaseCollector.__init__(self, rate_limit_per_minute)
        self.logger = logging.getLogger(__name__)

    # ========== PUBLIC API ==========
    async def collect(self, symbol: str, data_type: str, **kwargs) -> CollectResult:
        start = now_tz()
        try:
            if data_type == "nav":
                raw = self._fetch_fund_nav_sync(symbol)
                if raw is None:
                    return CollectResult(False, None, "akshare", symbol, "nav", "基金净值数据不可用", (now_tz()-start).total_seconds())
                normalized = self._normalize_nav(raw, symbol)
                return CollectResult(True, normalized, "akshare", symbol, "nav", duration_seconds=(now_tz()-start).total_seconds())

            elif data_type == "news":
                raw_list = self._fetch_stock_news_sync(symbol)
                if raw_list is None:
                    return CollectResult(False, None, "akshare", symbol, "news", "新闻数据不可用", (now_tz()-start).total_seconds())
                normalized_list = [self._normalize_news(item, symbol) for item in raw_list]
                return CollectResult(True, normalized_list, "akshare", symbol, "news", duration_seconds=(now_tz()-start).total_seconds())

            elif data_type == "price":
                backup_sources = kwargs.get('backup_sources')
                raw = await self._fetch_stock_quote(symbol, backup_sources)
                if raw is None:
                    return CollectResult(False, None, "akshare", symbol, "price", "实时行情数据不可用（所有源失败）", (now_tz()-start).total_seconds())
                normalized = self._normalize_price(raw, symbol)
                return CollectResult(True, normalized, raw.get('source', 'akshare'), symbol, "price", duration_seconds=(now_tz()-start).total_seconds())

            elif data_type == "fundamental":
                raw = self.fetch_fundamental(symbol)
                if raw is None:
                    return CollectResult(False, None, "akshare", symbol, "fundamental", "基本面数据不可用", (now_tz()-start).total_seconds())
                normalized = self.normalize_fundamental(raw)
                return CollectResult(True, normalized, raw.get('source', 'akshare'), symbol, "fundamental", duration_seconds=(now_tz()-start).total_seconds())

            elif data_type == "estimate_nav":
                # 盘中估算净值（东方财富 fundgz 接口）
                raw = self._fetch_fund_estimate_sync(symbol)
                if raw is None:
                    return CollectResult(False, None, "akshare", symbol, "estimate_nav", "盘中估算净值不可用（非交易日或接口失败）", (now_tz()-start).total_seconds())
                normalized = self._normalize_fund_estimate(raw, symbol)
                return CollectResult(True, normalized, "fundgz", symbol, "estimate_nav", duration_seconds=(now_tz()-start).total_seconds())

            else:
                return CollectResult(False, None, "akshare", symbol, data_type, f"未知数据类型: {data_type}", (now_tz()-start).total_seconds())
        except Exception as e:
            self.logger.error(f"AKShare采集异常 {symbol}/{data_type}: {e}")
            return CollectResult(False, None, "akshare", symbol, data_type, str(e), (now_tz()-start).total_seconds())

    async def _store_result(self, result: CollectResult, db) -> bool:
        try:
            if result.data_type == "price":
                return db.insert_price(result.data)
            elif result.data_type == "nav":
                return db.insert_nav(result.data)
            elif result.data_type == "news":
                return db.insert_news(result.data)
            elif result.data_type == "fundamental":
                return db.save_fundamental(result.data)
            return False
        except Exception as e:
            self.logger.error(f"入库失败 {result.data_type}: {e}")
            return False

    # ========== 股票行情 ==========
    async def _fetch_stock_quote(self, code: str, backup_sources: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._fetch_stock_quote_sync, code, backup_sources)

    def _fetch_stock_quote_sync(self, code: str, backup_sources: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        now = now_tz()
        # 主源: akshare (可能失败)
        try:
            df = ak.stock_zh_a_spot_em()
            row = df[df['代码'] == code]
            if not row.empty:
                row = row.iloc[0]
                self.logger.info(f"_fetch_stock_quote_sync({code}) 实时行情获取成功")
                return {
                    'source': 'akshare_spot',
                    'price': float(row['最新价']),
                    'volume': float(row['成交量']),
                    'amount': float(row['成交额']),
                    'high': float(row['最高']),
                    'low': float(row['最低']),
                    'open': float(row['今开']),
                    'prev_close': float(row['昨收']),
                    'timestamp': now.isoformat()
                }
        except Exception as e:
            self.logger.warning(f"_fetch_stock_quote_sync({code})主数据源异常: {e}")
        # 降级1: tencent
        if not backup_sources or 'tencent' in backup_sources:
            result = self._fetch_stock_quote_tencent(code)
            if result:
                return result
        # 降级2: sina
        if not backup_sources or 'sina' in backup_sources:
            result = self._fetch_stock_quote_sina(code)
            if result:
                return result
        return None

    def _fetch_stock_quote_tencent(self, code: str) -> Optional[Dict[str, Any]]:
        """腾讯财经实时行情（含部分基本面字段）"""
        url = f"https://qt.gtimg.cn/q={code}"
        try:
            raw = self._get_with_backoff(url, headers={"Referer": "https://gu.qq.com/"})
        except Exception as e:
            self.logger.warning(f"_fetch_stock_quote_tencent({code}) 失败: {e}")
            return None
        if "~" not in raw:
            return None
        parts = raw.split("~")
        if len(parts) < 14:
            return None
        try:
            price = float(parts[3]) if parts[3] else 0.0
            prev_close = float(parts[4]) if parts[4] else 0.0
            open_ = float(parts[5]) if parts[5] else 0.0
            volume = float(parts[6]) if parts[6] else 0.0
            amount = float(parts[7]) if parts[7] else 0.0
            high = float(parts[9]) if parts[9] else 0.0
            low = float(parts[8]) if parts[8] else 0.0
            trade_time = now_tz().isoformat()
            self.logger.info(f"_fetch_stock_quote_tencent({code}) 成功: price={price}")
            return {
                'source': 'tencent',
                'price': price,
                'volume': volume,
                'amount': amount,
                'open': open_,
                'high': high,
                'low': low,
                'prev_close': prev_close,
                'trade_time': trade_time,
            }
        except Exception as e:
            self.logger.warning(f"_fetch_stock_quote_tencent({code}) 字段解析失败: {e}")
            return None

    def _fetch_stock_quote_sina(self, code: str) -> Optional[Dict[str, Any]]:
        """新浪财经实时行情"""
        try:
            import requests
            url = f'https://hq.sinajs.cn/list=sh{code}' if code.startswith('6') else f'https://hq.sinajs.cn/list=sz{code}'
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': 'https://finance.sina.com.cn'
            }
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code != 200:
                self.logger.warning(f"_fetch_stock_quote_sina({code}) HTTP {resp.status_code}")
                return None
            text = resp.text.strip()
            if '=' not in text:
                return None
            data_str = text.split('=', 1)[1].strip('"')
            fields = data_str.split(',')
            if len(fields) < 10:
                self.logger.warning(f"_fetch_stock_quote_sina({code}) 字段不足: {len(fields)}")
                return None
            price = float(fields[3]) if fields[3] else 0.0
            prev_close = float(fields[2]) if fields[2] else 0.0
            open_ = float(fields[1]) if fields[1] else 0.0
            volume = float(fields[8]) if len(fields) > 8 and fields[8] else 0.0
            amount = float(fields[9]) if len(fields) > 9 and fields[9] else 0.0
            high = float(fields[4]) if fields[4] else 0.0
            low = float(fields[5]) if fields[5] else 0.0
            trade_time = now_tz().isoformat()
            self.logger.info(f"_fetch_stock_quote_sina({code}) 成功: price={price}")
            return {
                'source': 'sina',
                'price': price,
                'volume': volume,
                'amount': amount,
                'open': open_,
                'high': high,
                'low': low,
                'prev_close': prev_close,
                'trade_time': trade_time
            }
        except Exception as e:
            self.logger.error(f"_fetch_stock_quote_sina({code})异常: {e}")
            return None

    def _normalize_price(self, raw: Dict[str, Any], symbol: str) -> Dict[str, Any]:
        trade_time_val = raw.get('timestamp') or raw.get('trade_time')
        if trade_time_val:
            if isinstance(trade_time_val, datetime):
                if trade_time_val.tzinfo is None:
                    trade_time_val = trade_time_val.replace(tzinfo=TZ)
                trade_time_val = trade_time_val.isoformat()
        else:
            trade_time_val = now_tz().isoformat()
        return {
            'source': raw.get('source', 'akshare'),
            'symbol': symbol,
            'data_type': 'price',
            'trade_time': trade_time_val,
            'price': raw.get('price'),
            'volume': raw.get('volume'),
            'amount': raw.get('amount'),
            'open': raw.get('open'),
            'high': raw.get('high'),
            'low': raw.get('low'),
            'prev_close': raw.get('prev_close'),
            'raw': raw
        }

    # ========== 基本面数据（新功能）==========
    def fetch_fundamental(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取基本面数据（统一入口）"""
        return self._fetch_stock_fundamental_tencent(symbol)

    def normalize_fundamental(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """标准化基本面数据"""
        if not raw:
            return None
        symbol = raw.get('symbol', '')
        price = raw.get('price')
        pe_ttm = raw.get('pe_ttm')
        pb = raw.get('pb')
        total_mv = raw.get('total_mv')
        circ_mv = raw.get('circ_mv')
        industry = raw.get('industry')
        date = raw.get('date', now_tz().strftime('%Y-%m-%d'))
        return {
            'symbol': symbol,
            'date': date,
            'price': price,
            'pe_ttm': pe_ttm,
            'pb': pb,
            'total_mv': total_mv,
            'circ_mv': circ_mv,
            'industry': industry,
            'raw': raw.get('raw', '')
        }

    def _fetch_stock_fundamental_tencent(self, code: str) -> Optional[Dict[str, Any]]:
        """从腾讯获取基本面数据（PE/PB/市值/行业）"""
        url = f"https://qt.gtimg.cn/q={code}"
        try:
            raw = self._get_with_backoff(url, headers={"Referer": "https://gu.qq.com/"})
        except Exception as e:
            self.logger.warning(f"_fetch_stock_fundamental_tencent({code}) 失败: {e}")
            return None
        if "~" not in raw or len(raw.split("~")) < 54:
            return None
        parts = raw.split("~")
        try:
            # 提取基本面字段（索引基于测试）
            pe_ttm = float(parts[46]) if len(parts) > 46 and parts[46] else None
            pb = float(parts[47]) if len(parts) > 47 and parts[47] else None
            total_mv = float(parts[50]) if len(parts) > 50 and parts[50] else None  # 亿元
            circ_mv = float(parts[51]) if len(parts) > 51 and parts[51] else None
            industry = parts[53] if len(parts) > 53 and parts[53] else None
            price = float(parts[3]) if parts[3] else None
        except Exception as e:
            self.logger.warning(f"解析基本面失败: {e}")
            return None
        return {
            'source': 'tencent',
            'symbol': code[2:] if code.startswith(('sh','sz')) else code,  # 提取真实代码
            'price': price,
            'pe_ttm': pe_ttm,
            'pb': pb,
            'total_mv': total_mv,
            'circ_mv': circ_mv,
            'industry': industry,
            'raw': raw[:1000],
            'date': now_tz().strftime('%Y-%m-%d')
        }

    # ========== 基金净值 ==========
    def _fetch_fund_nav_sync(self, symbol: str) -> Optional[Dict[str, Any]]:
        try:
            df = ak.fund_open_fund_info_em(symbol=symbol)
            if df.empty:
                return None
            latest = df.iloc[-1]
            return {
                'nav_date': str(latest.get('净值日期', '')),
                'nav': float(latest.get('单位净值', 0)),
                'acc_nav': float(latest.get('累计净值', 0)),
                'change_pct': str(latest.get('日增长率', ''))
            }
        except Exception as e:
            self.logger.warning(f"_fetch_fund_nav_sync({symbol})异常: {e}")
            return None

    # ========== 盘中估算净值（东方财富 fundgz） ==========
    ESTIMATE_URL = 'http://fundgz.1234567.com.cn/js/{code}.js'

    def _fetch_fund_estimate_sync(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取盘中估算净值（东方财富 fundgz 接口）

        返回格式：jsonpgz({"fundcode":"008114","name":"天弘中证红利...",
                             "jzrq":"2026-04-09","dwjz":"1.7334","gsz":"1.7327",
                             "gszzl":"-0.04","gztime":"2026-04-10 15:00"});

        注意：估值15:00后停止更新，gztime会停留在15:00（当日收盘）
        """
        try:
            import re
            url = self.ESTIMATE_URL.format(code=symbol)
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            text = resp.text.strip()

            # 解析 JSONP：jsonpgz({...});
            match = re.search(r'jsonpgz\((.*)\);?', text)
            if not match:
                self.logger.warning(f"_fetch_fund_estimate({symbol}) 无法解析JSONP，响应: {text[:100]}")
                return None

            import json
            data = json.loads(match.group(1))

            return {
                'fund_code': data.get('fundcode', symbol),
                'fund_name': data.get('name', ''),
                'nav_date': data.get('jzrq', ''),        # 昨日净值日期
                'nav': float(data.get('dwjz', 0)),       # 昨日单位净值
                'estimate_nav': float(data.get('gsz', 0)),  # 估算净值
                'estimate_growth': float(data.get('gszzl', 0)),  # 估算增长率%
                'estimate_time': data.get('gztime', ''), # 估算时间
            }
        except Exception as e:
            self.logger.warning(f"_fetch_fund_estimate({symbol}) 异常: {e}")
            return None

    def _normalize_fund_estimate(self, raw: Dict[str, Any], symbol: str) -> Dict[str, Any]:
        """标准化盘中估算净值"""
        return {
            'source': 'fundgz',
            'symbol': symbol,
            'data_type': 'fund_estimate',
            'fund_name': raw.get('fund_name', ''),
            'nav_date': raw.get('nav_date', ''),          # 昨日净值日期
            'nav': raw.get('nav'),                        # 昨日净值
            'estimate_nav': raw.get('estimate_nav'),       # 估算净值
            'estimate_growth': raw.get('estimate_growth'), # 估算增长率%
            'estimate_time': raw.get('estimate_time', ''), # 估值时间
            'raw': raw
        }

    def _normalize_nav(self, raw: Dict[str, Any], symbol: str) -> Dict[str, Any]:
        nav_date_str = raw.get('nav_date', '')
        try:
            if nav_date_str:
                if isinstance(nav_date_str, str) and len(nav_date_str) == 8:
                    nav_date = f"{nav_date_str[:4]}-{nav_date_str[4:6]}-{nav_date_str[6:8]}"
                else:
                    nav_date = str(nav_date_str)[:10]
            else:
                nav_date = now_tz().strftime('%Y-%m-%d')
        except Exception as e:
            logger.warning(f"_normalize_nav日期解析失败: {e}, 使用今天")
            nav_date = now_tz().strftime('%Y-%m-%d')
        return {
            'source': 'akshare',
            'symbol': symbol,
            'data_type': 'nav',
            'nav_date': nav_date,
            'nav': raw.get('nav'),
            'acc_nav': raw.get('acc_nav'),
            'change_pct': raw.get('change_pct'),
            'raw': raw
        }

    # ========== 财经新闻 ==========
    def _fetch_stock_news_sync(self, symbol: str) -> Optional[List[Dict[str, Any]]]:
        try:
            df = ak.stock_news_em()
            if df.empty:
                return []
            news_list = []
            for _, row in df.head(20).iterrows():
                news_list.append({
                    'title': row.get('标题', ''),
                    'content': row.get('内容', ''),
                    'link': row.get('链接', ''),
                    'source': 'akshare_news_em'
                })
            return news_list
        except Exception as e:
            self.logger.warning(f"_fetch_stock_news_sync异常: {e}")
            return None

    def _normalize_news(self, raw: Dict[str, Any], symbol: str) -> Dict[str, Any]:
        from datetime import datetime
        return {
            'source': raw.get('source', 'akshare'),
            'symbol': symbol,
            'data_type': 'news',
            'title': raw.get('title', '')[:200],
            'summary': raw.get('content', '')[:500],
            'link': raw.get('link', ''),
            'published': now_tz().isoformat()
        }
