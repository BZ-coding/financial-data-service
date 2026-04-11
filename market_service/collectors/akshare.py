#!/usr/bin/env python3
"""
AKShare数据采集器
支持：基金净值、实时行情（多源降级）、财经新闻
"""

import sys
import os
import json
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone, timedelta
import logging
from pathlib import Path
from dataclasses import dataclass
import akshare as ak

# 时区
TZ = timezone(timedelta(hours=8))

def now_tz():
    return datetime.now(TZ)

@dataclass
class CollectResult:
    success: bool
    data: Optional[Any]
    source: str
    symbol: Optional[str]
    data_type: str
    error: Optional[str] = None
    duration_seconds: float = 0.0

from .base import BaseCollector

class AKShareFetcher(BaseCollector):
    def __init__(self, rate_limit_per_minute: int = 30):
        BaseCollector.__init__(self, rate_limit_per_minute)
        self.logger = logging.getLogger(__name__)
    

    def _get_with_backoff(self, url: str, headers: Optional[Dict] = None, timeout: int = 10) -> str:
        """HTTP GET with simple retry"""
        import requests
        for attempt in range(3):
            try:
                resp = requests.get(url, headers=headers or {}, timeout=timeout)
                resp.raise_for_status()
                return resp.text
            except Exception as e:
                if attempt == 2:
                    raise
                time.sleep(1 * (attempt + 1))
        return ""
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

            elif data_type == "estimate_nav":
                # 盘中估算净值（东方财富 fundgz 接口）
                raw = self._fetch_fund_estimate_nav_sync(symbol)
                if raw is None:
                    return CollectResult(False, None, "akshare", symbol, "estimate_nav", "盘中估算净值不可用（非交易日或接口失败）", (now_tz()-start).total_seconds())
                normalized = {
                    "source": "fundgz",
                    "symbol": symbol,
                    "data_type": "estimate_nav",
                    "fund_name": raw.get('fund_name'),
                    "nav_date": raw.get('nav_date'),
                    "dwjz": raw.get('dwjz'),
                    "estimate_nav": raw.get('estimate_nav'),
                    "estimate_growth": raw.get('estimate_growth'),
                    "estimate_time": raw.get('estimate_time'),
                    "raw": raw,
                }
                return CollectResult(True, normalized, "fundgz", symbol, "estimate_nav", duration_seconds=(now_tz()-start).total_seconds())
            
            elif data_type == "news":
                raw_list = self._fetch_stock_news_sync(symbol)
                if raw_list is None:
                    return CollectResult(False, None, "akshare", symbol, "news", "新闻数据不可用", (now_tz()-start).total_seconds())
                normalized_list = [self._normalize_news(item, symbol) for item in raw_list]
                return CollectResult(True, normalized_list, "akshare", symbol, "news", duration_seconds=(now_tz()-start).total_seconds())
            
            elif data_type == "price":
                backup_sources = kwargs.get('backup_sources')
                raw = await self._fetch_stock_quote(symbol, data_type='price', backup_sources=backup_sources)
                if raw is None:
                    return CollectResult(False, None, "akshare", symbol, "price", "实时行情数据不可用（所有源失败）", (now_tz()-start).total_seconds())
                normalized = self._normalize_price(raw, symbol)
                return CollectResult(True, normalized, raw.get('source', 'akshare'), symbol, "price", duration_seconds=(now_tz()-start).total_seconds())

            elif data_type == "fundamental":
                # 使用同步的 fetch_fundamental（自动区分基金/股票）
                raw = self.fetch_fundamental(symbol)
                if raw is None:
                    source_hint = "雪球" if self._is_fund_symbol(symbol) else "腾讯"
                    return CollectResult(False, None, "akshare", symbol, "fundamental", f"基本面数据不可用（{source_hint}源失败）", (now_tz()-start).total_seconds())
                # 添加 data_type 标记
                raw['data_type'] = 'fundamental'
                source = raw.get('source', 'xueqiu' if self._is_fund_symbol(symbol) else 'tencent')
                return CollectResult(True, raw, source, symbol, "fundamental", duration_seconds=(now_tz()-start).total_seconds())
            
            elif data_type == "index":
                # 采集大盘/指数数据（使用腾讯新浪接口）
                codes = kwargs.get('codes', ['sh000001', 'sh000300', 'sh000688', 'sz399001', 'sz399006'])
                raw_list = self._fetch_index_data_sync(codes)
                if raw_list is None:
                    return CollectResult(False, None, "akshare", symbol or "*", "index", "指数数据不可用", (now_tz()-start).total_seconds())
                normalized_list = [self._normalize_index(item) for item in raw_list]
                return CollectResult(True, normalized_list, "akshare", symbol or "*", "index", duration_seconds=(now_tz()-start).total_seconds())

            elif data_type == "announcement":
                # 采集公告数据（巨潮资讯）
                raw_list = self._fetch_announcement_cninfo(symbol)
                if raw_list is None:
                    return CollectResult(False, None, "akshare", symbol, "announcement", "公告数据不可用", (now_tz()-start).total_seconds())
                normalized_list = [self._normalize_announcement(item, symbol) for item in raw_list]
                return CollectResult(True, normalized_list, "cninfo", symbol, "announcement", duration_seconds=(now_tz()-start).total_seconds())

            elif data_type == "community":
                # 采集社区/股吧数据（东方财富）
                raw_list = self._fetch_community_guba(symbol)
                if raw_list is None:
                    return CollectResult(False, None, "akshare", symbol, "community", "社区数据不可用", (now_tz()-start).total_seconds())
                normalized_list = [self._normalize_community(item, symbol) for item in raw_list]
                return CollectResult(True, normalized_list, "eastmoney_guba", symbol, "community", duration_seconds=(now_tz()-start).total_seconds())

            elif data_type == "minute":
                # 采集分钟K线数据（当日所有分钟线）
                import asyncio
                loop = asyncio.get_event_loop()
                bars = await loop.run_in_executor(None, self._fetch_minute_bars_sina, symbol, '1')
                if bars is None:
                    return CollectResult(False, None, "akshare", symbol, "minute", "分钟数据不可用", (now_tz()-start).total_seconds())
                return CollectResult(True, bars, "sina", symbol, "minute", duration_seconds=(now_tz()-start).total_seconds())

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
            elif result.data_type == "index":
                # index 返回的是列表
                return db.insert_index(result.data)
            elif result.data_type == "announcement":
                # announcement 返回的是列表
                success = True
                for item in result.data:
                    if not db.insert_announcement(item):
                        success = False
                return success
            elif result.data_type == "community":
                # community 返回的是列表
                success = True
                for item in result.data:
                    if not db.insert_community(item):
                        success = False
                return success
            elif result.data_type == "minute":
                bars = result.data if isinstance(result.data, list) else [result.data]
                return db.insert_minute_bars(bars) > 0
            elif result.data_type == "estimate_nav":
                return db.insert_estimate_nav(result.data)
            elif result.data_type == "daily":
                return db.insert_daily(result.data)
            return False
        except Exception as e:
            self.logger.error(f"入库失败 {result.data_type}: {e}")
            return False

    async def collect_batch(self, symbols: List[str], data_type: str, **kwargs) -> List[CollectResult]:
        results = []
        for symbol in symbols:
            result = await self.collect(symbol, data_type, **kwargs)
            results.append(result)
        return results

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

    # ========== 盘中基金估算净值 ==========
    def _fetch_fund_estimate_nav_sync(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        从东方财富 fundgz.1234567.com.cn 获取盘中估算净值

        API返回格式：jsonpgz({"fundcode":"000001","name":"华夏成长混合",
                              "jzrq":"2026-02-10","dwjz":"1.1490",
                              "gsz":"1.1370","gszzl":"-1.05",
                              "gztime":"2026-02-11 15:00"});
        """
        import re, json, requests as _requests
        url = f'http://fundgz.1234567.com.cn/js/{symbol}.js'
        try:
            text = _requests.get(url, timeout=10).text
            match = re.search(r'jsonpgz\((.*)\)\s*;?\s*$', text)
            if not match:
                self.logger.warning(f"_fetch_fund_estimate_nav({symbol})无法解析响应")
                return None
            data = json.loads(match.group(1))
            return {
                'fund_code': data.get('fundcode', symbol),
                'fund_name': data.get('name', ''),
                'nav_date': data.get('jzrq', ''),      # 昨日确认净值日期
                'dwjz': float(data.get('dwjz', 0)),    # 昨日单位净值
                'estimate_nav': float(data.get('gsz', 0)),   # 估算净值
                'estimate_growth': float(data.get('gszzl', 0)),  # 估算涨幅%
                'estimate_time': data.get('gztime', ''),  # 估算时间
            }
        except Exception as e:
            self.logger.warning(f"_fetch_fund_estimate_nav({symbol})异常: {e}")
            return None

    def _normalize_nav(self, raw: Dict[str, Any], symbol: str) -> Dict[str, Any]:
        nav_date_str = raw.get('nav_date', '')
        try:
            # 解析原始日期（如20260403），返回YYYY-MM-DD格式
            if nav_date_str:
                if isinstance(nav_date_str, str) and len(nav_date_str) == 8:
                    # 格式: 20260403
                    nav_date = f"{nav_date_str[:4]}-{nav_date_str[4:6]}-{nav_date_str[6:8]}"
                else:
                    nav_date = str(nav_date_str)[:10]  # 取前10位
            else:
                nav_date = now_tz().strftime('%Y-%m-%d')
        except Exception as e:
            logger.warning(f"_normalize_nav日期解析失败: {e}, 使用今天")
            nav_date = now_tz().strftime('%Y-%m-%d')
        return {
            "source": "akshare",
            "symbol": symbol,
            "data_type": "nav",
            "nav_date": nav_date,
            "nav": raw.get('nav'),
            "acc_nav": raw.get('acc_nav'),
            "change_pct": raw.get('change_pct'),
            "raw": raw
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
                    'summary': row.get('摘要', ''),
                    'url': row.get('链接', ''),
                    'source': row.get('来源', ''),
                    'published': row.get('发布时间', '')
                })
            return news_list
        except Exception as e:
            self.logger.warning(f"_fetch_stock_news_sync异常: {e}")
            return []

    def _fetch_index_data_sync(self, codes: List[str]) -> Optional[List[Dict[str, Any]]]:
        """从腾讯获取指数实时数据"""
        try:
            df = ak.stock_zh_index_spot_sina()
            if df.empty:
                return None
            # 筛选需要的指数
            filtered = df[df['代码'].isin(codes)]
            if filtered.empty:
                # 如果没找到，尝试前缀匹配
                filtered = df[df['代码'].isin([c.replace('sh', '').replace('sz', '') for c in codes])]
                if filtered.empty:
                    return None
            result = []
            for _, row in filtered.iterrows():
                result.append({
                    'code': row.get('代码', ''),
                    'name': row.get('名称', ''),
                    'price': row.get('最新价', 0),
                    'change_pct': row.get('涨跌幅', 0),
                    'change_amount': row.get('涨跌额', 0),
                    'volume': row.get('成交量', 0),
                    'amount': row.get('成交额', 0),
                    'high': row.get('最高', 0),
                    'low': row.get('最低', 0),
                    'open': row.get('今开', 0),
                    'prev_close': row.get('昨收', 0),
                })
            return result
        except Exception as e:
            self.logger.warning(f"_fetch_index_data_sync异常: {e}")
            return None

    # ========== 公告采集（巨潮资讯） ==========
    def _fetch_announcement_cninfo(self, symbol: str) -> Optional[List[Dict[str, Any]]]:
        """从巨潮资讯获取公告数据"""
        try:
            # 近30天
            end_date = now_tz().strftime('%Y%m%d')
            start_date = (now_tz() - timedelta(days=30)).strftime('%Y%m%d')
            df = ak.stock_zh_a_disclosure_report_cninfo(
                symbol=symbol,
                market='沪深京',
                start_date=start_date,
                end_date=end_date
            )
            if df is None or df.empty:
                return []
            result = []
            for _, row in df.head(50).iterrows():
                result.append({
                    'title': str(row.get('公告标题', '')),
                    'announcement_time': str(row.get('公告时间', '')),
                    'link': str(row.get('公告链接', '')),
                    'raw': row.to_dict()
                })
            return result
        except Exception as e:
            self.logger.warning(f"_fetch_announcement_cninfo({symbol})异常: {e}")
            return None

    def _normalize_announcement(self, raw: Dict[str, Any], symbol: str) -> Dict[str, Any]:
        """标准化公告数据"""
        ann_time = raw.get('announcement_time', '')
        return {
            'source': 'cninfo',
            'symbol': symbol,
            'title': raw.get('title', ''),
            'announcement_time': ann_time,
            'link': raw.get('link', ''),
            'raw': raw.get('raw', {})
        }

    # ========== 社区/股吧采集（东方财富） ==========
    # 重要发现（2026-04-10）：东方财富股吧 guba.eastmoney.com 是服务端渲染，
    # 数据直接嵌入 HTML 的 JavaScript 变量 var article_list={...} 中，
    # 不需要 JS 客户端渲染！无需 Playwright 等浏览器自动化工具。
    #
    # 帖子数据结构（article_list.re 数组）：
    #   post_id, post_title, post_click_count, post_comment_count,
    #   post_publish_time, nick, stockbar_name, source, ...
    #
    # 注意：author 字段（nick）有时为空，但点击/回复数据完整。
    def _fetch_community_guba(self, symbol: str) -> Optional[List[Dict[str, Any]]]:
        """从东方财富股吧获取社区帖子（从页面JS变量中提取JSON）"""
        try:
            url = f"https://guba.eastmoney.com/list,{symbol},1,f.html"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': 'https://guba.eastmoney.com/'
            }
            import requests
            import re
            import json as json_lib
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code != 200:
                self.logger.warning(f"_fetch_community_guba({symbol}) HTTP {resp.status_code}")
                return None

            html = resp.text
            # 从页面JS变量中提取 article_list JSON
            match = re.search(r'var article_list=(\{.*?\});', html, re.DOTALL)
            if not match:
                self.logger.warning(f"_fetch_community_guba({symbol}) 未找到 article_list 变量")
                return []
            
            data = json_lib.loads(match.group(1))
            posts = data.get('re', [])
            if not posts:
                self.logger.warning(f"_fetch_community_guba({symbol}) 帖子列表为空")
                return []
            
            result = []
            for p in posts[:50]:
                post_id = p.get('post_id', '')
                result.append({
                    'title': p.get('post_title', ''),
                    'author': p.get('nick', ''),
                    'reply_count': p.get('post_comment_count', 0),
                    'click_count': p.get('post_click_count', 0),
                    'published': p.get('post_publish_time', ''),
                    'link': f"https://guba.eastmoney.com/post/{post_id}" if post_id else '',
                    'stockbar_name': p.get('stockbar_name', ''),
                    'raw': p
                })
            self.logger.info(f"_fetch_community_guba({symbol}) 成功获取 {len(result)} 条帖子")
            return result
        except Exception as e:
            self.logger.warning(f"_fetch_community_guba({symbol})异常: {e}")
            return None

    def _normalize_community(self, raw: Dict[str, Any], symbol: str) -> Dict[str, Any]:
        """标准化社区数据"""
        return {
            'source': 'eastmoney_guba',
            'symbol': symbol,
            'title': raw.get('title', ''),
            'author': raw.get('author', ''),
            'reply_count': raw.get('reply_count', 0),
            'click_count': raw.get('click_count', 0),
            'published': raw.get('published', ''),
            'link': raw.get('link', ''),
            'raw': raw.get('raw', {})
        }

    def _normalize_index(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """标准化指数数据"""
        return {
            'source': 'tencent_sina',
            'code': raw.get('code', ''),
            'name': raw.get('name', ''),
            'data_type': 'index',
            'price': raw.get('price', 0),
            'change_pct': raw.get('change_pct', 0),
            'change_amount': raw.get('change_amount', 0),
            'volume': raw.get('volume', 0),
            'amount': raw.get('amount', 0),
            'high': raw.get('high', 0),
            'low': raw.get('low', 0),
            'open': raw.get('open', 0),
            'prev_close': raw.get('prev_close', 0),
            'timestamp': now_tz().isoformat()
        }

    def _normalize_news(self, raw: Dict[str, Any], symbol: Optional[str]) -> Dict[str, Any]:
        pub_str = raw.get('published', '')
        try:
            if pub_str:
                try:
                    published = datetime.strptime(pub_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=TZ)
                except:
                    published = now_tz()
            else:
                published = now_tz()
        except:
            published = now_tz()
        return {
            "source": "akshare",
            "symbol": symbol,
            "data_type": "news",
            "title": raw.get('title', ''),
            "summary": raw.get('summary', ''),
            "link": raw.get('url', ''),
            "published": published.isoformat(),
            "raw": raw
        }

    # ========== 股票行情（多源降级）==========
    async def _fetch_stock_quote(self, code: str, data_type: str = 'price', backup_sources: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._fetch_stock_quote_sync, code, data_type, backup_sources)


    def _is_fund_symbol(self, symbol: str) -> bool:
        """判断是否为基金代码（以0、1、4、5开头且长度6位）"""
        return len(symbol) == 6 and symbol[0] in ('0', '1', '4', '5') and symbol.isdigit()

    def _fetch_fund_fundamental_xq(self, symbol: str) -> Optional[Dict[str, Any]]:
        """从雪球获取基金基本信息"""
        try:
            df = ak.fund_individual_basic_info_xq(symbol=symbol)
            if df is None or df.empty:
                return None
            # 转换为dict格式
            info = {}
            for _, row in df.iterrows():
                info[row['item']] = row['value']
            return {
                'symbol': symbol,
                'name': info.get('基金名称', ''),
                'full_name': info.get('基金全称', ''),
                'company': info.get('基金公司', ''),
                'manager': info.get('基金经理', ''),
                'fund_type': info.get('基金类型', ''),
                'establish_date': info.get('成立时间', ''),
                'scale': info.get('最新规模', ''),
                'strategy': info.get('投资策略', ''),
                'benchmark': info.get('业绩比较基准', ''),
                '托管行': info.get('托管银行', ''),
            }
        except Exception as e:
            self.logger.warning(f"_fetch_fund_fundamental_xq({symbol}) 失败: {e}")
            return None

    def fetch_fundamental(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取基本面数据（自动识别基金/股票）"""
        if self._is_fund_symbol(symbol):
            # 基金使用雪球接口
            return self._fetch_fund_fundamental_xq(symbol)
        else:
            # 股票使用腾讯接口（转换为sh/sz格式）
            code = ('sh' if symbol.startswith('6') else 'sz') + symbol
            return self._fetch_stock_fundamental_tencent(code)

    def normalize_fundamental(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """标准化基本面数据（直接透传）"""
        # raw已经包含所有字段，只需添加symbol和data_type标记
        normalized = dict(raw)
        normalized['data_type'] = 'fundamental'
        return normalized

    def _fetch_stock_quote_sync(self, code: str, data_type: str = 'price', backup_sources: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        now = now_tz()
        is_trading_hours = (
            (now.hour == 9 and now.minute >= 30) or
            (10 <= now.hour < 11) or
            (now.hour == 11 and now.minute < 30) or
            (13 <= now.hour < 15)
        )
        # 主源
        if data_type == 'fundamental':
            # 转换为腾讯格式（添加sh/sz前缀）
            if not (code.startswith('sh') or code.startswith('sz')):
                code = ('sh' if code.startswith('6') else 'sz') + code
            return self._fetch_stock_fundamental_tencent(code) or self._fetch_stock_quote_tencent(code)
        elif data_type == 'price':
            # 继续原有价格采集流程
            pass
        else:
            self.logger.warning(f"未知data_type: {data_type}")
            return None
        
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
        # 降级1: akshare历史
        try:
            import datetime as dt
            end_date = now
            for i in range(7):
                check_day = end_date - dt.timedelta(days=i)
                if check_day.weekday() < 5:
                    end_date = check_day
                    break
            date_str = end_date.strftime('%Y%m%d')
            df = ak.stock_zh_a_hist(symbol=code, period='daily', start_date=date_str, end_date=date_str)
            if not df.empty:
                row = df.iloc[0]
                self.logger.info(f"_fetch_stock_quote_sync({code}) [akshare_hist] 降级成功")
                return {
                    'source': 'akshare_hist',
                    'price': float(row['收盘']),
                    'volume': float(row['成交量']),
                    'amount': float(row.get('成交额', 0.0)),
                    'high': float(row['最高']),
                    'low': float(row['最低']),
                    'open': float(row['开盘']),
                    'prev_close': float(row.get('前收盘', row['开盘'])),
                    'timestamp': now.isoformat()
                }
        except Exception as e:
            self.logger.error(f"_fetch_stock_quote_sync({code}) [akshare_hist] 降级失败: {e}")
        # 降级2: 腾讯
        if backup_sources:
            for source in backup_sources:
                if source == 'tencent':
                    try:
                        result = self._fetch_stock_quote_tencent(code)
                        if result:
                            self.logger.info(f"_fetch_stock_quote_sync({code}) [tencent] 降级成功")
                            return result
                    except Exception as e:
                        self.logger.error(f"_fetch_stock_quote_sync({code}) [tencent] 降级失败: {e}")
                elif source == 'sina':
                    try:
                        result = self._fetch_stock_quote_sina(code)
                        if result:
                            self.logger.info(f"_fetch_stock_quote_sync({code}) [sina] 降级成功")
                            return result
                    except Exception as e:
                        self.logger.error(f"_fetch_stock_quote_sync({code}) [sina] 降级失败: {e}")
                elif source == 'tushare':
                    pass
        return None

    def _fetch_stock_quote_tencent(self, code: str) -> Optional[Dict[str, Any]]:
        """从腾讯获取个股行情（包含基本面字段）"""
        url = f"https://qt.gtimg.cn/q={code}"
        try:
            raw = self._get_with_backoff(url, headers={"Referer": "https://gu.qq.com/"})
        except Exception as e:
            self.logger.warning(f"_fetch_stock_quote_tencent({code}) 失败: {e}")
            return None
        self.logger.info(f"[tencent] 返回长度: {len(raw)}")
        if "~" not in raw:
            return None
        parts = raw.split("~")
        if len(parts) < 48:
            self.logger.warning(f"字段不足({len(parts)})，跳过基本面")
            return None
        try:
            price = float(parts[3]) if parts[3] else 0.0
            prev_close = float(parts[4]) if parts[4] else 0.0
            open_ = float(parts[5]) if parts[5] else 0.0
            volume = float(parts[6]) if parts[6] else 0.0
            amount = float(parts[7]) if parts[7] else 0.0
            high = float(parts[9]) if parts[9] else 0.0
            low = float(parts[8]) if parts[8] else 0.0

            # 基本面字段（可能为空）
            pe = float(parts[46]) if len(parts) > 46 and parts[46] else None
            pb_val = float(parts[47]) if len(parts) > 47 and parts[47] else None
            pb = pb_val if pb_val < 100 else None  # 合理性校验
            total_mv = float(parts[50]) if len(parts) > 50 and parts[50] else None
            circ_mv = float(parts[51]) if len(parts) > 51 and parts[51] else None
            industry = parts[52] if len(parts) > 52 and parts[52] else None
        except Exception as e:
            self.logger.warning(f"_fetch_stock_quote_tencent({code}) 字段解析失败: {e}")
            return None
        trade_time = now_tz().isoformat()
        self.logger.info(f"_fetch_stock_quote_tencent({code}) 成功: price={price}, pe={pe}, pb={pb}")
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
            # 基本面附加信息
            'pe_ttm': pe,
            'pb': pb,
            'total_mv': total_mv,
            'circ_mv': circ_mv,
            'industry': industry,
            'raw': raw[:500],  # 截断保存原始
        }



    def _fetch_stock_fundamental_tencent(self, code: str) -> Optional[Dict[str, Any]]:
        """从腾讯获取基本面数据（PE/市值等）"""
        url = f"https://qt.gtimg.cn/q={code}"
        try:
            raw = self._get_with_backoff(url, headers={"Referer": "https://gu.qq.com/"})
        except Exception as e:
            self.logger.warning(f"_fetch_stock_fundamental_tencent({code}) 失败: {e}")
            return None
        if "~" not in raw:
            return None
        parts = raw.split("~")
        try:
            # 字段索引（已验证）
            price = float(parts[3]) if len(parts) > 3 and parts[3] else None
            pe_ttm = float(parts[46]) if len(parts) > 46 and parts[46] else None
            total_mv = float(parts[50]) if len(parts) > 50 and parts[50] else None
            circ_mv = float(parts[51]) if len(parts) > 51 and parts[51] else None
            # PB：索引47，需合理性校验（通常<100）
            pb = None
            if len(parts) > 47 and parts[47]:
                try:
                    pb_val = float(parts[47])
                    pb = pb_val if pb_val < 100 else None
                except:
                    pb = None
            # 行业代码：索引52（与53相同）
            industry = parts[52] if len(parts) > 52 and parts[52] else None
        except Exception as e:
            self.logger.warning(f"解析基本面失败: {e}")
            return None
        return {
            'source': 'tencent',
            'symbol': code[2:],
            'price': price,
            'pe_ttm': pe_ttm,
            'pb': pb,
            'total_mv': total_mv,
            'circ_mv': circ_mv,
            'industry': industry,
            'raw': raw[:1000],
            'date': now_tz().strftime('%Y-%m-%d')
        }


    def _fetch_stock_quote_sina(self, code: str) -> Optional[Dict[str, Any]]:
        """通过新浪财经获取股票实时行情"""
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
            # 格式: var hq_str_sh600519="名称,最新价,昨收,今开,..."
            data_str = text.split('=', 1)[1].strip('"')
            fields = data_str.split(',')
            if len(fields) < 10:
                self.logger.warning(f"_fetch_stock_quote_sina({code}) 字段不足: {len(fields)}")
                return None
            # 字段映射（基于常见格式）
            # [0]=名称, [1]=最新价, [2]=昨收, [3]=今开, [4]=成交量, [5]=成交额, [6]=最高, [7]=最低, ...
            name = fields[0]
            price = float(fields[1]) if fields[1] else 0.0
            prev_close = float(fields[2]) if fields[2] else 0.0
            open_ = float(fields[3]) if fields[3] else 0.0
            volume = float(fields[4]) if fields[4] else 0.0
            amount = float(fields[5]) if fields[5] else 0.0
            high = float(fields[6]) if fields[6] else 0.0
            low = float(fields[7]) if fields[7] else 0.0
            # 时间：通常没有，用当前时间
            trade_time = now_tz().isoformat()
            self.logger.info(f"_fetch_stock_quote_sina({code}) 成功: price={price}")
            return {
                'source': 'sina',
                'price': price,
                'volume': volume,
                'amount': amount,
                'high': high,
                'low': low,
                'open': open_,
                'prev_close': prev_close,
                'timestamp': trade_time
            }
        except Exception as e:
            self.logger.error(f"_fetch_stock_quote_sina({code})异常: {e}")
            return None


            # 查找指定股票
            code_col = '代码' if '代码' in df.columns else 'code'
            if code_col not in df.columns:
                self.logger.warning(f"_fetch_stock_quote_sina_spot({code}) 缺少代码列")
                return None
            row = df[df[code_col] == code]
            if row.empty:
                self.logger.warning(f"_fetch_stock_quote_sina_spot({code}) 未找到该股票")
                return None
            row = row.iloc[0]
            # 字段映射（基于ak.stock_zh_a_spot的返回）
            price = float(row.get('最新价', 0))
            prev_close = float(row.get('昨收', 0))
            open_ = float(row.get('今开', 0))
            volume = float(row.get('成交量', 0))
            amount = float(row.get('成交额', 0))
            high = float(row.get('最高', 0))
            low = float(row.get('最低', 0))
            # 新浪实时没有明确时间戳，用当前时间
            trade_time = now_tz().isoformat()
            self.logger.info(f"_fetch_stock_quote_sina_spot({code}) 成功: price={price}")
            return {
                'source': 'sina_spot',
                'price': price,
                'volume': volume,
                'amount': amount,
                'high': high,
                'low': low,
                'open': open_,
                'prev_close': prev_close,
                'timestamp': trade_time
            }
        except Exception as e:
            self.logger.error(f"_fetch_stock_quote_sina_spot({code})异常: {e}")
            return None
    def _normalize_price(self, raw: Dict[str, Any], symbol: str) -> Dict[str, Any]:
        trade_time_val = raw.get('timestamp')
        if trade_time_val:
            if isinstance(trade_time_val, datetime):
                if trade_time_val.tzinfo is None:
                    trade_time_val = trade_time_val.replace(tzinfo=TZ)
                trade_time_iso = trade_time_val.isoformat()
            else:
                trade_time_iso = str(trade_time_val)
        else:
            trade_time_iso = now_tz().isoformat()
        return {
            "source": raw.get('source', 'akshare'),
            "symbol": symbol,
            "data_type": "price",
            "trade_time": trade_time_iso,
            "price": raw.get('price'),
            "volume": raw.get('volume'),
            "amount": raw.get('amount'),
            "high": raw.get('high'),
            "low": raw.get('low'),
            "open": raw.get('open'),
            "prev_close": raw.get('prev_close'),
            "raw": raw
        }
    # [已废弃: fetch_fundamental 已移至第242行，支持基金/股票自动识别]

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

    # ==================== MINUTE DATA ====================

    def _fetch_minute_bars_sina(self, symbol: str, period: str = '1') -> Optional[List[Dict[str, Any]]]:
        """通过新浪财经获取个股分钟K线数据（当日所有分钟线）

        Args:
            symbol: 股票代码，如 'sh600519' 或 'sz000001'
            period: '1'=1分钟, '5'=5分钟
        Returns:
            分钟K线列表，每条含 open/high/low/close/volume/amount/trade_time
        """
        try:
            # 转换代码格式：symbol可能是 '600519' 或 'sh600519'
            code = symbol
            for prefix in ['sh', 'sz', 'hk', 'gb_']:
                if code.startswith(prefix):
                    code = code[len(prefix):]
                    break

            # 新浪需要 sh/sz 前缀
            sina_code = ('sh' + code) if code.startswith('6') else ('sz' + code)

            df = ak.stock_zh_a_minute(symbol=sina_code, period=period, adjust='')
            if df is None or df.empty:
                return None

            bars = []
            today = now_tz().strftime('%Y-%m-%d')
            for _, row in df.iterrows():
                # 处理时间格式
                trade_time_raw = str(row.get('day', ''))
                if not trade_time_raw or trade_time_raw == 'nan':
                    continue
                # 时间格式: '2026-04-10 09:31:00'
                if ' ' not in trade_time_raw:
                    continue
                trade_date, trade_hour_min = trade_time_raw.split(' ', 1)
                # 只保留今日数据
                if trade_date != today:
                    continue
                bars.append({
                    'symbol': symbol,
                    'name': None,
                    'trade_date': trade_date,
                    'trade_time': trade_time_raw,
                    'open': float(row['open']) if row.get('open') else 0,
                    'high': float(row['high']) if row.get('high') else 0,
                    'low': float(row['low']) if row.get('low') else 0,
                    'close': float(row['close']) if row.get('close') else 0,
                    'volume': int(float(row['volume'])) if row.get('volume') else 0,
                    'amount': float(row['amount']) if row.get('amount') else 0,
                    'source': 'sina',
                })
            self.logger.info(f"_fetch_minute_bars_sina({symbol}) 获取{len(bars)}条分钟K")
            return bars if bars else None
        except Exception as e:
            self.logger.error(f"_fetch_minute_bars_sina({symbol})异常: {e}")
            return None

