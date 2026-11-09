#!/usr/bin/env python3
"""
Massive (Polygon.io) 数据采集器
支持：美股、期权、指数、外汇、期货
文档：https://massive.com/docs
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
import requests

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

class MassiveCollector(BaseCollector):
    """Massive API 数据采集器"""
    
    def __init__(self, api_key: str, rate_limit_per_minute: int = 30):
        BaseCollector.__init__(self, rate_limit_per_minute)
        self.api_key = api_key
        self.base_url = "https://api.massive.com/v2"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "User-Agent": "Massive.com PythonClient/1.0"
        }
        self.logger = logging.getLogger(__name__)

    def _get(self, endpoint: str, params: Dict = None, timeout: int = 10) -> Optional[Dict]:
        """GET请求"""
        url = f"{self.base_url}{endpoint}"
        try:
            resp = requests.get(url, headers=self.headers, params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                self.logger.warning(f"Massive API 限流: {resp.text}")
                return None
            else:
                self.logger.warning(f"Massive API 错误 {resp.status_code}: {resp.text[:200]}")
                return None
        except Exception as e:
            self.logger.error(f"请求异常: {e}")
            return None

    # Massive不支持的市场后缀映射
    MASSIVE_MAPPING = {
        "US": "",      # 美股去后缀：TSLA.US -> TSLA
        "HK": None,    # 港股不支持
        "SH": ".SS",   # A股沪市：600519.SH -> 600519.SS
        "SZ": ".SZ",   # A股深市：000001.SZ -> 000001.SZ
    }

    def _to_massive_symbol(self, symbol: str) -> str:
        """将标准符号转换为Massive格式"""
        if "." in symbol:
            code, suffix = symbol.rsplit(".", 1)
            if suffix in self.MASSIVE_MAPPING:
                mapped = self.MASSIVE_MAPPING[suffix]
                return code + mapped if mapped else code
            return symbol
        return symbol

    async def collect(self, symbol: str, data_type: str, **kwargs) -> CollectResult:
        """采集单条数据"""
        start = now_tz()

        # 转换为Massive格式
        massive_symbol = self._to_massive_symbol(symbol)
        if massive_symbol is None:
            return CollectResult(False, None, "massive", symbol, data_type,
                               f"Massive不支持该市场: {symbol}", (now_tz()-start).total_seconds())

        if data_type == "price":
            # 获取日K线数据
            raw = self._fetch_daily_aggs(massive_symbol)
            if raw is None:
                return CollectResult(False, None, "massive", symbol, "price",
                                   "Massive价格数据不可用", (now_tz()-start).total_seconds())
            normalized = self._normalize_price(raw, symbol)
            return CollectResult(True, normalized, "massive", symbol, "price",
                               duration_seconds=(now_tz()-start).total_seconds())

        elif data_type == "minute":
            # 获取分钟K线数据
            multiplier = kwargs.get('multiplier', 1)
            timespan = kwargs.get('timespan', 'minute')
            raw = self._fetch_minute_aggs(massive_symbol, multiplier=multiplier, timespan=timespan)
            if raw is None:
                return CollectResult(False, None, "massive", symbol, "minute",
                                   "Massive分钟数据不可用", (now_tz()-start).total_seconds())
            normalized_list = [self._normalize_price(item, symbol) for item in raw]
            return CollectResult(True, normalized_list, "massive", symbol, "minute",
                               duration_seconds=(now_tz()-start).total_seconds())

        else:
            return CollectResult(False, None, "massive", symbol, data_type,
                               f"不支持的数据类型: {data_type}", (now_tz()-start).total_seconds())

    async def collect_batch(self, symbols: List[str], data_type: str, **kwargs) -> List[CollectResult]:
        """批量采集"""
        results = []
        for symbol in symbols:
            result = await self.collect(symbol, data_type, **kwargs)
            results.append(result)
            # 避免限流
            await asyncio.sleep(0.5)
        return results

    def _fetch_daily_aggs(self, symbol: str, from_date: str = None, to_date: str = None, 
                          limit: int = 30) -> Optional[Dict]:
        """获取日K线数据"""
        if from_date is None:
            # 默认取最近30天
            to_dt = datetime.now(TZ)
            from_dt = to_dt - timedelta(days=30)
            from_date = from_dt.strftime('%Y-%m-%d')
            to_date = to_dt.strftime('%Y-%m-%d')
        
        params = {
            "adjusted": "true",
            "sort": "desc",
            "limit": limit,
            "apiKey": self.api_key
        }
        
        endpoint = f"/aggs/ticker/{symbol}/range/1/day/{from_date}/{to_date}"
        return self._get(endpoint, params)

    def _fetch_minute_aggs(self, symbol: str, multiplier: int = 1, timespan: str = 'minute',
                           from_date: str = None, to_date: str = None, 
                           limit: int = 5000) -> Optional[List[Dict]]:
        """获取分钟K线数据"""
        if from_date is None:
            # 默认取最近7天
            to_dt = datetime.now(TZ)
            from_dt = to_dt - timedelta(days=7)
            from_date = from_dt.strftime('%Y-%m-%d')
            to_date = to_dt.strftime('%Y-%m-%d')
        
        params = {
            "adjusted": "true",
            "sort": "desc",
            "limit": limit,
            "apiKey": self.api_key
        }
        
        endpoint = f"/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_date}/{to_date}"
        result = self._get(endpoint, params)
        if result and 'results' in result:
            # 返回列表格式
            return result['results']
        return None

    def _normalize_price(self, raw: Dict, symbol: str) -> Dict[str, Any]:
        """标准化价格数据"""
        # Massive 返回格式: {"t": timestamp, "o": open, "h": high, "l": low, "c": close, "v": volume}
        # 或者嵌套格式: {"results": [{...}]}
        results = raw.get('results', [raw]) if isinstance(raw, dict) else [raw]
        kline = results[0] if results else raw
        
        timestamp_ms = kline.get('t', 0)
        if timestamp_ms:
            trade_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=TZ)
            trade_time_iso = trade_dt.isoformat()
        else:
            trade_time_iso = now_tz().isoformat()
        
        return {
            "source": "massive",
            "symbol": symbol,
            "data_type": "price",
            "trade_time": trade_time_iso,
            "price": kline.get('c'),      # close price
            "volume": kline.get('v'),     # volume
            "open": kline.get('o'),       # open
            "high": kline.get('h'),       # high
            "low": kline.get('l'),        # low
            "prev_close": kline.get('o'),  # 日线的open可作为参考
            "amount": None,
            "raw": raw
        }
