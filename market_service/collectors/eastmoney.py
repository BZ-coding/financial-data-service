#!/usr/bin/env python3
"""
东方财富（EastMoney）数据采集器
支持：A股、港股（secid=116开头）、指数
文档：https://www.eastmoney.com
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

TZ = timezone(timedelta(hours=8))

def now_tz():
    return datetime.now(TZ)


from .base import BaseCollector, CollectResult

logger = logging.getLogger(__name__)


class EastMoneyCollector(BaseCollector):
    """东方财富数据采集器"""

    # 字段索引（东方财富 push2.eastmoney.com API）
    # f43=当前价, f44=最高, f45=最低, f46=今开, f47=成交量
    # f48=成交额, f50=换手率, f57=股票代码, f58=股票名称
    # f107=涨跌额, f169=市场类型（MktLineL3.0有）, f170=更新时间
    EM_FIELDS = "f43,f44,f45,f46,f47,f48,f50,f57,f58,f107"

    def __init__(self, rate_limit_per_minute: int = 60):
        BaseCollector.__init__(self, rate_limit_per_minute)
        self.base_url = "https://push2.eastmoney.com/api/qt/stock/get"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://quote.eastmoney.com/",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9",
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.logger = logging.getLogger(__name__)

    def _get(self, url: str, params: Dict = None, timeout: int = 8) -> Optional[Dict]:
        try:
            resp = self.session.get(url, params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            return None
        except Exception as e:
            self.logger.warning(f"EastMoney 请求失败: {e}")
            return None

    def _to_em_secid(self, symbol: str) -> str:
        """
        将标准符号转换为东方财富 secid 格式
        A股：600519 -> 1.600519 (SH=1, SZ=0)
        港股：hk03690 -> 116.03690
        """
        symbol = symbol.upper()
        if symbol.startswith("HK"):
            code = symbol[2:]
            return f"116.{code}"  # 港股 secid 前缀 116
        elif symbol.startswith("6"):
            return f"1.{symbol}"  # 沪市
        elif symbol.startswith("0") or symbol.startswith("3"):
            return f"0.{symbol}"  # 深市
        elif symbol.startswith("8") or symbol.startswith("4"):
            return f"0.{symbol}"  # 北交所/新三板
        else:
            return symbol

    async def fetch_price(self, symbol: str) -> CollectResult:
        """获取实时行情"""
        start = datetime.now()
        secid = self._to_em_secid(symbol)
        ts = int(datetime.now().timestamp() * 1000)
        params = {
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",
            "fltt": "2",
            "invt": "2",
            "secid": secid,
            "fields": self.EM_FIELDS,
            "cb": "",
            "req_trace": str(ts),  # 关键参数：绕过数据中心IP封禁
            "_": ts,
        }
        data = self._get(self.base_url, params)
        if not data:
            return CollectResult(success=False, source="eastmoney", symbol=symbol, data_type="price", error="请求失败")
        try:
            rc = data.get("rc", -1)
            if rc != 0:
                return CollectResult(success=False, source="eastmoney", symbol=symbol, data_type="price", error=f"RC={rc}")
            raw = data.get("data", {})
            if not raw:
                return CollectResult(success=False, source="eastmoney", symbol=symbol, data_type="price", error="无数据")
            price = raw.get("f43")
            if price is None or price == 0:
                return CollectResult(success=False, source="eastmoney", symbol=symbol, data_type="price", error="价格为空")
            # 涨跌额
            change_amt = raw.get("f107")
            # 涨跌额没有的话从当前价和昨收算
            prev_close = raw.get("f60")  # 昨收（如果有的话）
            if change_amt is None and prev_close:
                change_amt = round(price - prev_close, 2)
            result = {
                "symbol": symbol,
                "name": raw.get("f58", symbol),
                "price": price,
                "open": raw.get("f46"),
                "high": raw.get("f44"),
                "low": raw.get("f45"),
                "volume": raw.get("f47"),  # 成交量（股）
                "amount": raw.get("f48"),   # 成交额（元）
                "turnover_rate": raw.get("f50"),  # 换手率%
                "change_amt": change_amt,
                "prev_close": prev_close,
                "trade_time": now_tz().isoformat(),
                "raw": raw,
            }
            duration = (datetime.now() - start).total_seconds()
            return CollectResult(success=True, data=[result], source="eastmoney", symbol=symbol, data_type="price", duration_seconds=duration)
        except Exception as e:
            self.logger.warning(f"EastMoney price 解析失败 {symbol}: {e}")
            return CollectResult(success=False, source="eastmoney", symbol=symbol, data_type="price", error=str(e))

    async def collect(self, data_type: str, symbol: str, **kwargs) -> CollectResult:
        if data_type == "price":
            return await self.fetch_price(symbol)
        return CollectResult(success=False, source="eastmoney", symbol=symbol, data_type=data_type, error=f"不支持 data_type={data_type}")

    async def collect_batch(self, data_type: str, symbols: List[str], **kwargs) -> List[CollectResult]:
        """批量采集（顺序执行）"""
        results = []
        for symbol in symbols:
            result = await self.collect(data_type, symbol, **kwargs)
            results.append(result)
            await asyncio.sleep(0.3)  # 避免频率限制
        return results
