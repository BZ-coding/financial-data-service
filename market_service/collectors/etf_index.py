"""ETF 基础画像 / 两融 / 指数估值 / 成份股 采集器（对标基金公司 ETF skill 数据面）。

数据源：akshare（东财 ETF spot、中证指数官网估值与成份股权重）。
口径纪律：所有输出带 as_of（trade_date）与频率标注（daily / weekly）。
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date

import pandas as pd

from .base import BaseCollector, CollectResult

logger = logging.getLogger(__name__)


def _norm_index_code(code: str) -> str:
    """csindex 返回的指数代码缺前导零（'300'），统一补齐为 6 位。"""
    code = (code or "").strip()
    if code.isdigit() and len(code) < 6:
        return code.zfill(6)
    return code


def _num(v):
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


class ETFProfileCollector(BaseCollector):
    """ETF 基础画像：价格/IOPV/折溢价/成交（日频）。"""

    name = "etf_profile"

    def _fetch_items(self, symbol: str | None = None) -> list[dict]:
        import akshare as ak

        df = ak.fund_etf_spot_em()
        if df is None or df.empty:
            return []
        trade_date = date.today().isoformat()
        items = []
        for _, row in df.iterrows():
            code = str(row.get("代码") or "")
            if not code:
                continue
            if symbol and code != symbol:
                continue
            items.append({
                "trade_date": trade_date,
                "code": code,
                "name": row.get("名称"),
                "price": _num(row.get("最新价")),
                "iopv": _num(row.get("IOPV实时估值")),
                "premium_discount_pct": _num(row.get("基金折价率")),
                "volume": _num(row.get("成交量")),
                "amount": _num(row.get("成交额")),
                "source": "eastmoney_em",
            })
        return items

    async def collect(self, symbol: str, data_type: str, **kwargs) -> CollectResult:
        import time as _t
        start = _t.monotonic()
        try:
            items = await asyncio.to_thread(self._fetch_items, None)  # 全市场快照
            return CollectResult(True, items, "eastmoney_em", symbol or "*", data_type,
                                 duration_seconds=_t.monotonic() - start)
        except Exception as exc:
            return CollectResult(False, None, "eastmoney_em", symbol or "*", data_type,
                                 error=repr(exc),
                                 duration_seconds=_t.monotonic() - start)

    async def collect_batch(self, symbols, data_type, **kwargs) -> List[CollectResult]:
        # spot 接口一次返回全市场，无需按标的循环
        r = await self.collect("*", data_type, **kwargs)
        return [r]


class IndexValuationCollector(BaseCollector):
    """指数估值：PE（TTM/MR）+ 股息率（总/已付），日频。

    percentile_5y 由服务在入库时基于历史窗口计算，这里只抓原始值。
    """

    name = "index_valuation"

    def __init__(self, index_codes: list[str] | None = None, **kwargs):
        super().__init__(rate_limit_per_minute=10)
        # 默认覆盖常见宽基 + 用户持仓常用指数
        self.index_codes = index_codes or ["000300", "000905", "000852", "000688", "000001"]

    def _fetch_items(self, codes: list[str]) -> list[dict]:
        import akshare as ak

        trade_date = date.today().isoformat()
        items = []
        for code in codes:
            try:
                df = ak.stock_zh_index_value_csindex(symbol=code)
                if df is None or df.empty:
                    continue
                last = df.tail(1).iloc[0]
                items.append({
                    "trade_date": str(last.get("日期") or trade_date),
                    "index_code": _norm_index_code(str(last.get("指数代码") or code)),
                    "index_name": last.get("指数中文简称"),
                    "pe_ttm": _num(last.get("市盈率1")),
                    "pe_mr": _num(last.get("市盈率2")),
                    "dividend_yield_total": _num(last.get("股息率1")),
                    "dividend_yield_paid": _num(last.get("股息率2")),
                    "source": "csindex",
                    "frequency": "daily",
                })
            except Exception as exc:
                logger.warning("index valuation %s failed: %r", code, exc)
        return items

    async def collect(self, symbol: str, data_type: str, **kwargs) -> CollectResult:
        import time as _t
        start = _t.monotonic()
        codes = [symbol] if symbol and symbol != "*" else self.index_codes
        try:
            items = await asyncio.to_thread(self._fetch_items, codes)
            return CollectResult(True, items, "csindex", symbol or "*", data_type,
                                 duration_seconds=_t.monotonic() - start)
        except Exception as exc:
            return CollectResult(False, None, "csindex", symbol or "*", data_type,
                                 error=repr(exc),
                                 duration_seconds=_t.monotonic() - start)

    async def collect_batch(self, symbols, data_type, **kwargs) -> List[CollectResult]:
        r = await self.collect("*", data_type, **kwargs)
        return [r]


class IndexConstituentsCollector(BaseCollector):
    """指数成份股权重（周频：中证官网月度/周度更新）。"""

    name = "index_constituents"

    def __init__(self, index_codes: list[str] | None = None, **kwargs):
        super().__init__(rate_limit_per_minute=10)
        self.index_codes = index_codes or ["000300", "000905", "000852"]

    def _fetch_items(self, codes: list[str]) -> list[dict]:
        import akshare as ak

        items = []
        for code in codes:
            try:
                df = ak.index_stock_cons_weight_csindex(symbol=code)
                if df is None or df.empty:
                    continue
                for _, row in df.iterrows():
                    items.append({
                        "as_of_date": str(row.get("日期") or date.today().isoformat()),
                        "index_code": _norm_index_code(str(row.get("指数代码") or code)),
                        "index_name": row.get("指数名称"),
                        "stock_code": str(row.get("成分券代码") or ""),
                        "stock_name": row.get("成分券名称"),
                        "exchange": row.get("交易所"),
                        "weight": _num(row.get("权重")),
                        "source": "csindex",
                        "frequency": "weekly",
                    })
            except Exception as exc:
                logger.warning("index constituents %s failed: %r", code, exc)
        return items

    async def collect(self, symbol: str, data_type: str, **kwargs) -> CollectResult:
        import time as _t
        start = _t.monotonic()
        codes = [symbol] if symbol and symbol != "*" else self.index_codes
        try:
            items = await asyncio.to_thread(self._fetch_items, codes)
            return CollectResult(True, items, "csindex", symbol or "*", data_type,
                                 duration_seconds=_t.monotonic() - start)
        except Exception as exc:
            return CollectResult(False, None, "csindex", symbol or "*", data_type,
                                 error=repr(exc),
                                 duration_seconds=_t.monotonic() - start)

    async def collect_batch(self, symbols, data_type, **kwargs) -> List[CollectResult]:
        r = await self.collect("*", data_type, **kwargs)
        return [r]


class ETFMarginCollector(BaseCollector):
    """ETF 融资融券余额（日频）。上交所/深交所披露口径。"""

    name = "etf_margin"

    async def collect(self, symbol: str, data_type: str, **kwargs) -> CollectResult:
        # 融资融券明细依赖交易所披露，akshare 接口变动频繁；MVP 先返回空并留接口。
        # TODO: 接入 ak.stock_margin_detail_sse / szse（分页较重，待排期）。
        return CollectResult(False, None, "exchange", symbol or "*", data_type,
                             error="etf_margin 采集器待接入交易所披露接口")

    async def collect_batch(self, symbols, data_type, **kwargs) -> List[CollectResult]:
        return [await self.collect(s, data_type, **kwargs) for s in (symbols or ["*"])]
