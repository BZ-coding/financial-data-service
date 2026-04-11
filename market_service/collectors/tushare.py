#!/usr/bin/env python3
"""
Tushare数据采集器
免费接口支持：
  - daily       股票日线（OHLCV）
  - stock_basic 股票基本信息
  - hsgt_top10  沪深港通Top10（北向资金）

数据来源：Tushare Pro API (https://tushare.pro)
Token: 配置在 config.yaml -> collectors.tushare.token
"""

import sys
import os
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone, timedelta
import logging

from .base import BaseCollector, CollectResult

TZ = timezone(timedelta(hours=8))

def now_tz():
    return datetime.now(TZ)


class TushareCollector(BaseCollector):
    """Tushare数据采集器（免费接口）"""

    def __init__(self, api_token: str = None, rate_limit_per_minute: int = 30):
        BaseCollector.__init__(self, rate_limit_per_minute)
        self.logger = logging.getLogger(__name__)
        self._api_token = api_token
        self._pro = None
        self._initialized = False

    def _init(self):
        """延迟初始化（避免模块加载时 Token 未配置就报错）"""
        if self._initialized:
            return
        self._initialized = True
        if not self._api_token:
            self.logger.warning("Tushare token 未配置，请检查 config.yaml")
            return
        try:
            import tushare as ts
            ts.set_token(self._api_token)
            self._pro = ts.pro_api()
            self.logger.info("Tushare 初始化成功")
        except Exception as e:
            self.logger.error(f"Tushare 初始化失败: {e}")
            self._pro = None

    # ========== PUBLIC API ==========

    async def collect(self, symbol: str, data_type: str, **kwargs) -> CollectResult:
        start = now_tz()
        self._init()

        if self._pro is None:
            return CollectResult(
                success=False, data=None, source="tushare",
                symbol=symbol, data_type=data_type,
                error="Tushare 未初始化（Token 未配置或初始化失败）",
                duration_seconds=0
            )

        try:
            if data_type == "daily":
                raw = self._fetch_daily(symbol, kwargs.get('limit', 5))
                if raw is None:
                    return CollectResult(
                        success=False, data=None, source="tushare",
                        symbol=symbol, data_type=data_type,
                        error="daily 数据不可用",
                        duration_seconds=(now_tz()-start).total_seconds()
                    )
                normalized = [self._normalize_daily(row) for row in raw]
                return CollectResult(
                    success=True, data=normalized, source="tushare",
                    symbol=symbol, data_type=data_type,
                    duration_seconds=(now_tz()-start).total_seconds()
                )

            elif data_type == "hsgt":
                # 沪深港通 Top10（北向资金）
                raw = self._fetch_hsgt_top10(kwargs.get('trade_date'))
                if raw is None:
                    return CollectResult(
                        success=False, data=None, source="tushare",
                        symbol=symbol, data_type=data_type,
                        error="沪深港通数据不可用",
                        duration_seconds=(now_tz()-start).total_seconds()
                    )
                normalized = [self._normalize_hsgt(row) for row in raw]
                return CollectResult(
                    success=True, data=normalized, source="tushare",
                    symbol=symbol, data_type=data_type,
                    duration_seconds=(now_tz()-start).total_seconds()
                )

            elif data_type == "stock_basic":
                raw = self._fetch_stock_basic(symbol)
                if raw is None:
                    return CollectResult(
                        success=False, data=None, source="tushare",
                        symbol=symbol, data_type=data_type,
                        error="股票基本信息不可用",
                        duration_seconds=(now_tz()-start).total_seconds()
                    )
                normalized = self._normalize_basic(raw)
                return CollectResult(
                    success=True, data=normalized, source="tushare",
                    symbol=symbol, data_type=data_type,
                    duration_seconds=(now_tz()-start).total_seconds()
                )

            else:
                return CollectResult(
                    success=False, data=None, source="tushare",
                    symbol=symbol, data_type=data_type,
                    error=f"Tushare 不支持数据类型: {data_type}",
                    duration_seconds=(now_tz()-start).total_seconds()
                )

        except Exception as e:
            self.logger.error(f"Tushare 采集异常 {symbol}/{data_type}: {e}")
            return CollectResult(
                success=False, data=None, source="tushare",
                symbol=symbol, data_type=data_type,
                error=str(e),
                duration_seconds=(now_tz()-start).total_seconds()
            )

    async def collect_batch(self, symbols: List[str], data_type: str, **kwargs) -> List[CollectResult]:
        results = []
        for symbol in symbols:
            result = await self.collect(symbol, data_type, **kwargs)
            results.append(result)
        return results

    # ========== 内部方法 ==========

    def _fetch_daily(self, symbol: str, limit: int = 5) -> Optional[List[Dict]]:
        """股票日线数据"""
        try:
            import tushare as ts
            ts.set_token(self._api_token)
            pro = ts.pro_api()
            # 转换代码格式：600519.SH -> 600519.SH
            df = pro.daily(ts_code=symbol, limit=limit)
            if df is None or df.empty:
                return None
            return df.to_dict('records')
        except Exception as e:
            self.logger.warning(f"tushare daily 失败 {symbol}: {e}")
            return None

    def _fetch_hsgt_top10(self, trade_date: str = None) -> Optional[List[Dict]]:
        """沪深港通 Top10 北向资金（附 pct_chg 来自 daily 数据）"""
        try:
            import tushare as ts
            ts.set_token(self._api_token)
            pro = ts.pro_api()
            df = pro.hsgt_top10(limit=10)
            if df is None or df.empty:
                return None

            records = df.to_dict('records')
            if not records:
                return records

            # 取交易日期（取第一条的日期）
            td = trade_date or records[0].get('trade_date')
            if not td:
                return records

            # 一次性拉当天所有股票的日线，获取 pct_chg
            pct_map: Dict[str, float] = {}
            try:
                daily_df = pro.daily(trade_date=td)
                if daily_df is not None and not daily_df.empty:
                    pct_map = dict(zip(daily_df['ts_code'], daily_df['pct_chg']))
            except Exception as e:
                self.logger.warning(f"补全 pct_chg 失败: {e}")

            # 给每条 hsgt 记录注入 pct_chg
            for row in records:
                ts_code = row.get('ts_code', '')
                row['pct_chg'] = pct_map.get(ts_code, 0.0)

            return records
        except Exception as e:
            self.logger.warning(f"tushare hsgt_top10 失败: {e}")
            return None

    def _fetch_stock_basic(self, symbol: str, limit: int = 1) -> Optional[Dict]:
        """股票基本信息"""
        try:
            import tushare as ts
            ts.set_token(self._api_token)
            pro = ts.pro_api()
            df = pro.stock_basic(ts_code=symbol, limit=limit)
            if df is None or df.empty:
                return None
            return df.iloc[0].to_dict()
        except Exception as e:
            self.logger.warning(f"tushare stock_basic 失败 {symbol}: {e}")
            return None

    # ========== 数据标准化 ==========

    def _normalize_daily(self, row: Dict) -> Dict:
        """标准化日线数据"""
        return {
            "symbol": row.get('ts_code', ''),
            "trade_date": row.get('trade_date', ''),
            "open": float(row.get('open', 0)),
            "high": float(row.get('high', 0)),
            "low": float(row.get('low', 0)),
            "close": float(row.get('close', 0)),
            "pre_close": float(row.get('pre_close', 0)),
            "change": float(row.get('change', 0)),
            "pct_chg": float(row.get('pct_chg', 0)),
            "vol": float(row.get('vol', 0)),
            "amount": float(row.get('amount', 0)),
            "source": "tushare",
            "data_type": "daily",
        }

    def _normalize_hsgt(self, row: Dict) -> Dict:
        """标准化沪深港通数据"""
        return {
            "trade_date": row.get('trade_date', ''),
            "ts_code": row.get('ts_code', ''),
            "name": row.get('name', ''),
            "close": float(row.get('close', 0)),
            "change": float(row.get('change', 0)),
            "pct_chg": float(row.get('pct_chg', 0)),
            "vol": float(row.get('vol', 0)),
            "amount": float(row.get('amount', 0)),
            "source": "tushare",
            "data_type": "hsgt",
        }

    def _normalize_basic(self, row: Dict) -> Dict:
        """标准化股票基本信息"""
        return {
            "symbol": row.get('symbol', ''),
            "ts_code": row.get('ts_code', ''),
            "name": row.get('name', ''),
            "area": row.get('area', ''),
            "industry": row.get('industry', ''),
            "market": row.get('market', ''),
            "list_date": row.get('list_date', ''),
            "source": "tushare",
            "data_type": "stock_basic",
        }
