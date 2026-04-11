"""
TickFlow 日K采集器
数据源：https://tickflow.org 免费层
接口：klines.get(symbol, period='1d', as_dataframe=True)
限制：历史日K，盘中不更新（收盘后才刷新）
用途：akshare → tushare → tickflow 降级链路的最后一层
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from .base import BaseCollector, CollectResult

logger = logging.getLogger(__name__)

# 懒加载，延缓 import
_TickFlow = None


def _get_sync_tf():
    global _TickFlow
    if _TickFlow is None:
        from tickflow import TickFlow
        _TickFlow = TickFlow.free()
    return _TickFlow


class TickFlowCollector(BaseCollector):
    """TickFlow 日K采集器（免费层，仅历史日K）"""

    def __init__(self, rate_limit_per_minute: int = 30):
        super().__init__(rate_limit_per_minute)
        self._tf = None

    def _get_tf(self):
        if self._tf is None:
            from tickflow import TickFlow
            self._tf = TickFlow.free()
        return self._tf

    def _normalize(self, df, symbol: str) -> list:
        """将 DataFrame 转为 CollectResult.data 格式（并计算 pct_chg）"""
        if df is None or df.empty:
            return []

        import pandas as pd

        records = []
        prev_close = None
        for _, row in df.iterrows():
            trade_date = None
            if 'trade_date' in row:
                val = row['trade_date']
                if isinstance(val, (datetime, str)):
                    try:
                        dt = pd.to_datetime(val)
                        trade_date = dt.strftime('%Y%m%d')
                    except Exception:
                        pass

            close = float(row['close']) if row.get('close') is not None else None
            pct_chg = None
            if close is not None and prev_close is not None and prev_close != 0:
                pct_chg = round((close - prev_close) / prev_close * 100, 4)

            records.append({
                'symbol': symbol,
                'trade_date': trade_date,
                'open': float(row['open']) if row.get('open') is not None else None,
                'high': float(row['high']) if row.get('high') is not None else None,
                'low': float(row['low']) if row.get('low') is not None else None,
                'close': close,
                'volume': float(row['volume']) if row.get('volume') is not None else 0,
                'amount': float(row['amount']) if row.get('amount') is not None else 0,
                'pct_chg': pct_chg,
            })
            prev_close = close
        return records

    async def collect(
        self,
        symbol: str,
        data_type: str = "daily",
        **kwargs,
    ) -> CollectResult:
        """
        获取日K历史数据。
        tickflow.free() 每次调用从服务器拉最新历史数据，
        内部已有缓存（避免同一进程重复请求）。
        """
        start = datetime.now(timezone(timedelta(hours=8)))

        try:
            # tickflow 的 klines.get 是同步的，放到 executor
            loop = asyncio.get_event_loop()

            def _fetch():
                tf = self._get_tf()
                # 拉最近30天，足够覆盖近期交易
                df = tf.klines.get(symbol, period='1d', as_dataframe=True)
                return df

            df = await loop.run_in_executor(None, _fetch)

            if df is None or df.empty:
                return CollectResult(
                    success=False,
                    data=None,
                    source="tickflow",
                    symbol=symbol,
                    data_type="daily",
                    error="TickFlow 无数据",
                    duration_seconds=(datetime.now(timezone(timedelta(hours=8))) - start).total_seconds(),
                )

            records = self._normalize(df, symbol)
            if not records:
                return CollectResult(
                    success=False,
                    data=None,
                    source="tickflow",
                    symbol=symbol,
                    data_type="daily",
                    error="TickFlow 数据标准化失败",
                    duration_seconds=(datetime.now(timezone(timedelta(hours=8))) - start).total_seconds(),
                )

            duration = (datetime.now(timezone(timedelta(hours=8))) - start).total_seconds()
            return CollectResult(
                success=True,
                data=records,
                source="tickflow",
                symbol=symbol,
                data_type="daily",
                duration_seconds=duration,
            )

        except Exception as e:
            logger.warning(f"TickFlow 采集失败 {symbol}: {e}")
            return CollectResult(
                success=False,
                data=None,
                source="tickflow",
                symbol=symbol,
                data_type="daily",
                error=f"TickFlow 异常: {e}",
                duration_seconds=(datetime.now(timezone(timedelta(hours=8))) - start).total_seconds(),
            )

    async def collect_batch(self, symbols, data_type="daily", **kwargs):
        """并发批量采集（单进程内串行，网络并发）"""
        tasks = [self.collect(s, data_type, **kwargs) for s in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r if not isinstance(r, Exception) else
                CollectResult(success=False, data=None, source="tickflow",
                              symbol=s, data_type="daily", error=str(r))
                for r, s in zip(results, symbols)]
