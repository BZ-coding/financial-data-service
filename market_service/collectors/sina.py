#!/usr/bin/env python3
"""
Sina财经数据采集器
通过新浪财经 hq.sinajs.cn 获取 A 股实时行情
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional

import requests

from .base import BaseCollector, CollectResult

TZ = timezone(timedelta(hours=8))

def now_tz():
    return datetime.now(TZ)

logger = logging.getLogger(__name__)


class SinaCollector(BaseCollector):
    """通过新浪财经获取 A 股实时行情"""

    def __init__(self, rate_limit_per_minute: int = 60):
        super().__init__(rate_limit_per_minute=rate_limit_per_minute)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://finance.sina.com.cn',
        })

    async def collect(self, symbol: str, data_type: str = 'price', **kwargs) -> CollectResult:
        if data_type != 'price':
            return CollectResult(
                success=False,
                data=None,
                source='sina',
                symbol=symbol,
                data_type=data_type,
                error=f"SinaCollector 仅支持 price，当前请求: {data_type}"
            )

        loop = asyncio.get_event_loop()
        try:
            raw = await loop.run_in_executor(None, self._fetch_sync, symbol)
        except Exception as e:
            logger.error(f"SinaCollector.collect({symbol}) 异常: {e}")
            return CollectResult(
                success=False, data=None, source='sina',
                symbol=symbol, data_type=data_type, error=str(e)
            )

        if raw is None:
            return CollectResult(
                success=False, data=None, source='sina',
                symbol=symbol, data_type=data_type, error="行情数据不可用"
            )

        return CollectResult(
            success=True, data=raw, source='sina',
            symbol=symbol, data_type=data_type
        )

    def _fetch_sync(self, symbol: str) -> Optional[Dict[str, Any]]:
        """同步从新浪抓数据"""
        try:
            prefix = 'sh' if symbol.startswith('6') else 'sz'
            url = f'https://hq.sinajs.cn/list={prefix}{symbol}'
            resp = self.session.get(url, timeout=10)
            if resp.status_code != 200:
                logger.warning(f"SinaCollector({symbol}) HTTP {resp.status_code}")
                return None

            text = resp.text.strip()
            if '=' not in text or len(text) < 50:
                logger.warning(f"SinaCollector({symbol}) 响应异常: {text[:100]}")
                return None

            data_str = text.split('=', 1)[1].strip('"')
            fields = data_str.split(',')
            if len(fields) < 10:
                logger.warning(f"SinaCollector({symbol}) 字段不足: {len(fields)}")
                return None

            name = fields[0]
            price = float(fields[3]) if fields[3] else 0.0
            open_px = float(fields[1]) if fields[1] else 0.0
            prev_close = float(fields[2]) if fields[2] else 0.0
            high = float(fields[4]) if fields[4] else 0.0
            low = float(fields[5]) if fields[5] else 0.0
            volume = float(fields[8]) if len(fields) > 8 and fields[8] else 0.0  # 股数
            amount = float(fields[9]) if len(fields) > 9 and fields[9] else 0.0   # 成交额

            if price == 0:
                logger.warning(f"SinaCollector({symbol}) price=0，数据无效")
                return None

            trade_time = now_tz().isoformat()
            return {
                'source': 'sina',
                'symbol': symbol,
                'trade_time': trade_time,
                'price': price,
                'open': open_px,
                'prev_close': prev_close,
                'high': high,
                'low': low,
                'volume': volume,
                'amount': amount,
                'timestamp': trade_time,
            }
        except Exception as e:
            logger.error(f"SinaCollector._fetch_sync({symbol}) 异常: {e}")
            return None

    async def collect_batch(self, symbols: List[str], data_type: str = 'price', **kwargs) -> List[CollectResult]:
        results = []
        for symbol in symbols:
            result = await self.collect(symbol, data_type=data_type, **kwargs)
            results.append(result)
        return results
