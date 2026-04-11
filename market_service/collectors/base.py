#!/usr/bin/env python3
"""
采集器抽象基类
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
import asyncio
import logging
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class CollectResult:
    """采集结果"""
    success: bool
    data: Optional[Dict[str, Any] | List[Dict[str, Any]]]
    source: str
    symbol: Optional[str]
    data_type: str
    error: Optional[str] = None
    duration_seconds: float = 0.0

class BaseCollector(ABC):
    """采集器基类"""

    def __init__(self, rate_limit_per_minute: int = 30):
        self.rate_limit = rate_limit_per_minute
        self.tokens = rate_limit_per_minute
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def _acquire_token(self):
        """令牌桶限流"""
        async with self._lock:
            now = time.time()
            # 补充令牌
            delta_minutes = (now - self.last_refill) / 60.0
            self.tokens = min(self.rate_limit, self.tokens + delta_minutes * self.rate_limit)
            self.last_refill = now

            if self.tokens < 1:
                wait_seconds = (1 - self.tokens) / self.rate_limit * 60
                logger.debug(f"限流等待 {wait_seconds:.2f}s")
                await asyncio.sleep(wait_seconds)
                self.tokens = 0
            else:
                self.tokens -= 1

    @abstractmethod
    async def collect(self, symbol: str, data_type: str, **kwargs) -> CollectResult:
        """采集单条数据"""
        pass

    @abstractmethod
    async def collect_batch(self, symbols: List[str], data_type: str, **kwargs) -> List[CollectResult]:
        """批量采集"""
        pass

    async def collect_with_retry(
        self,
        symbol: str,
        data_type: str,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        initial_delay: float = 1.0,
        **kwargs
    ) -> CollectResult:
        """带重试的采集"""
        last_error = None
        for attempt in range(max_retries):
            try:
                await self._acquire_token()
                result = await self.collect(symbol, data_type, **kwargs)
                if result.success or attempt == max_retries - 1:
                    return result
                # 如果失败但可重试，记录错误继续
                last_error = result.error
            except Exception as e:
                last_error = str(e)
                if attempt < max_retries - 1:
                    delay = initial_delay * (backoff_factor ** attempt)
                    logger.warning(f"采集失败，{delay:.1f}s后重试 ({attempt+1}/{max_retries}): {e}")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"采集最终失败 {symbol}/{data_type}: {e}")

        return CollectResult(
            success=False,
            data=None,
            source=self.__class__.__name__,
            symbol=symbol,
            data_type=data_type,
            error=last_error
        )
