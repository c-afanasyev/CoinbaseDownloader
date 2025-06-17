import asyncio
import logging
import aiohttp
from typing import List
from tenacity import retry, wait_fixed, stop_before_delay, before_sleep_log
from tenacity.asyncio import retry_if_exception

from coinbasedownloader._typing import Interval
from coinbasedownloader.utils import is_not_429

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


class CoinbaseClient:
    def __init__(self, timeout=aiohttp.ClientTimeout(total=3)):
        self._session = aiohttp.ClientSession(timeout=timeout, connector=aiohttp.TCPConnector(limit=500))

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self._session.close()

    async def fetch_active_trading_pairs(self) -> List[str]:
        url: str = f"https://api.exchange.coinbase.com/products"
        async with self._session.get(url) as resp:
            resp.raise_for_status()
            pairs = await resp.json()
            return [pair["id"] for pair in pairs if not pair["status"] == "delisted"]

    @retry(
        retry=retry_if_exception(is_not_429),
        reraise=True,
        wait=wait_fixed(10),
        stop=stop_before_delay(60),
        before_sleep=before_sleep_log(logger, logging.ERROR)
    )
    async def fetch_candle(self, pair: str, interval: Interval = Interval.HOUR, from_ts: int = None) -> dict:
        url: str = f"https://api.exchange.coinbase.com/products/{pair}/candles"
        params: dict = {"granularity": interval.value}
        if from_ts is not None:
            params["start"] = from_ts

        async with self._session.get(url, params=params) as resp:
            resp.raise_for_status()
            result = await resp.json()
            return {
                "pair_name": pair,
                "candles": result,
            }

    async def fetch_candles(self, pairs: List[str], interval: Interval = Interval.HOUR, from_ts: int = None) -> List[dict | Exception]:
        # tasks = [self.fetch_candle(pair, interval, from_ts) for pair in pairs]
        tasks = []
        for pair in pairs:
            await asyncio.sleep(0.01)  # 10ms delay
            tasks.append(self.fetch_candle(pair, interval, from_ts))

        return await asyncio.gather(*tasks, return_exceptions=True)

    async def fetch_all_active_candles(self, interval: Interval = Interval.HOUR, from_ts: int = None) -> List[dict | Exception]:
        pairs: List[str] = await self.fetch_active_trading_pairs()
        return await self.fetch_candles(pairs, interval, from_ts)