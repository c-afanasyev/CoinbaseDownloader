import asyncio
import logging
import aiohttp
from typing import List

from aiolimiter import AsyncLimiter
from tenacity import retry, wait_fixed, before_sleep_log, stop_after_attempt

from coinbasedownloader._types import Range

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


class CoinbaseClient:
    def __init__(self, timeout=aiohttp.ClientTimeout(total=3)):
        self._session = aiohttp.ClientSession(timeout=timeout, connector=aiohttp.TCPConnector(limit=100))
        self._rate_limit = AsyncLimiter(9, 1)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self._session.close()

    # fetch metadata of all trading pairs
    async def pairs_meta(self) -> List[dict]:
        async with self._rate_limit:
            url: str = f"https://api.exchange.coinbase.com/products"
            async with self._session.get(url) as resp:
                resp.raise_for_status()
                pairs = await resp.json()
                return pairs

    @retry(
        reraise=True,
        wait=wait_fixed(3),
        stop=stop_after_attempt(3),
        before_sleep=before_sleep_log(logger, logging.ERROR)
    )
    async def pair(self, rng: Range) -> dict | Exception:
        async with self._rate_limit:
            url: str = f"https://api.exchange.coinbase.com/products/{rng.pair}/candles"
            params: dict = {
                "granularity": rng.period.granularity,
                "start": int(rng.from_dt.timestamp()),
                "end": int(rng.to_dt.timestamp())
            }

            async with self._session.get(url, params=params) as resp:
                resp.raise_for_status()
                result = await resp.json()
                return {
                    "pair_name": rng.pair,
                    "candles": result,
                }

    # fetch specified range of candles for specified pair names
    async def pairs(self, ranges: List[Range]) -> List[dict | Exception]:
        tasks = [self.pair(rng) for rng in ranges]
        return await asyncio.gather(*tasks, return_exceptions=True)