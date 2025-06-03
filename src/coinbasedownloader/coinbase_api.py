import asyncio
import aiohttp

from typing import List, Any, TypeAlias
from tenacity import retry, stop_after_attempt, stop_before_delay, wait_fixed

CandleResult: TypeAlias = tuple[str, List[List[Any]]]
PairsResult: TypeAlias = List[dict[str, Any]]

class CoinbaseClient:
    def __init__(self, timeout=aiohttp.ClientTimeout(total=3)):
        self._session = aiohttp.ClientSession(timeout=timeout, connector=aiohttp.TCPConnector(limit=1000))

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self._session.close()

    @retry(reraise=True, wait=wait_fixed(3), stop=stop_after_attempt(3))
    async def fetch_known_trading_pairs(self) -> PairsResult:
        url: str = f"https://api.exchange.coinbase.com/products"
        async with self._session.get(url) as resp:
            resp.raise_for_status()
            return await resp.json()

    @retry(reraise=True, wait=wait_fixed(3), stop=stop_after_attempt(3))
    async def fetch_candle(self, pair: str) -> CandleResult:
        url: str = f"https://api.exchange.coinbase.com/products/{pair}/candles"
        async with self._session.get(url) as resp:
            resp.raise_for_status()
            return pair, await resp.json()

    async def fetch_candles(self, pairs: List[str]) -> List[CandleResult | BaseException]:
        tasks = [self.fetch_candle(pair) for pair in pairs]
        return await asyncio.gather(*tasks, return_exceptions=True)


    async def fetch_all_active_candles(self) -> List[CandleResult | BaseException]:
        pairs: PairsResult = await self.fetch_known_trading_pairs()
        pairs: List[str] = [pair["id"] for pair in pairs if not pair["status"] == "delisted"]
        return await self.fetch_candles(pairs)