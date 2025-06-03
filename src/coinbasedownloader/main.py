import asyncio
import logging

from coinbasedownloader.coinbase_api import CoinbaseClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main() -> None:
    async with CoinbaseClient() as client:
        candles = await client.fetch_all_active_candles()
        print(candles)


if __name__ == "__main__":
    asyncio.run(main())