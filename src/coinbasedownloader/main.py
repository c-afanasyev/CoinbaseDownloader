import asyncio
import logging
from time import time

from coinbasedownloader.db_utils import init_db
from coinbasedownloader.download_job import fetch_and_persist_1h_candles_updates

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main() -> None:
    init_db()
    start = time()
    await fetch_and_persist_1h_candles_updates()
    print(f"Time taken: {time() - start:.2f} seconds")
    # aiocron.crontab('0 * * * *', func=fetch_and_persist_1h_candles_updates())  # every hour at minute 0
    # aiocron.crontab('0 * * * *', func=fetch_and_persist_1d_candles_updates())  # every hour at minute 0
    # asyncio.get_event_loop().run_forever()
    # init_db()
    # ts = get_latest_ts_per_pair(Interval.HOUR)
    # print(ts)
    # async with CoinbaseClient() as client:
    #     candles: List[dict] = await client.fetch_all_active_candles()
    #     print(candles)



if __name__ == "__main__":
    asyncio.run(main())