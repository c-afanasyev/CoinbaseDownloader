import asyncio
import logging
from datetime import datetime, timezone
from time import time

from coinbasedownloader.db_utils import init_db
from coinbasedownloader.service import sync_all_active_updates, fetch_all_active_from_to
from coinbasedownloader._types import CandlePeriod

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    init_db()
    start = time()
    # from_dt: datetime = datetime.fromisoformat("2015-01-01 00:00:00+00:00")
    # to_dt: datetime = datetime.now(timezone.utc)
    # await fetch_all_active_from_to(from_dt, to_dt, CandlePeriod.ONE_DAY)
    #
    # await asyncio.sleep(2)
    #
    # from_dt: datetime = datetime.fromisoformat("2020-00-01 00:00:00+00:00")
    # to_dt: datetime = datetime.now(timezone.utc)
    # await fetch_all_active_from_to(from_dt, to_dt, CandlePeriod.ONE_HOUR)

    await sync_all_active_updates(CandlePeriod.ONE_DAY)
    print(f"Time taken to sync ONE_DAY: {time() - start:.2f} seconds")
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