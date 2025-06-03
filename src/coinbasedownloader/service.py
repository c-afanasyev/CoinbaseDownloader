from datetime import datetime, timedelta, timezone
from operator import itemgetter
from typing import List, Tuple

import pandas as pd
import polars as pl
from jsonschema.validators import extend
from polars import DataFrame

from coinbasedownloader._types import CandlePeriod, Range
from coinbasedownloader.api import CoinbaseClient
from coinbasedownloader.db_utils import get_latest_ts_per_pair, write_to_db


def to_polars_df(candles: List[dict]) -> DataFrame:
    df = pl.LazyFrame(candles, strict=False)
    df = df.explode("candles")
    fields = ["ts", "low", "high", "open", "close", "volume"]
    df = df.with_columns(pl.col("candles").list.to_struct(fields=fields))
    df = df.unnest("candles")
    df = df.cast({"ts": pl.Int64})

    return df.collect()


async def fetch_all_active_from_to(from_dt: datetime, to_dt: datetime, period: CandlePeriod):
    to_dt = truncate_last_candle(to_dt, period)

    async with CoinbaseClient() as client:
        pairs: dict = await client.pairs_meta()
        not_delisted_pairs: List[str] = [pair["id"] for pair in pairs if not pair["status"] == "delisted"]
        split_datetime: List[Tuple[datetime, datetime]] = split_datetime_range(from_dt, to_dt, period)

        ranges: List[Range] = []

        for from_dt, to_dt in split_datetime:
            for pair in not_delisted_pairs:
                ranges.append(Range(pair, period, from_dt, to_dt))

        candles: List[dict | Exception] = await client.pairs(ranges)

        await persist_candles(candles, period)


async def sync_all_active_updates(period: CandlePeriod) -> None:
    async with (CoinbaseClient() as client):
        pairs_meta: dict = await client.pairs_meta()
        not_delisted_pairs: List[str] = [pair["id"] for pair in pairs_meta if not pair["status"] == "delisted"]
        delisted_pairs: List[str] = [pair["id"] for pair in pairs_meta if pair["status"] == "delisted"]
        latest_updates: List[Tuple[str, int]] = get_latest_ts_per_pair(period=period, include=not_delisted_pairs, exclude=delisted_pairs)

        if len(latest_updates) == 0:
            print("nothing to sync")
            return  # nothing to sync

        to_dt: datetime = datetime.now(timezone.utc)
        to_dt: datetime = truncate_last_candle(to_dt, period)


        pairs_to_sync: List[Tuple[str, datetime, datetime]] = \
            [(pair, datetime.fromtimestamp(last, tz=timezone.utc) + timedelta(seconds=period.granularity), to_dt) for pair, last in latest_updates]

        new_pairs = list(set(not_delisted_pairs) - set(map(itemgetter('id'), pairs_meta)))

        if len(new_pairs) > 0:
            from_dt: datetime = pairs_to_sync[0][1]
            extend(pairs_to_sync, [(pair, from_dt, to_dt) for pair in new_pairs])

        td = timedelta(seconds=period.granularity) - timedelta(microseconds=1)
        pairs_to_sync = [pair for pair in pairs_to_sync if (pair[2]-pair[1]) >= td]

        if len(pairs_to_sync) == 0:
            print("nothing to sync")
            return  # nothing to sync

        split_pairs_to_sync: List[Tuple[str, datetime, datetime]] = []

        for pair, from_dt, to_dt in pairs_to_sync:
            split_dts: List[Tuple[datetime,datetime]] = split_datetime_range(from_dt, to_dt, period)

            for split_from_dt, split_to_dt in split_dts:
                split_pairs_to_sync.append((pair, split_from_dt, split_to_dt))

        ranges: List[Range] = [Range(pair, period, from_dt, to_dt) for pair, from_dt, to_dt in split_pairs_to_sync]

        candles: List[dict | Exception] = await client.pairs(ranges)

        await persist_candles(candles, period)


async def persist_candles(candles: List[dict | Exception], period: CandlePeriod) -> None:
    errors: List[Exception] = [result for result in candles if isinstance(result, Exception)]
    if len(errors) > 0:
        print(f"number of errors: {len(errors)}")
        print(errors)

    candles: List[dict] = [result for result in candles if not isinstance(result, Exception) and len(result["candles"]) > 0]

    df: DataFrame = to_polars_df(candles)

    write_to_db(df, period)

# response must contain as many as 300 candles
# so split time ranges so each request consists of not more than 300 candles
def split_datetime_range(from_dt: datetime, to_dt: datetime, period: CandlePeriod) -> List[Tuple[datetime, datetime]]:
    delta: timedelta = period.delta

    # Split into ranges
    ranges: List[Tuple[datetime, datetime]] = []
    current_dt: datetime = from_dt

    while current_dt < to_dt:
        next_dt: datetime = min(current_dt + delta, to_dt)
        ranges.append((current_dt, next_dt - timedelta(microseconds=1)))
        current_dt = next_dt

    return ranges

# truncate end datetime to not fetch the last (not closed yet) candle
def truncate_last_candle(to_dt: datetime, period: CandlePeriod) -> datetime:
    match period:
        case period.ONE_MIN:
            return pd.Timestamp(to_dt).floor('1min').to_pydatetime() - timedelta(microseconds=1)
        case period.FIVE_MINS:
            return pd.Timestamp(to_dt).floor('5min').to_pydatetime() - timedelta(microseconds=1)
        case period.FIFTEEN_MINS:
            return pd.Timestamp(to_dt).floor('15min').to_pydatetime() - timedelta(microseconds=1)
        case period.ONE_HOUR:
            return pd.Timestamp(to_dt).floor('h').to_pydatetime() - timedelta(microseconds=1)
        case period.SIX_HOURS:
            return pd.Timestamp(to_dt).floor('6h').to_pydatetime() - timedelta(microseconds=1)
        case period.ONE_DAY:
            return pd.Timestamp(to_dt).floor('24h').to_pydatetime() - timedelta(microseconds=1)

    raise ValueError(f"period {period} not supported")