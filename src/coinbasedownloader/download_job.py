from typing import List
import polars as pl
from polars import DataFrame

from coinbasedownloader._typing import Interval
from coinbasedownloader.coinbase_api import CoinbaseClient
from coinbasedownloader.db_utils import get_latest_ts_per_pair, write_to_db


def to_polars_df(candles: List[dict]) -> DataFrame:
    df = pl.LazyFrame(candles, strict=False)
    df = df.explode("candles")
    fields = ["ts", "open", "high", "low", "close", "volume"]
    df = df.with_columns(pl.col("candles").list.to_struct(fields=fields))
    df = df.unnest("candles")
    df = df.cast({"ts": pl.Int64})
    df = df.cast({pl.Float64: pl.Utf8})

    return df.collect()


async def fetch_and_persist_1h_candles_updates() -> None:
    await fetch_and_persist_candles_updates(Interval.HOUR)

async def fetch_and_persist_1d_candles_updates() -> None:
    await fetch_and_persist_candles_updates(Interval.DAY)

async def fetch_and_persist_candles_updates(interval: Interval) -> None:
    latest_updates = get_latest_ts_per_pair(interval)

    async with CoinbaseClient() as client:
        if len(latest_updates) == 0:
            candles: List[dict | Exception] = await client.fetch_all_active_candles()

            errors: List[Exception] = [result for result in candles if isinstance(result, Exception)]
            if len(errors) > 0:
                print(len(errors))
                print(errors)

            candles: List[dict] = [result for result in candles if not isinstance(result, Exception)]

            df: DataFrame = to_polars_df(candles)
            write_to_db(df, interval)
        else:
            pairs: List[str] = await client.fetch_active_trading_pairs()
