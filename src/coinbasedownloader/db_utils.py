import sqlite3
from typing import Tuple, List

from polars import DataFrame

from coinbasedownloader._types import CandlePeriod

DB_NAME = "coinbase_ohlc.sqlite3"
ONE_HOUR_TABLE = "hour"
DAILY_TABLE = "day"


def map_table_name(period: CandlePeriod) -> str:
    match period:
        case CandlePeriod.ONE_HOUR:
            return ONE_HOUR_TABLE
        case CandlePeriod.ONE_DAY:
            return DAILY_TABLE


def write_to_db(df: DataFrame, period: CandlePeriod) -> None:
    df.write_database(
        table_name=map_table_name(period),
        connection="sqlite:///coinbase_ohlc.sqlite3",
        if_table_exists="append")


def get_latest_ts_per_pair(period: CandlePeriod, include: List[str] = None, exclude: List[str] = None) -> List[Tuple[str, int]]:
    with (sqlite3.connect(DB_NAME) as conn):
        cursor = conn.cursor()

        select_clause = \
        f"SELECT pair_name, MAX(ts) as max_ts FROM {map_table_name(period)}"

        where_clause = ""

        if include and len(include) > 0:
            included = ",".join(f"'{x}'" for x in include)
            where_clause += f" WHERE pair_name IN ({included})"

        if exclude and len(exclude) > 0:
            excluded = ",".join(f"'{x}'" for x in exclude)
            if where_clause != "":
                where_clause += " AND"
            else: where_clause += " WHERE"

            where_clause += f" pair_name NOT IN ({excluded})"

        group_clause = f" GROUP BY pair_name"

        cursor.execute(select_clause + where_clause + group_clause)

        return cursor.fetchall()


def init_db() -> None:
    # Connect to database (creates coinbase_ohlc.sqlite3 if it doesn't exist)
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()

        def new_table_query(table_name: str) -> None:
            cursor.executescript(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pair_name TEXT NOT NULL,
                    ts INTEGER NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL
                );
        
                CREATE INDEX IF NOT EXISTS idx_pair_name_{table_name}_ts ON {table_name}(pair_name, ts);
            """)

        new_table_query(ONE_HOUR_TABLE)
        new_table_query(DAILY_TABLE)

        conn.commit()