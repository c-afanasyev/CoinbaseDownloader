import sqlite3
from polars import DataFrame

from coinbasedownloader._typing import Interval

DB_NAME = "coinbase_ohlc.sqlite3"
ONE_HOUR_TABLE = "hour"
DAILY_TABLE = "day"


def map_table_name(interval: Interval) -> str:
    match interval:
        case Interval.HOUR:
            return ONE_HOUR_TABLE
        case Interval.DAY:
            return DAILY_TABLE


def write_to_db(df: DataFrame, interval: Interval) -> None:
    with sqlite3.connect(DB_NAME) as conn:
        df.write_database(table_name=map_table_name(interval), connection="sqlite:///coinbase_ohlc.sqlite3", if_table_exists="append")


def get_latest_ts_per_pair(interval: Interval) -> list:
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()

        cursor.execute(f"""
            SELECT pair_name, MAX(ts) as max_ts
            FROM {map_table_name(interval)}
            GROUP BY pair_name
        """)

        return cursor.fetchall()


def init_db() -> None:
    # Connect to database (creates crypto_ohlc_1h.sqlite3 if it doesn't exist)
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()

        def new_table_query(table_name: str) -> None:
            cursor.executescript(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pair_name TEXT NOT NULL,
                    ts INTEGER NOT NULL,
                    open TEXT NOT NULL,
                    high TEXT NOT NULL,
                    low TEXT NOT NULL,
                    close TEXT NOT NULL,
                    volume TEXT NOT NULL
                );
        
                CREATE INDEX IF NOT EXISTS idx_pair_name_{table_name}_ts ON {table_name}(pair_name, ts);
            """)

        new_table_query(ONE_HOUR_TABLE)
        new_table_query(DAILY_TABLE)

        conn.commit()