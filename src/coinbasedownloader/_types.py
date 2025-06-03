from dataclasses import dataclass
from datetime import timedelta, datetime
from enum import Enum


# Coinbase granularity field must be one of the following "second" values: 60, 300, 900, 3600, 21600, 86400
class CandlePeriod(Enum):
    granularity: int
    delta: timedelta

    def __new__(cls, granularity: int, delta: timedelta):
        obj = object.__new__(cls)
        obj.granularity = granularity
        obj.delta = delta
        return obj

    ONE_MIN = (60, timedelta(minutes=300))
    FIVE_MINS = (300, timedelta(minutes=1500))
    FIFTEEN_MINS = (900, timedelta(minutes=4500))
    ONE_HOUR = (3600, timedelta(hours=300))
    SIX_HOURS = (21600, timedelta(hours=1800))
    ONE_DAY = (86400, timedelta(days=300))


@dataclass
class Range:
    pair: str             # coin name
    period: CandlePeriod  # time interval
    from_dt: datetime     # from (timestamp)
    to_dt: datetime       # to (timestamp)