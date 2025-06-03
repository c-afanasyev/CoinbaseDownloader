from datetime import datetime, timedelta
from coinbasedownloader.service import  split_datetime_range
from coinbasedownloader._types import CandlePeriod
import pytest

@pytest.fixture
def date_range():
    start = datetime(2025, 1, 1, 0, 0)
    end = datetime(2025, 1, 15, 0, 0)
    return start, end

def test_hours_split(date_range):
    start, end = date_range
    ranges = split_datetime_range(start, end, CandlePeriod.ONE_HOUR)
    assert len(ranges) == 2
    assert ranges[0][0] == start
    assert ranges[0][1] == start + timedelta(hours=300)
    assert ranges[1][0] == start + timedelta(hours=300)
    assert ranges[1][1] == end

def test_minutes_split(date_range):
    start, end = date_range
    ranges = split_datetime_range(start, end, CandlePeriod.ONE_MIN)
    assert len(ranges) == 68
    assert ranges[0][0] == start
    assert ranges[0][1] == start + timedelta(minutes=300)
    assert ranges[-1][1] == end

def test_days_split(date_range):
    start, end = date_range
    ranges = split_datetime_range(start, end, CandlePeriod.ONE_DAY)
    assert len(ranges) == 1
    assert ranges[0][0] == start
    assert ranges[0][1] == end

def test_invalid_unit():
    start = datetime(2025, 1, 1)
    end = datetime(2025, 1, 2)
    with pytest.raises(AttributeError):
        split_datetime_range(start, end, "INVALID")

def test_empty_range():
    start = datetime(2025, 1, 1)
    end = start
    ranges = split_datetime_range(start, end, CandlePeriod.ONE_HOUR)
    assert ranges == []

def test_small_range():
    start = datetime(2025, 1, 1)
    end = start + timedelta(hours=100)
    ranges = split_datetime_range(start, end, CandlePeriod.ONE_HOUR)
    assert len(ranges) == 1
    assert ranges[0] == (start, end)
