from io import BytesIO
from datetime import datetime, time, date

import pytest
from pyhdb.protocol import types

@pytest.mark.parametrize("input,expected", [
    (b'\x90\x0C\xD8\x59', time(16, 12, 23)),
    (b"\x00\x00\x00\x00", None),
])
def test_unpack_time(input, expected):
    input = BytesIO(input)
    assert types.Time.from_resultset(input) == expected

@pytest.mark.parametrize("input,expected", [
    (time(8, 9, 50), "'08:09:50'"),
    (time(23, 59, 59), "'23:59:59'"),
])
def test_escape_date(input, expected):
    assert types.Time.to_sql(input) == expected

@pytest.mark.parametrize("input,expected", [
    (b'\xDE\x87\x07\x16', date(2014, 8, 22)),
    (b"\x00\x00\x00\x00", None),
])
def test_unpack_date(input, expected):
    input = BytesIO(input)
    assert types.Date.from_resultset(input) == expected

@pytest.mark.parametrize("input,expected", [
    (date(2014, 8, 22), "'2014-08-22'"),
    (date(1988, 3, 23), "'1988-03-23'"),
])
def test_escape_date(input, expected):
    assert types.Date.to_sql(input) == expected

@pytest.mark.parametrize("input,expected", [
    (b'\xDE\x87\x07\x19\x89\x2F\xC8\x01',
        datetime(2014, 8, 25, 9, 47, 0, 456000)),
    (b"\x00\x00\x00\x00\x00\x00\x00\x00", None),
])
def test_unpack_timestamp(input, expected):
    input = BytesIO(input)
    assert types.Timestamp.from_resultset(input) == expected

@pytest.mark.parametrize("input,expected", [
    (datetime(2014, 8, 22, 8, 9, 50), "'2014-08-22 08:09:50.0'"),
    (datetime(1988, 3, 23, 23, 59, 50, 123), "'1988-03-23 23:59:50.123'"),
])
def test_escape_timestamp(input, expected):
    assert types.Timestamp.to_sql(input) == expected
