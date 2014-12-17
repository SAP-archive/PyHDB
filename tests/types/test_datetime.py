# Copyright 2014 SAP SE.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http: //www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

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
