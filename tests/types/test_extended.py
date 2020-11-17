# Copyright 2014, 2015 SAP SE.
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
from pyhdb.protocol.constants import type_codes


# ########################## Test NoneType packing extention ##########################

@pytest.mark.parametrize("given,expected", [
    (type_codes.LONGDATE, b"\xbd"),
    (type_codes.SECONDTIME, b"\x40\x81\x51\x01\x00"),
])

def test_none_pack(given, expected):
    assert types.NoneType.prepare(given) == expected

# ########################## Test value unpacking #####################################

@pytest.mark.parametrize("given,expected", [
    (b"\x02\x88\x38", '00000008'),
    (b"\x05\x08\x30\x30\x30\x41", '000A'),
])
def test_unpack_alphanum(given, expected):
    given = BytesIO(given)
    assert types.Alphanum.from_resultset(given) == expected

@pytest.mark.parametrize("input,expected", [
    ("0123", "'0123'"),
    ("'", "''''"),
])
def test_escape_Alphanum(input, expected):
    assert types.Alphanum.to_sql(input) == expected

@pytest.mark.parametrize("input,expected", [
    (b'\x31\x00\x22\x19\xe3\xbd\x66\x05', datetime(1234, 5, 6, 7, 8, 9, 101112)),
    (b'\x01\x20\xd3\x65\xf2\x8c\xd8\x08', datetime(2020, 11, 18, 1, 19, 59, 123456)),
    (b'\x31\x0e\xd3\x65\xf2\x8c\xd8\x08', datetime(2020, 11, 18, 1, 19, 59, 123000)),
    (b'\x01\xc0\x0a\x49\x08\x2a\xca\x2b', None),
])
def test_unpack_longdate(input, expected):
    input = BytesIO(input)
    assert types.Longdate.from_resultset(input) == expected

@pytest.mark.parametrize("input,expected", [
    (datetime(2014, 8, 22, 8, 9, 50), "'2014-08-22 08:09:50'"),
    (datetime(1988, 3, 23, 23, 59, 50, 123000), "'1988-03-23 23:59:50.123000'"),
    (datetime(1988, 3, 23, 23, 59, 50, 123456), "'1988-03-23 23:59:50.123456'"),
])
def test_escape_longdate(input, expected):
    assert types.Longdate.to_sql(input) == expected

@pytest.mark.parametrize("input,expected", [
    (b'\x5a\xcf\x4c\xd7\x0e\x00\x00\x00', datetime(2020,11,20,21,30,1,0)),
    (b'\x81\xdb\x88\x77\x49\x00\x00\x00', None),
])
def test_unpack_seconddate(input, expected):
    input = BytesIO(input)
    assert types.Seconddate.from_resultset(input) == expected

@pytest.mark.parametrize("input,expected", [
    (datetime(2014, 8, 22, 8, 9, 50), "'2014-08-22 08:09:50'"),
    (datetime(1988, 3, 23, 23, 59, 50, 123000), "'1988-03-23 23:59:50'"),
])
def test_escape_seconddate(input, expected):
    assert types.Seconddate.to_sql(input) == expected
    
@pytest.mark.parametrize("input,expected", [
    (b'\xd7\x41\x0b\x00\x00\x00\x00\x00', date(2020,11,20)),
    (b'\xde\xb9\x37\x00\x00\x00\x00\x00', None),
])
def test_unpack_daydate(input, expected):
    input = BytesIO(input)
    assert types.Daydate.from_resultset(input) == expected

@pytest.mark.parametrize("input,expected", [
    (date(1961, 4, 12), "'1961-04-12'"),
])
def test_escape_daydate(input, expected):
    assert types.Daydate.to_sql(input) == expected

@pytest.mark.parametrize("input,expected", [
    (b'\xf1\xb0\x00\x00', time(12, 34, 56)),
    (b'\x82\x51\x01\x00', None),
])
def test_unpack_secondtime(input, expected):
    input = BytesIO(input)
    assert types.Secondtime.from_resultset(input) == expected

@pytest.mark.parametrize("input,expected", [
    (time(8, 9, 50), "'08:09:50'"),
    (time(23, 59, 59), "'23:59:59'"),
])
def test_escape_secondtime(input, expected):
    assert types.Secondtime.to_sql(input) == expected

@pytest.mark.parametrize("input,expected", [
    (date(2014,  2, 18), 735284),
    (date(1582, 10, 15), 577738),
    (date(1582, 10,  4), 577737),
    (date(1,  1,  1), 1),
])
def test_to_daydate(input, expected):
    assert types.Date.to_daydate(input) == expected

# ########################## Test value packing #####################################

@pytest.mark.parametrize("input,expected", [
    ('8', b'\x37\x01\x38')
])
def test_pack_alphanum(input, expected):
        assert types.Alphanum.prepare(input) == expected

@pytest.mark.parametrize("input,expected", [
    (datetime(1234, 5, 6, 7, 8, 9, 101112), b'\x3d\x31\x00\x22\x19\xe3\xbd\x66\x05'),
    ("1234-05-06 07:08:09.101112", b'\x3d\x31\x00\x22\x19\xe3\xbd\x66\x05'),
    ("1234-05-06 07:08:09", b'\x3d\x81\x92\x12\x19\xe3\xbd\x66\x05'),
])
def test_pack_longdate(input, expected):
        assert types.Longdate.prepare(input) == expected

@pytest.mark.parametrize("input,expected", [
    (datetime(2020,11,20,21,30,1,0), b'\x3e\x5a\xcf\x4c\xd7\x0e\x00\x00\x00'),
    ('2020-11-20 21:30:01', b'\x3e\x5a\xcf\x4c\xd7\x0e\x00\x00\x00'),
])
def test_pack_seconddate(input, expected):
        assert types.Seconddate.prepare(input) == expected

@pytest.mark.parametrize("input,expected", [
    (time(12, 34, 56), b'\x40\xf1\xb0\x00\x00'),
    ('12:34:56', b'\x40\xf1\xb0\x00\x00'),
])
def test_pack_secondtime(input, expected):
        assert types.Secondtime.prepare(input) == expected
