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
from decimal import Decimal, getcontext
getcontext().prec = 36

import pytest
from pyhdb.protocol import types


# ########################## Test value unpacking #####################################

@pytest.mark.parametrize("input,expected", [
    (b"\x01\x15\xCD\x5B\x07", 123456789),
    (b"\x01\xB1\x68\xDE\x3A", 987654321),
    (b"\x00", None),
])
def test_unpack_int(input, expected):
    input = BytesIO(input)
    assert types.Int.from_resultset(input) == expected


@pytest.mark.parametrize("input,expected", [
    (123456789, '123456789'),
    (987654321, '987654321'),
])
def test_escape_int(input, expected):
    assert types.Int.to_sql(input) == expected


@pytest.mark.parametrize("input,expected", [
    (b"\x01\x00", 0),
    (b"\x01\x01", 1),
    (b"\x01\x7B", 123),
    (b"\x01\xFF", 255),
    (b"\x00", None),
])
def test_unpack_tinyint(input, expected):
    input = BytesIO(input)
    assert types.TinyInt.from_resultset(input) == expected


@pytest.mark.parametrize("input,expected", [
    (b"\x01\x39\x30", 12345),
    (b"\x01\xC7\xCF", -12345),
    (b"\x00", None),
])
def test_unpack_smallint(input, expected):
    input = BytesIO(input)
    assert types.SmallInt.from_resultset(input) == expected


@pytest.mark.parametrize("input,expected", [
    (b"\x01\x00\x00\x00\x00\x01\x00\x00\x00", 2**32),
    (b"\x01\x00\x00\x00\x00\xFF\xFF\xFF\xFF", -2**32),
    (b"\x00", None),
])
def test_unpack_bigint(input, expected):
    input = BytesIO(input)
    assert types.BigInt.from_resultset(input) == expected


@pytest.mark.parametrize("input,expected", [
    (b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x70",
     None),
    (b"\x56\xE6\x47\x6F\x0E\xDE\xD6\x93\x68\xB0\x26\x78\xFB\x99\x1A\xB0",
     Decimal('-312313212312321.1245678910111213142')),
    (b"\x4F\xF6\x59\x25\x49\x00\x00\x00\x00\x00\x00\x00\x00\x00\x2A\x30",
     Decimal('3.14159265359')),
])
def test_unpack_decimal(input, expected):
    input = BytesIO(input)
    assert types.Decimal.from_resultset(input) == expected

@pytest.mark.parametrize("input,expected", [
    (Decimal('3.14159265359'), '3.14159265359'),
    (Decimal('-312313212312321.1245678910111213142'),
     '-312313212312321.1245678910111213142'),
])
def test_escape_decimal(input, expected):
    assert types.Decimal.to_sql(input) == expected


@pytest.mark.parametrize("input,expected", [
    (b"\x85\xEB\x21\x41", float("10.119999885559082")),
    (b"\x85\xEB\x21\xC1", float("-10.119999885559082")),
    (b"\xFF\xFF\xFF\xFF", None),
])
def test_unpack_real(input, expected):
    input = BytesIO(input)
    assert types.Real.from_resultset(input) == expected


@pytest.mark.parametrize("input,expected", [
    (b"\x3D\x0A\xD7\xA3\x70\x3D\x24\x40", float("10.12")),
    (b"\x3D\x0A\xD7\xA3\x70\x3D\x24\xC0", float("-10.12")),
    (b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", None),
])
def test_unpack_double(input, expected):
    input = BytesIO(input)
    assert types.Double.from_resultset(input) == expected


# ########################## Test value packing #####################################

@pytest.mark.parametrize("input,expected", [
    (123456789, b"\x01\x15\xCD\x5B\x07"),
    ('123456789', b"\x01\x15\xCD\x5B\x07"),    # also ensure that strings are accepted
    (u'123456789', b"\x01\x15\xCD\x5B\x07"),   # also ensure that unicodes are accepted
    (987654321, b"\x01\xB1\x68\xDE\x3A"),
    (None, b"\x00"),
])
def test_pack_int(input, expected):
    types.Int.prepare(input) == expected


@pytest.mark.parametrize("input,expected", [
    (0, b"\x01\x00"),
    (1, b"\x01\x01"),
    (123, b"\x01\x7B"),
    ('123', b"\x01\x7B"),
    (u'123', b"\x01\x7B"),
    (255, b"\x01\xFF"),
    (None, b"\x00"),
])
def test_pack_tinyint(input, expected):
    types.TinyInt.prepare(input) == expected


@pytest.mark.parametrize("input,expected", [
    (12345, b"\x01\x39\x30"),
    (-12345, b"\x01\xC7\xCF"),
    ('-12345', b"\x01\xC7\xCF"),
    (u'-12345', b"\x01\xC7\xCF"),
    (None, b"\x00"),
])
def test_pack_smallint(input, expected):
    types.SmallInt.prepare(input) == expected


@pytest.mark.parametrize("input,expected", [
    (2**32, b"\x01\x00\x00\x00\x00\x01\x00\x00\x00"),
    (-2**32, b"\x01\x00\x00\x00\x00\xFF\xFF\xFF\xFF"),
    ('4294967296', b"\x01\x00\x00\x00\x00\xFF\xFF\xFF\xFF"),
    (u'4294967296', b"\x01\x00\x00\x00\x00\xFF\xFF\xFF\xFF"),
    (None, b"\x00"),
])
def test_pack_bigint(input, expected):
    types.BigInt.prepare(input) == expected


@pytest.mark.parametrize("input,expected", [
    (float("10.119999885559082"), b"\x85\xEB\x21\x41"),
    (float("-10.119999885559082"), b"\x85\xEB\x21\xC1"),
    ("-10.119999885559082", b"\x85\xEB\x21\xC1"),
    (u"-10.119999885559082", b"\x85\xEB\x21\xC1"),
    (None, b"\xFF\xFF\xFF\xFF"),
])
def test_pack_real(input, expected):
    types.Real.prepare(input) == expected


@pytest.mark.parametrize("input,expected", [
    (float("10.12"), b"\x3D\x0A\xD7\xA3\x70\x3D\x24\x40"),
    (float("-10.12"), b"\x3D\x0A\xD7\xA3\x70\x3D\x24\xC0"),
    (None, b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"),
])
def test_pack_double(input, expected):
    types.Double.prepare(input) == expected

@pytest.mark.parametrize("input,expected", [
    (None, b"\x00"),
    (Decimal('-312313212312321.1245678910111213142'),
     b"\x05\x56\xe6\x47\x6f\x0e\xde\xd6\x93\x68\xb0\x26\x78\xfb\x99\x1a\xb0"),
    (Decimal('3.14159265359'),
     b"\x05\x4f\xf6\x59\x25\x49\x00\x00\x00\x00\x00\x00\x00\x00\x00\x2a\x30"),
    (Decimal("-15.756299999999999528199623455293476581573486328125"),
     b"\x05\x03\xa0\x1d\x27\x14\xb4\xd7\xc3\x92\x8e\x1c\x3f\xaf\x4d\x00\xb0")
])
def test_pack_decimal(input, expected):
    assert types.Decimal.prepare(input) == expected
