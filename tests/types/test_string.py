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

import os
import random
from io import BytesIO
import string
###
import pytest
###
from pyhdb.protocol import types
from pyhdb.exceptions import InterfaceError
from pyhdb.compat import byte_type, iter_range


# ########################## Test value unpacking #####################################

@pytest.mark.parametrize("given,expected", [
    (b"\xFF", None),
    (b"\x0B\x48\x65\x6C\x6C\x6F\x20\x57\x6F\x72\x6C\x64", "Hello World"),
])
def test_unpack_string(given, expected):
    given = BytesIO(given)
    assert types.String.from_resultset(given) == expected


def test_unpack_long_string():
    text = u'%030x' % random.randrange(16**300)
    given = BytesIO(b"\xF6\x2C\x01" + text.encode('cesu-8'))
    assert types.String.from_resultset(given) == text


def test_unpack_very_long_string():
    text = u'%030x' % random.randrange(16**30000)
    given = BytesIO(b"\xF7\x30\x75\x00\x00" + text.encode('cesu-8'))
    assert types.String.from_resultset(given) == text


def test_unpack_invalid_string_length_indicator():
    # The string length indicator 254 is not definied
    with pytest.raises(InterfaceError):
        types.String.from_resultset(BytesIO(b"\xFE"))


@pytest.mark.parametrize("given,expected", [
    (b"\xFF", None),
    (
        b"\x0B\xFF\x00\xFF\x00\xFF\x00\xFF\x00\xFF\x00\xFF",
        byte_type(b"\xFF\x00\xFF\x00\xFF\x00\xFF\x00\xFF\x00\xFF")
    ),
])
def test_unpack_binary(given, expected):
    given = BytesIO(given)
    assert types.Binary.from_resultset(given) == expected


def test_unpack_long_binary():
    binary_data = os.urandom(300)
    given = BytesIO(b"\xF6\x2C\x01" + binary_data)
    assert types.Binary.from_resultset(given) == binary_data


def test_unpack_very_long_binary():
    binary_data = os.urandom(30000)
    given = BytesIO(b"\xF7\x30\x75\x00\x00" + binary_data)
    assert types.Binary.from_resultset(given) == binary_data


@pytest.mark.parametrize("given,expected", [
    (byte_type(b"\xFF\x00\xFF\xA3\x5B"), "'ff00ffa35b'"),
    (byte_type(b"\x75\x08\x15\xBB\xAA"), "'750815bbaa'"),
])
def test_escape_binary(given, expected):
    assert types.Binary.to_sql(given) == expected


# ########################## Test value packing #####################################

@pytest.mark.parametrize("given,expected", [
    (None, b"\x08\xFF", ),
    ('123', b"\x08\x03123"),       # convert an integer into its string representation
    ("Hello World", b"\x08\x0B\x48\x65\x6C\x6C\x6F\x20\x57\x6F\x72\x6C\x64"),
])
def test_pack_string(given, expected):
    assert types.String.prepare(given) == expected


def test_pack_long_string():
    text = b'\xe6\x9c\xb1' * 3500
    expected = b"\x08\xF6\x04\x29" + text
    assert types.String.prepare(text.decode('cesu-8')) == expected


def test_pack_very_long_string():
    text = b'\xe6\x9c\xb1' * 35000
    expected = b"\x08\xF7\x28\x9a\x01\x00" + text
    assert types.String.prepare(text.decode('cesu-8')) == expected


# #############################################################################################################
#                         Real HANA interaction with strings (integration tests)
# #############################################################################################################

import tests.helper
TABLE = 'PYHDB_TEST_STRING'
TABLE_FIELDS = 'name varchar(5000)'   # 5000 chars is maximum for VARCHAR field


@pytest.fixture
def test_table(request, connection):
    """Fixture to create table for testing lobs, and dropping it after test run"""
    tests.helper.create_table_fixture(request, connection, TABLE, TABLE_FIELDS)


@pytest.mark.hanatest
def test_insert_string(connection, test_table):
    """Insert string into table"""
    cursor = connection.cursor()
    large_string = ''.join(random.choice(string.ascii_letters) for _ in iter_range(5000))
    cursor.execute("insert into %s (name) values (:1)" % TABLE, [large_string])
    connection.commit()
    cursor = connection.cursor()
    row = cursor.execute('select name from %s' % TABLE).fetchone()
    assert row[0] == large_string
