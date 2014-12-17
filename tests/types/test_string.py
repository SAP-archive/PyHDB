import os
import random
import string
from io import BytesIO

import pytest
from pyhdb.protocol import types
from pyhdb.exceptions import InterfaceError
from pyhdb._compat import byte_type

@pytest.mark.parametrize("input,expected", [
    (b"\xFF", None),
    (b"\x0B\x48\x65\x6C\x6C\x6F\x20\x57\x6F\x72\x6C\x64", "Hello World"),
])
def test_unpack_string(input, expected):
    input = BytesIO(input)
    assert types.String.from_resultset(input) == expected

def test_unpack_long_string():
    text = u'%030x' % random.randrange(16**300)
    input = BytesIO(b"\xF6\x2C\x01" + text.encode('cesu-8'))
    assert types.String.from_resultset(input) == text

def test_unpack_very_long_string():
    text = u'%030x' % random.randrange(16**30000)
    input = BytesIO(b"\xF7\x30\x75\x00\x00" + text.encode('cesu-8'))
    assert types.String.from_resultset(input) == text

def test_unpack_invalid_string_length_indicator():
    # The string length indicator 254 is not definied
    with pytest.raises(InterfaceError):
        types.String.from_resultset(BytesIO(b"\xFE"))

@pytest.mark.parametrize("input,expected", [
    (b"\xFF", None),
    (
        b"\x0B\xFF\x00\xFF\x00\xFF\x00\xFF\x00\xFF\x00\xFF",
        byte_type(b"\xFF\x00\xFF\x00\xFF\x00\xFF\x00\xFF\x00\xFF")
    ),
])
def test_unpack_binary(input, expected):
    input = BytesIO(input)
    assert types.Binary.from_resultset(input) == expected

def test_unpack_long_binary():
    binary_data = os.urandom(300)
    input = BytesIO(b"\xF6\x2C\x01" + binary_data)
    assert types.Binary.from_resultset(input) == binary_data

def test_unpack_very_long_binary():
    binary_data = os.urandom(30000)
    input = BytesIO(b"\xF7\x30\x75\x00\x00" + binary_data)
    assert types.Binary.from_resultset(input) == binary_data

@pytest.mark.parametrize("input,expected", [
    (byte_type(b"\xFF\x00\xFF\xA3\x5B"), "'ff00ffa35b'"),
    (byte_type(b"\x75\x08\x15\xBB\xAA"), "'750815bbaa'"),
])
def test_escape_binary(input, expected):
    assert types.Binary.to_sql(input) == expected
