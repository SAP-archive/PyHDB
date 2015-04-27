# -*- coding: utf-8 -*-

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

import io
import mock
import pytest
import string
from pyhdb.protocol import lobs
from pyhdb.protocol import parts
from pyhdb.protocol.types import type_codes
from pyhdb.exceptions import DataError
from pyhdb.compat import PY2


# #############################################################################################################
#                         Basic LOB creation from ascii and unicode strings
# #############################################################################################################

# ### Testing BLOBs

def test_blob_uses_bytes_io():
    data = b'abc \x01 \x45 vv'
    blob = lobs.Blob(data)
    assert isinstance(blob.data, io.BytesIO)


def test_blob_from_bytestring():
    data = b'abc \x01 \x45 vv'
    blob = lobs.Blob(data)
    assert blob.getvalue() == data


def test_blob_from_string():
    data = 'abc \x01 \x45 vv'
    blob = lobs.Blob(data)
    assert blob.getvalue() == data


def test_blob_from_bytes_io():
    data = b'abc \x01 \x45 vv'
    bytes_io = io.BytesIO(data)
    blob = lobs.Blob(bytes_io)
    assert blob.getvalue() == data
    # check that io container is used directly without copying:
    assert blob.data is bytes_io


# ### Testing CLOBs

def test_clob_uses_string_io():
    data = string.ascii_letters
    clob = lobs.Clob(data)
    assert isinstance(clob.data, lobs.CLOB_STRING_IO_CLASSES)


def test_clob_returns_string_instance():
    """For PY2 the result value must be a str-instance, not a unicode"""
    data = string.ascii_letters
    clob = lobs.Clob(data)
    assert isinstance(clob.read(), str)


def test_clob_from_ascii_string():
    data = string.ascii_letters
    clob = lobs.Clob(data)
    assert clob.getvalue() == data
    assert clob.encode() == data


def test_clob_from_ascii_unicode():
    """Feeding unicode string works as long as only contains ascii chars"""
    data = string.ascii_letters.decode('ascii')
    clob = lobs.Clob(data)
    assert clob.getvalue() == data
    assert clob.encode() == data


def test_clob_from_nonascii_unicode_raises():
    """Feeding unicode string with non-ascii chars should raise an exception"""
    data = u'朱の子ましけ'
    with pytest.raises(UnicodeEncodeError):
        lobs.Clob(data)


def test_clob_from_nonascii_string_raises():
    if PY2:
        pytest.skip('test only makes sense for io.StringIO in PY3')
    data = u'朱の子ましけ'
    utf8_data = data.encode('utf8')
    with pytest.raises(UnicodeDecodeError):
        lobs.Clob(utf8_data)


def test_clob_from_string_io():
    data = string.ascii_letters.decode('ascii')
    text_io = lobs.CLOB_STRING_IO(data)
    clob = lobs.Clob(text_io)
    assert clob.getvalue() == data
    assert clob.data is text_io


def test_clob___str___method():
    """Test that the magic __str__ method returns a proper string"""
    data = string.ascii_letters
    clob = lobs.Clob(data)
    assert str(clob) == data


def test_clob___unicode___method():
    """Test that the magic __unicode__ method returns a proper unicode string"""
    data = string.ascii_letters
    clob = lobs.Clob(data)
    assert type(unicode(clob)) == unicode
    assert unicode(clob) == data.decode('ascii')


# ### Testing NCLOBs

def test_nclob_uses_string_io():
    data = string.ascii_letters
    nclob = lobs.NClob(data)
    assert isinstance(nclob.data, io.StringIO)


def test_nclob_from_ascii_string():
    data = string.ascii_letters
    nclob = lobs.NClob(data)
    assert nclob.getvalue() == data
    assert nclob.encode() == data


def test_nclob_from_utf8_string():
    data = u'朱の子ましけ'
    utf8_data = data.encode('utf8')
    nclob = lobs.NClob(utf8_data)
    assert nclob.getvalue() == data
    assert nclob.encode() == utf8_data


def test_nclob_from_unicode():
    data = u'朱の子ましけ'
    nclob = lobs.NClob(data)
    assert nclob.getvalue() == data
    assert nclob.encode() == data.encode('utf8')


def test_nclob_from_string_io():
    data = u'朱の子ましけ'
    text_io = io.StringIO(data)
    nclob = lobs.NClob(text_io)
    assert nclob.getvalue() == data
    assert nclob.data is text_io


def test_nclob___str___method_for_ascii_chars():
    """Test that the magic __str__ method returns a proper string when only ascii chars are contained in the string"""
    data = string.ascii_letters
    nclob = lobs.NClob(data)
    str_nclob = str(nclob)
    assert type(str_nclob) == str
    assert str_nclob == data


def test_nclob__str___method_for_nonascii_chars_raises():
    """Test that the magic __str__ method raise Unicode error for non-ascii chars"""
    data = u'朱の子ましけ'
    nclob = lobs.NClob(data)
    with pytest.raises(UnicodeEncodeError):
        str(nclob)


def test_nclob___unicode___method_for_nonascii_chars():
    """Test that the magic __unicode__ method returns a proper unicode string"""
    data = u'朱の子ましけ'
    nclob = lobs.NClob(data)
    uni_nclob = unicode(nclob)
    assert type(uni_nclob) == unicode
    assert uni_nclob == data


def test_nclob___repr___method():
    data = u'朱の子ましけ'
    nclob = lobs.NClob(data)
    assert repr(nclob) == '<NClob length: %d>' % len(data)


# #############################################################################################################
#                         Creating LOBs from binary data (e.g. database payload)
# #############################################################################################################


# maximum length of lob data from result set:
MAX_LOB_DATA_LENGTH = 1024

# Fixture: binary data for reading 2000 bytes blob:
BLOB_HEADER = b'\x01\x02\x00\x00\xd0\x07\x00\x00\x00\x00\x00\x00\xd0\x07\x00\x00' \
              b'\x00\x00\x00\x00\x00\x00\x00\x00\xb2\xb9\x04\x00\x00\x04\x00\x00'
BLOB_DATA = 'qXHUi0ChHWEWUgSBYhq3LvrgtOOjgGMubxPs3nbfUsRrFKVs0uTgQB4eJtQnPFjG1ZD2rB6qXt0QKvOpyRurpAWYWAH6Q3O2iaGA' \
            'Ul0hwJhArNiB4vX3ZHJDC0TbF7crHPQktAzvkBf7SWtnnJ1OcC7pObioCIBp7iUoppenrMzwGoGdOeHCYJhTrVGN5ctRM1mYc3N9' \
            'kIsBR6cbmKxVVdVruFdYCZfoAYHa23Mhif3i6U7EqvvOJ7WSCFLz4eeB6DKCROoCBawYqUmkbIYVo5oyfge61qhULjv5jH5HOp1v' \
            'dvAfzpietVSUqmhDMZoR8Mb2jGmDBI1FMhxyfdiXXqjwFuFWGT4ecg46IfIWbppXWz9PYaf6c89rchV5VTRCwiIPCm32fcisBKLs' \
            'Z3Abro7iDrsuGG8Xs8avn75wRiI5mGMbfHMys8uinhrCQwn7zT2jXOWNdpmh06mly1pMjZY0HWlpN9bOjomgsty4IFY4wDKBawiz' \
            'nNNvCQOXeVREzYiMQfrgBYcG0GkfOzgxgLLnjIVkOjbcPMdwUgI0JLeru5Tg4Emuqq9LTYQYsHtfdGXDAAIbfz6GjxApeEpberxq' \
            'yERi2bzH5m7mk7NdbkM0H3WJNDNauRj8cJ3JYxZ4sVFdr52ugmJl7ZuIH67flo0VcfYz0G2K36AjKo4SQRVXJyH2LWfcA20Jd7TT' \
            'qv8m4W7SiHVgtBfmTdgFpemk8hYeLdwCGCGEWQffeSY6I64SrMLwVi0UpE1bWKuS8y5yZ4p0zZOxBQPvYG4H60gtraWmc9qbpZIg' \
            '4HgHbMmB4N01WxOtnRuVwMnseDuEY6YrxVj9wd79nPnPQsbQTQuRdzOS2lrLL5rcMiLGnUQXt6E9GUIvqHyi53RpxWqEdIWhTeh3' \
            'Fdt8bWYhUw4LgM3YX4ejeSXwlcNEat0mc7zWfha4EjFSKE6kTc9Xeowkrc5jjlHUkFXmwaOT469kWChc9ws4ew5mYSCKyYu7baWj' \
            'XxaFO5IlPgKQhE1dKNJtkYjfSTGwmBY5Jiw1XEdl5Ae0hSFUq7OPjuEbnNg7KLIkiSizbLlbcEUGHT92ZjcnPStOUsPRN0Pa2x3o' \
            'i1n8JXG3sfF2VAYaVrOAiQDWZ6W8rIfGB6a5YG2Gnf7rtvIfi0QP7mkV7BniBqFnQmmA7YAbFzkOYkIRUtqmry7IyXwIAE3N2NJ8' \
            'r1f3APsdY4M4hXNziYRKn8XW3l26ukR32SC8UFNPJU8hUn548YxF8hNr8B0cJoYo6H1erhKPYpFpPlYI3HzhA3mVHSXfLzxE0E9a' \
            'DjxCJ4frDUW8bBgG8T7FNnYv1rJGB1vwXMxYTs2feKq5QhvZJ5CG0JY432ghw4IrDklgu8UnyMZGJ3ZPowdYnsEHO5ukn3R3YWJ2' \
            'jh0ywq8yNfw5c4EPs5578Kp2jV1NdyLCzAbkJrt3WVObxRaMmGD0WSFSLxEQ8p2Uz2f6AmQFPvfWnKnv0MNu8rQAzNOiuPL1x63Z' \
            'UVilHtB7uAcfy8OEGwDSzeD0b8OhEVOZREXYfQQReD65E0r4zGfi5QLnAcu81l4BdvkIeuWuNpZysH5sxl9t9b1OSUNc6RYfiwmu' \
            'zvNOJyr2WY4hItnGXncV3kcRzq4BhhdtrxCwYFr482HofG9id9V5QBXO5x9y2zyUXEPdvh3UESyAa8xmvEXJHLO1EXvW1cFPCJGw' \
            'isQZXgVtvPSlkd8Eal4l6weKNMX8LXbuZdFAfIWeEPkeAIgboeuUAYJSFH4UPg4a0vp0tzXKRgqaVcAZl81CjrrGm5fBw3r1mDT3' \
            'IYPguIyGDs8xz4QvMjV4SPGWxkRrCrZgbCbO2t2PM6czC49c5FLbw3QX3UzinDaOumhJtzMpmAPUVjzX0cPiDalsmkxIb1Razz4e' \
            '1cdPATFx3vFelO8KOMurkMxFZKB0tWDjUWOGuQ4hiBu29TXAbR7Q9sxj8erB8omv5R4JyHivVz4DdQ6rWrVccsepgCI1Oydmfy6G'

# Fixture: binary data for reading 1500 character CLOB (ascii character LOB):
CLOB_HEADER = b'\x00\x02\x00\x00\xdd\x05\x00\x00\x00\x00\x00\x00\xdd\x05\x00\x00' \
              b'\x00\x00\x00\x00\x02\x00\x00\x00\x69\xef\x04\x00\x00\x04\x00\x00'
CLOB_DATA = 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Pellentesque aliquam aliquam nulla, in ' \
            'suscipit urna semper sit amet. Nunc eget lacus risus. Sed maximus augue a mattis tempor. In imperdiet ' \
            'felis in odio vehicula dapibus. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Vestibulum ' \
            'sit amet aliquam justo. Nullam a libero ut magna pellentesque varius sit amet at lectus. Vestibulum ' \
            'pharetra, magna nec iaculis elementum, dolor metus tristique turpis, eget feugiat metus ante eu erat. ' \
            'In dignissim ipsum fermentum tortor elementum, sed cursus massa ullamcorper. Quisque eu libero vel ' \
            'massa aliquet tincidunt. Donec accumsan tincidunt magna, sit amet imperdiet nulla dignissim vitae. ' \
            'Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Maecenas ' \
            'sed sapien nibh.\n' \
            'Sed laoreet lectus ut cursus iaculis. Aliquam convallis egestas ante eget facilisis. Integer quis ' \
            'ullamcorper nisi, vel posuere mauris. Fusce porta laoreet ante, eu accumsan ipsum bibendum sit amet. ' \
            'Morbi vel nunc nisi. Mauris eu turpis sapien. Donec dignissim risus ac molestie porttitor. Donec ' \
            'pharetra in dolor quis aliquam. Nullam leo magna, efficitur quis tincidunt tincidunt, eleifend sed ' \
            'purus. Maecenas quis urna et quam blandit vulputate et nec odio. Nullam posuere placerat turpis, non ' \
            'luctus nulla. Vestibulum efficitur odio sem, eu molestie sem malesuada ut. Nulla feugiat nibh tortor, ' \
            'et dapibus libero aliquet in. Aenean eleifend mauris eget lacus volutpat, id euismod metus.'

# Fixture; binary data for reading 1500 bytes (500 character) NCLOB (unicode character LOB):
NCLOB_HEADER = b'\x00\x02\x00\x00\xd0\x07\x00\x00\x00\x00\x00\x00\x70\x17\x00\x00' \
               b'\x00\x00\x00\x00\x05\x00\x00\x00\xb7\xef\x04\x00\x00\x0c\x00\x00'
NCLOB_DATA = u'朱の子ましける日におえつかうまつらずはしにさはる事侍りてして延光雀院朝臣につかは野の若菜も日は小松引' * 40

BIN_NCLOB_DATA = NCLOB_DATA.encode('utf8')

lob_params = pytest.mark.parametrize("type_code, lob_header, bin_lob_data, lob_data", [
    (type_codes.BLOB, BLOB_HEADER, BLOB_DATA, BLOB_DATA),
    (type_codes.CLOB, CLOB_HEADER, CLOB_DATA, CLOB_DATA),
    (type_codes.NCLOB, NCLOB_HEADER, BIN_NCLOB_DATA, NCLOB_DATA),
])


# ### Test of initializing and reading LOB instances (w/o db) #####################################

@lob_params
def test_lob_init_and_more(type_code, lob_header, bin_lob_data, lob_data):
    _LobClass = lobs.LOB_TYPE_CODE_MAP[type_code]
    lob = _LobClass(lob_data)
    assert lob.type_code == type_code
    assert lob.length == len(lob_data)
    assert len(lob) == len(lob_data)
    assert lob.read() == lob_data
    assert lob.tell() == len(lob_data)
    assert lob.read(5) == ''
    # go back to begging of lob, and just read three chars:
    assert lob.seek(0) == 0
    assert lob.read(3) == lob_data[:3]
    assert lob.tell() == 3


# ### Test of reading of LOB data/parsing header ##################################################

@lob_params
def test_read_lob(type_code, lob_header, bin_lob_data, lob_data):
    """Read/parse a LOB with given payload (data)"""
    payload = io.BytesIO(lob_header + bin_lob_data)
    lob = lobs.from_payload(type_code, payload, None)
    _ExpectedLobClass = lobs.LOB_TYPE_CODE_MAP[type_code]
    assert isinstance(lob, _ExpectedLobClass)  # check for correct instance
    assert lob._lob_header.lob_type in (0, lob._lob_header.LOB_TYPES[type_code])
    assert lob._lob_header.options & lob._lob_header.LOB_OPTION_DATAINCLUDED
    assert lob._lob_header.char_length == len(lob_data)
    assert lob._lob_header.byte_length == len(bin_lob_data)
    assert lob._lob_header.locator_id == lob_header[20:28]
    # assert lob._lob_header.chunk_length == min(len(bin_lob_data), MAX_LOB_DATA_LENGTH) - chunklength can vary ...
    assert lob._lob_header.total_lob_length == lob.length == len(lob_data)
    assert lob._current_lob_length == len(lob.data.getvalue())
    assert lob.data.getvalue() == lob_data[:1024]

    assert repr(lob) == '<%s length: %d (currently loaded from hana: %d)>' % \
        (_ExpectedLobClass.__name__, lob.length, lob._current_lob_length)


@lob_params
def test_blob_io_functions(type_code, lob_header, bin_lob_data, lob_data):
    """Test that io functionality (read/seek/getvalue()/...) works fine
    Stay below the 1024 item range when reading to avoid lazy loading of additional lob data from DB.
    This feature is tested in a separate unittest.
    """
    payload = io.BytesIO(lob_header + bin_lob_data)
    lob = lobs.from_payload(type_code, payload, None)
    assert lob.tell() == 0   # should be at start of lob initially
    assert lob.read(10) == lob_data[:10]
    assert lob.tell() == 10
    lob.seek(20)
    assert lob.tell() == 20
    assert lob.read(10) == lob_data[20:30]
    assert lob.read(10) == lob_data[30:40]
    assert lob.tell() == 40


# ### Test of lazy loading of LOB data ############################################################

@mock.patch('pyhdb.protocol.lobs.Lob._make_read_lob_request')
@lob_params
def test_blob_read_triggers_further_loading(_make_read_lob_request, type_code, lob_header, bin_lob_data, lob_data):
    """Test that reading beyond currently available data (> 1024 items) triggers a READLOB request"""
    return_value = lob_data[1024:1024 + 100]
    _ExpectedLobClass = lobs.LOB_TYPE_CODE_MAP[type_code]
    enc_return_value = return_value.encode(_ExpectedLobClass.encoding) if _ExpectedLobClass.encoding else return_value
    _make_read_lob_request.return_value = enc_return_value

    payload = io.BytesIO(lob_header + bin_lob_data)
    lob = lobs.from_payload(type_code, payload, None)
    lob_len = len(lob.data.getvalue())
    assert lob._current_lob_length == lob_len
    assert repr(lob) == '<%s length: %d (currently loaded from hana: %d)>' % \
        (_ExpectedLobClass.__name__, lob.length, lob._current_lob_length)

    lob.read(lob_len + 100)  # read 100 items (chars/bytes) more than available

    # Reading extra 100 items should have triggered _read_missing_lob_data_from_db():
    _make_read_lob_request.assert_called_once_with(1024, 100)
    assert lob.getvalue() == lob_data[:1024 + 100]
    assert lob._current_lob_length == lob_len + 100

    assert repr(lob) == '<%s length: %d (currently loaded from hana: %d)>' % \
        (_ExpectedLobClass.__name__, lob.length, lob._current_lob_length)


@mock.patch('pyhdb.protocol.lobs.Lob._make_read_lob_request')
def test_blob_seek_triggers_further_loading(_make_read_lob_request):
    """Test that seeking beyond currently available data (> 1024 items) triggers a READLOB request"""

    # Since the actual size of the blob is smaller than the look ahead we are planning for, calculate
    # the correct number of items to be read:
    num_items_to_read = min(100 + lobs.Lob.EXTRA_NUM_ITEMS_TO_READ_AFTER_SEEK, len(BLOB_DATA) - 1024)
    _make_read_lob_request.return_value = BLOB_DATA[1024:1024 + num_items_to_read]

    payload = io.BytesIO(BLOB_HEADER + BLOB_DATA)
    lob = lobs.from_payload(type_codes.BLOB, payload, None)
    lob_len = len(lob.data.getvalue())
    lob.seek(lob_len + 100)  # seek to position 100 items (bytes/chars) after what is available

    # This should have triggered _read_missing_lob_data_from_db().
    # Since seek() makes the assumption that the user wants to read data from the new position
    # another EXTRA_NUM_ITEMS_TO_READ_AFTER_SEEK are read in addition to the 100 beyond the current num items:
    _make_read_lob_request.assert_called_once_with(1024, num_items_to_read)
    assert lob.getvalue() == BLOB_DATA[:1024 + num_items_to_read]
    assert lob.tell() == lob_len + 100


# ### Test NULL LOBs ##############################################################################

@pytest.mark.parametrize("type_code, null_lob_header", [
    (type_codes.BLOB, b'\x01\x01'),
    (type_codes.CLOB, b'\x02\x01'),
    (type_codes.NCLOB, b'\x03\x01'),
    (type_codes.NCLOB, b'\x00\x01'),  # test for additional case where LOB_TYPE has buggy value or zero
])
def test_parse_null_blob(type_code, null_lob_header):
    """Parse a BLOB which is NULL -> a None object is expected"""
    payload = io.BytesIO(null_lob_header)
    lob = lobs.from_payload(type_code, payload, None)
    assert lob is None


# #############################################################################################################
#                         Real HANA interaction with LOBs (integration tests)
# #############################################################################################################

# LOB_TABLE_NAME = 'PYHDB_LOB_TEST'

TABLE = "PYHDB_TEST_1"


def exists_table(connection, name):
    cursor = connection.cursor()
    cursor.execute('SELECT 1 FROM "SYS"."TABLES" WHERE "TABLE_NAME" = %s', (name,))
    return cursor.fetchone() is not None


@pytest.fixture
def test_lob_table(request, connection):
    """Fixture to create table for testing lobs, and dropping it after test run"""
    cursor = connection.cursor()
    if exists_table(connection, "PYHDB_TEST_1"):
        cursor.execute('DROP TABLE "PYHDB_TEST_1"')

    assert not exists_table(connection, "PYHDB_TEST_1")
    cursor.execute('CREATE TABLE "PYHDB_TEST_1" (name varchar(9), fblob blob, fclob clob, fnclob nclob)')
    if not exists_table(connection, "PYHDB_TEST_1"):
        pytest.skip("Couldn't create table PYHDB_TEST_1")
        return

    def _close():
        cursor.execute('DROP TABLE "PYHDB_TEST_1"')
    request.addfinalizer(_close)


@pytest.fixture
def content_lob_table(request, connection):
    """Additional fixture to test_lob_table, inserts some rows for testing"""
    cursor = connection.cursor()
    if not exists_table(connection, "PYHDB_TEST_1"):
        raise RuntimeError('Could not find table PYHDB_TEST_1')
    cursor.execute("insert into PYHDB_TEST_1 (name) values('nulls')")  # all lobs are NULL
    cursor.execute("insert into PYHDB_TEST_1 values('lob0', 'blob0', 'clob0', 'nclob0')")


# #############################################################################################################
# select statements

@pytest.mark.hanatest
def test_select_single_blob_row(connection, test_lob_table, content_lob_table):
    cursor = connection.cursor()
    row = cursor.execute("select name, fblob, fclob, fnclob from %s where name='lob0'" % TABLE).fetchone()
    name, blob, clob, nclob = row
    assert name == 'lob0'
    assert isinstance(blob, lobs.Blob)
    assert isinstance(clob, lobs.Clob)
    assert isinstance(nclob, lobs.NClob)
    assert blob.read() == 'blob0'
    assert clob.read() == 'clob0'
    assert nclob.read() == 'nclob0'


@pytest.mark.hanatest
def test_select_single_null_blob_row(connection, test_lob_table, content_lob_table):
    cursor = connection.cursor()
    row = cursor.execute("select name, fblob, fclob, fnclob from %s where name='nulls'" % TABLE).fetchone()
    name, blob, clob, nclob = row
    assert name == 'nulls'
    assert blob is None
    assert clob is None
    assert nclob is None


# insert statements  ### TODO: use parameterization for the next 6 tests!

@pytest.mark.hanatest
def test_insert_single_string_blob_row(connection, test_lob_table):
    """Insert a single row providing blob data in string format (argument order: name, blob)"""
    cursor = connection.cursor()
    blob_data = BLOB_DATA
    cursor.execute("insert into %s (name, fblob) values (:1, :2)" % TABLE, ['blob1', blob_data])
    blob = cursor.execute("select fblob from %s where name='blob1' " % TABLE).fetchone()[0]
    assert blob.read() == blob_data


@pytest.mark.hanatest
def test_insert_single_object_blob_row(connection, test_lob_table):
    """Insert a single row providing blob data as LOB object (argument order: blob, name)"""
    cursor = connection.cursor()
    blob_data = 'ab \0x1 \0x17 yz'
    blob_obj = lobs.Blob(blob_data)
    cursor.execute("insert into %s (fblob, name) values (:1, :2)" % TABLE, [blob_obj, 'blob1'])
    blob = cursor.execute("select fblob from %s where name='blob1' " % TABLE).fetchone()[0]
    assert blob.read() == blob_data


@pytest.mark.hanatest
def test_insert_single_string_clob_row(connection, test_lob_table):
    """Insert a single row providing clob data in string format (argument order: name, clob)"""
    cursor = connection.cursor()
    clob_data = CLOB_DATA
    cursor.execute("insert into %s (name, fclob) values (:1, :2)" % TABLE, ['clob1', clob_data])
    clob = cursor.execute("select fclob from %s where name='clob1' " % TABLE).fetchone()[0]
    assert clob.read() == clob_data


@pytest.mark.hanatest
def test_insert_single_object_clob_row(connection, test_lob_table):
    """Insert a single row providing clob data in string format (argument order: name, clob)"""
    cursor = connection.cursor()
    clob_data = CLOB_DATA
    clob_obj = lobs.Clob(clob_data)
    cursor.execute("insert into %s (name, fclob) values (:1, :2)" % TABLE, ['clob1', clob_obj])
    clob = cursor.execute("select fclob from %s where name='clob1' " % TABLE).fetchone()[0]
    assert clob.read() == clob_data


@pytest.mark.hanatest
def test_insert_single_object_nclob_row(connection, test_lob_table):
    """Insert a single row providing blob data as LOB object (argument order: nclob, name)"""
    cursor = connection.cursor()
    nclob_data = NCLOB_DATA
    nclob_obj = lobs.NClob(nclob_data)
    cursor.execute("insert into %s (fnclob, name) values (:1, :2)" % TABLE, [nclob_obj, 'nclob1'])
    nclob = cursor.execute("select fnclob from %s where name='nclob1' " % TABLE).fetchone()[0]
    assert nclob.read() == nclob_data


@pytest.mark.hanatest
def test_insert_single_object_nclob_row(connection, test_lob_table):
    """Insert a single row providing nclob data as LOB object (argument order: nclob, name)"""
    cursor = connection.cursor()
    nclob_data = NCLOB_DATA
    nclob_obj = lobs.NClob(nclob_data)
    cursor.execute("insert into %s (fnclob, name) values (:1, :2)" % TABLE, [nclob_obj, 'nclob1'])
    nclob = cursor.execute("select fnclob from %s where name='nclob1' " % TABLE).fetchone()[0]
    assert nclob.read() == nclob_data


###

@pytest.mark.hanatest
def test_insert_single_blob_and_clob_row(connection, test_lob_table):
    """Insert a single row providing blob (as string) and clob (as LOB obj) (argument order: blob, name, clob)"""
    cursor = connection.cursor()
    blob_data = 'ab \0x1 \0x17 yz'
    clob_data = string.ascii_letters
    clob_obj = lobs.Clob(clob_data)
    cursor.execute("insert into %s (fblob, name, fclob) values (:1, :2, :3)" % TABLE, [blob_data, 'blob1', clob_obj])
    blob, clob = cursor.execute("select fblob, fclob from %s where name='blob1' " % TABLE).fetchone()
    assert blob.read() == blob_data
    assert clob.read() == clob_data


@pytest.mark.hanatest
def test_insert_many_clob_and_nclob_rows(connection, test_lob_table):
    """Insert multiple rows of clob and nclob. Providing wild mix of string, unicode, and lob objects"""
    nclob_data1 = u'เขืองลาจะปเที่ยวเมได้ไาว'   # unicode format
    clob_data1 = string.ascii_letters[:10]
    nclob_data2 = 'ずはしにさはる事侍'  # string format
    clob_data2 = string.ascii_letters[10:20]
    nclob_data3 = 'λάμβδαี่'   # string format
    nclob_obj = lobs.NClob(nclob_data3)
    clob_data3 = string.ascii_letters[20:30]
    clob_obj = lobs.Clob(clob_data3)
    cursor = connection.cursor()

    cursor.executemany("insert into %s (fnclob, name, fclob) values (:1, :2, :3)" % TABLE,
                       [[nclob_data1, 'blob1', clob_data1],
                        [nclob_data2, 'blob2', clob_data2],
                        [nclob_obj, 'blob3', clob_obj]])

    connection.commit()
    cursor = connection.cursor()
    rows = cursor.execute("select name, fnclob, fclob from %s order by name" % TABLE).fetchall()
    assert len(rows) == 3

    n_name, n_nclob, n_clob = rows[0]
    assert n_name == 'blob1'
    assert n_nclob.read() == nclob_data1
    assert n_clob.read() == clob_data1

    n_name, n_nclob, n_clob = rows[1]
    assert n_name == 'blob2'
    assert n_nclob.read() == nclob_data2.decode('utf8')
    assert n_clob.read() == clob_data2

    n_name, n_nclob, n_clob = rows[2]
    assert n_name == 'blob3'
    assert n_nclob.read() == nclob_data3.decode('utf8')
    assert n_clob.read() == clob_data3


@pytest.mark.hanatest
def test_insert_to_large_data_raises(connection, test_lob_table):
    """Trying to insert data within one single row beyond MAX_MESSAGE_SIZE raises a DataError"""
    # This is actually not really a lob problem, it can also occur with many large strings in a row.
    # However uploading lobs with a WriteLob request has not yet been implemented, so providing
    # a large lob also triggers this error.
    cursor = connection.cursor()
    large_blob_data = parts.MAX_MESSAGE_SIZE * u'λ'
    with pytest.raises(DataError):
        cursor.execute("insert into %s (name, fblob) values (:1, :2)" % TABLE, ['bigblob', large_blob_data])

# MORE TESTS TO WRITE:
# - create cases where writing multiple rows gets split into multiple execute rounds
