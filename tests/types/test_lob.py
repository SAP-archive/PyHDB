# -*- coding: utf-8 -*-

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

import io
import mock
import pytest
import string
from pyhdb.protocol import lobs
from pyhdb.protocol import parts
from pyhdb.protocol.types import type_codes
from pyhdb.exceptions import DataError


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
    assert str(blob) == data


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
    assert isinstance(clob.data, io.StringIO)


def test_clob_from_ascii_string():
    data = string.ascii_letters
    clob = lobs.Clob(data)
    assert clob.getvalue() == data
    assert clob.encode() == data
    assert str(clob) == data


def test_clob_from_ascii_unicode():
    data = string.ascii_letters.decode('ascii')
    clob = lobs.Clob(data)
    assert clob.getvalue() == data
    assert clob.encode() == data


def test_clob_from_string_io():
    data = string.ascii_letters.decode('ascii')
    text_io = io.StringIO(data)
    clob = lobs.Clob(text_io)
    assert clob.getvalue() == data
    assert clob.data is text_io


def test_clob_from_nonascii_string_raises():
    data = u'朱の子ましけ'
    utf8_data = data.encode('utf8')
    with pytest.raises(UnicodeDecodeError):
        lobs.Clob(utf8_data)


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
    assert str(nclob) == data


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

# Fixture: binary data for reading 52 character CLOB (ascii character LOB):
CLOB_HEADER = b'\x00\x06\x00\x00\x34\x00\x00\x00\x00\x00\x00\x00\x34\x00\x00\x00' \
              b'\x00\x00\x00\x00\x00\x00\x00\x00\xae\xc9\x04\x00\x34\x00\x00\x00'
CLOB_DATA = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'

# Fixture; binary data for reading 52 character NCLOB (unicode character LOB):
NCLOB_HEADER = b'\x00\x06\x00\x00\x34\x00\x00\x00\x00\x00\x00\x00\x9c\x00\x00\x00' \
               b'\x00\x00\x00\x00\x00\x00\x00\x00\xd1\xc9\x04\x00\x9c\x00\x00\x00'
NCLOB_DATA = u'朱の子ましける日におえつかうまつらずはしにさはる事侍りてして延光雀院朝臣につかは野の若菜も日は小松引もさ'
BIN_NCLOB_DATA = NCLOB_DATA.encode('utf8')

lob_params = pytest.mark.parametrize("type_code, lob_header, bin_lob_data, lob_data", [
    (type_codes.BLOB, BLOB_HEADER, BLOB_DATA, BLOB_DATA),
    (type_codes.CLOB, CLOB_HEADER, CLOB_DATA, CLOB_DATA),
    (type_codes.NCLOB, NCLOB_HEADER, BIN_NCLOB_DATA, NCLOB_DATA),
])


# ### Test of reading of LOB data/parsing header ##################################################

@lob_params
def test_read_lob(type_code, lob_header, bin_lob_data, lob_data):
    """Read/parse a LOB with given payload (data)"""
    payload = io.BytesIO(lob_header + bin_lob_data)
    lob = lobs.from_payload(type_codes.BLOB, payload, None)
    assert isinstance(lob, lobs.Blob)  # check for correct instance
    assert lob.lob_header.lob_type in (0, lob.lob_header.LOB_TYPES[type_code])
    assert lob.lob_header.options & lob.lob_header.LOB_OPTION_DATAINCLUDED
    assert lob.lob_header.char_length == len(lob_data)
    assert lob.lob_header.byte_length == len(bin_lob_data)
    assert lob.lob_header.locator_id == lob_header[20:28]
    assert lob.lob_header.chunk_length == min(len(bin_lob_data), MAX_LOB_DATA_LENGTH)
    assert lob.lob_header.total_lob_length == len(lob_data)
    assert lob.data.getvalue() == bin_lob_data[:1024]


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

@mock.patch('pyhdb.protocol.lobs.Lob._read_missing_lob_data_from_db')
def test_blob_read_triggers_further_loading(_read_missing_lob_data_from_db):
    """Test that reading beyond currently available data (> 1024 items) triggers a READLOB request"""
    payload = io.BytesIO(BLOB_HEADER + BLOB_DATA)
    lob = lobs.from_payload(type_codes.BLOB, payload, None)
    lob_len = len(lob.data.getvalue())
    lob.read(lob_len + 100)  # read 100 items (chars/bytes) more than available
    # Reading extra 100 items should have triggered _read_missing_lob_data_from_db():
    _read_missing_lob_data_from_db.assert_called_once_with(1024, 100)


@mock.patch('pyhdb.protocol.lobs.Lob._read_missing_lob_data_from_db')
def test_blob_seek_triggers_further_loading(_read_missing_lob_data_from_db):
    """Test that seeking beyond currently available data (> 1024 items) triggers a READLOB request"""
    payload = io.BytesIO(BLOB_HEADER + BLOB_DATA)
    lob = lobs.from_payload(type_codes.BLOB, payload, None)
    lob_len = len(lob.data.getvalue())
    lob.seek(lob_len + 100)  # seek to position 100 items (bytes/chars) after what is available
    # This should have triggered _read_missing_lob_data_from_db().
    # Since seek() makes the assumption that the user wants to read data from the new position
    # another EXTRA_NUM_ITEMS_TO_READ_AFTER_SEEK are read in addition to the 100 beyond the current num items:
    _read_missing_lob_data_from_db.assert_called_once_with(1024, 100 + lobs.Lob.EXTRA_NUM_ITEMS_TO_READ_AFTER_SEEK)


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


# insert statements

@pytest.mark.hanatest
def test_insert_single_string_blob_row(connection, test_lob_table):
    """Insert a single row providing blob data in string format (argument order: name, blob)"""
    cursor = connection.cursor()
    blob_data = 'ab \0x1 \0x17 yz'
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
