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
from pyhdb.protocol import lobs
from pyhdb.protocol.types import type_codes


# Fixture binary data for reading 2000 bytes blob:
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

# Fixture binary data for reading null blob:
NULL_BLOB_HEADER = b'\x01\x01'


def test_parse_blob():
    """Parse a BLOB with 2000 random items (bytes/chars)"""
    payload = io.BytesIO(BLOB_HEADER + BLOB_DATA)
    lob = lobs.from_payload(type_codes.BLOB, payload, None)
    assert isinstance(lob, lobs.Blob)  # check for correct instance
    assert lob.lob_header.lob_type == lob.lob_header.BLOB_TYPE
    assert lob.lob_header.options & lob.lob_header.LOB_OPTION_DATAINCLUDED
    assert lob.lob_header.char_length == len(BLOB_DATA)
    assert lob.lob_header.byte_length == len(BLOB_DATA)
    assert lob.lob_header.locator_id == BLOB_HEADER[20:28]
    assert lob.lob_header.chunk_length == 1024  # default length of lob data read initially
    assert lob.lob_header.total_lob_length == len(BLOB_DATA)
    assert lob.data.getvalue() == BLOB_DATA[:1024]


def test_blob_io_functions():
    """Test that io functionality (read/seek/getvalue()/...) works fine
    Stay below the 1024 item range when reading to avoid loading of additional lob data from DB.
    This feature is tested in a separate unittest below.
    """
    payload = io.BytesIO(BLOB_HEADER + BLOB_DATA)
    lob = lobs.from_payload(type_codes.BLOB, payload, None)
    assert lob.tell() == 0   # should be at start of lob initially
    assert lob.read(10) == BLOB_DATA[:10]
    assert lob.tell() == 10
    lob.seek(20)
    assert lob.tell() == 20
    assert lob.read(10) == BLOB_DATA[20:30]
    assert lob.read(10) == BLOB_DATA[30:40]
    assert lob.tell() == 40


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


def test_parse_null_blob():
    """Parse a BLOB which is NULL -> a None object is expected"""
    payload = io.BytesIO(NULL_BLOB_HEADER)
    lob = lobs.from_payload(type_codes.BLOB, payload, None)
    assert lob is None
