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

import pytest
import pyhdb.cesu8  # import required to register cesu8 encoding


@pytest.mark.parametrize("encoded,unicode_obj", [
    (b"\xed\xa6\x9d\xed\xbd\xb7", u"\U00077777"),
    (b"\xed\xa0\x80\xed\xb0\xb0", u"\U00010030"),
    (b"\xed\xa0\x80\xed\xbc\xb0", u"\U00010330"),
    (b"\xed\xa0\x81\xed\xb0\x80", u"\U00010400"),
    (b"\xed\xa0\xbd\xed\xb1\x86", u"\U0001F446"),
    (b"\xed\xa0\xbd\xed\xb2\x86", u"\U0001f486"),
    (b"\xed\xa0\xbd\xed\xb8\x86", u"\U0001f606"),
    (b"\xed\xa0\xbf\xed\xbc\x84", u"\U0001FF04"),
])
def test_pure_cesu8_decode(encoded, unicode_obj):
    assert encoded.decode('cesu-8') == unicode_obj


@pytest.mark.parametrize("encoded,unicode_obj", [
    (b"\xc3\xa4", u"\xe4"),
    (b"\xe2\xac\xa1", u"\u2b21"),
])
def test_fallback_to_utf8_of_cesu8_decode(encoded, unicode_obj):
    assert encoded.decode('cesu-8') == unicode_obj


def test_multiple_chars_in_cesu8_decode():
    encoded = b"\xed\xa0\xbd\xed\xb0\x8d\xed\xa0\xbd\xed\xb1\x8d"
    assert encoded.decode('cesu-8') == u'\U0001f40d\U0001f44d'


def test_cesu8_and_utf8_mixed_decode():
    encoded = b"\xed\xa0\xbd\xed\xb0\x8d\x20\x69\x73\x20\x61\x20" \
              b"\xcf\x86\xce\xaf\xce\xb4\xce\xb9"
    assert encoded.decode('cesu-8') == \
        u'\U0001f40d is a \u03c6\u03af\u03b4\u03b9'


@pytest.mark.parametrize("encoded,unicode_obj", [
    (b"\xed\xa6\x9d\xed\xbd\xb7", u"\U00077777"),
    (b"\xed\xa0\x80\xed\xb0\xb0", u"\U00010030"),
    (b"\xed\xa0\x80\xed\xbc\xb0", u"\U00010330"),
    (b"\xed\xa0\x81\xed\xb0\x80", u"\U00010400"),
    (b"\xed\xa0\xbd\xed\xb1\x86", u"\U0001F446"),
    (b"\xed\xa0\xbd\xed\xb2\x86", u"\U0001f486"),
    (b"\xed\xa0\xbd\xed\xb8\x86", u"\U0001f606"),
    (b"\xed\xa0\xbf\xed\xbc\x84", u"\U0001FF04"),
])
def test_pure_cesu8_encode(encoded, unicode_obj):
    assert unicode_obj.encode('cesu-8') == encoded


@pytest.mark.parametrize("encoded,unicode_obj", [
    (b"\xc3\xa4", u"\xe4"),
    (b"\xe2\xac\xa1", u"\u2b21"),
])
def test_fallback_to_utf8_encode(encoded, unicode_obj):
    assert unicode_obj.encode('cesu-8') == encoded


def test_multiple_chars_in_cesu8_encode():
    encoded = b"\xed\xa0\xbd\xed\xb0\x8d\xed\xa0\xbd\xed\xb1\x8d"
    assert u'\U0001f40d\U0001f44d'.encode('cesu-8') == encoded


def test_cesu8_and_utf8_mixed_encode():
    encoded = b"\xed\xa0\xbd\xed\xb0\x8d\x20\x69\x73\x20\x61\x20" \
              b"\xcf\x86\xce\xaf\xce\xb4\xce\xb9"
    assert u'\U0001f40d is a \u03c6\u03af\u03b4\u03b9'.encode('cesu-8') == \
        encoded
