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

from pyhdb.lib.stringlib import humanhexlify, allhexlify, dehexlify


def test_humanhexlify():
    """Test plain humanhexlify function without shortening"""
    b = b'\x01\x62\x70\x00\xff'
    assert humanhexlify(b) == b'01 62 70 00 ff'


def test_humanhexlify_shorten():
    """Test plain humanhexlify function with shortening to 3 bytes"""
    b = b'\x01\x62\x70\x00\xff'
    assert humanhexlify(b, n=3) == b'01 62 70 ...'


def test_allhexlify():
    """Test that ALL byte chars are converted into hex values"""
    b = b'ab\x04ce'
    assert allhexlify(b) == b'\\x61\\x62\\x04\\x63\\x65'


def test_dehexlify():
    """Test reverting of humanhexlify"""
    b = '61 62 04 63 65'
    assert dehexlify(b) == b'ab\x04ce'
