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

import re
import binascii


def allhexlify(data):
    """Hexlify given data into a string representation with hex values for all chars
    Input like
        'ab\x04ce'
    becomes
        '\x61\x62\x04\x63\x65'
    """
    hx = binascii.hexlify(data)
    return b''.join([b'\\x' + o for o in re.findall(b'..', hx)])


def humanhexlify(data, n=-1):
    """Hexlify given data with 1 space char btw hex values for easier reading for humans
    :param data: binary data to hexlify
    :param n: If n is a positive integer then shorten the output of this function to n hexlified bytes.

    Input like
        'ab\x04ce'
    becomes
        '61 62 04 63 65'

    With n=3 input like
        data='ab\x04ce', n=3
    becomes
        '61 62 04 ...'
    """
    tail = b' ...' if 0 < n < len(data) else b''
    if tail:
        data = data[:n]
    hx = binascii.hexlify(data)
    return b' '.join(re.findall(b'..', hx)) + tail


def dehexlify(hx):
    """Revert human hexlification - remove white spaces from hex string and convert into real values
    Input like
        '61 62 04 63 65'
    becomes
        'ab\x04ce'
    """
    return binascii.unhexlify(hx.replace(' ', ''))
