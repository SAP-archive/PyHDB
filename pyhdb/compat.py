# Copyright 2014, 2015 SAP SE
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys

PY2 = sys.version_info[0] == 2
PY26 = PY2 and sys.version_info[1] == 6
PY3 = sys.version_info[0] == 3

if PY2:
    text_type = unicode
    byte_type = bytearray
    string_types = (str, unicode)
    int_types = (int, long)
    unichr = unichr
    iter_range = xrange
    import ConfigParser as configparser
    from itertools import izip
else:
    text_type = str
    byte_type = bytes
    string_types = (str,)
    int_types = (int,)
    unichr = chr
    iter_range = range
    import configparser
    izip = zip

# workaround for 'narrow' Python builds
if sys.maxunicode <= 65535:
    unichr = lambda n: ('\\U%08x' % n).decode('unicode-escape')


def with_metaclass(meta, *bases):
    """
    Function from jinja2/_compat.py.
    Author: Armin Ronacher
    License: BSD.
    """
    class metaclass(meta):
        __call__ = type.__call__
        __init__ = type.__init__
        def __new__(cls, name, this_bases, d):
            if this_bases is None:
                return type.__new__(cls, name, (), d)
            return meta(name, bases, d)
    return metaclass('temporary_class', None, {})


def is_text(obj):
    return isinstance(obj, text_type)
