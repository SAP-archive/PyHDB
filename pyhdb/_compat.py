import sys

PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

if PY2:
    text_type = unicode
    byte_type = bytearray
    string_types = (str, unicode)
    int_types = (int, long)
    unichr = unichr
    iter_range = xrange
else:
    text_type = str
    byte_type = bytes
    string_types = (str,)
    int_types = (int,)
    unichr = chr
    iter_range = range

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
