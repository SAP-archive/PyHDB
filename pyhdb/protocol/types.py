import sys
import re
import struct
import binascii
import decimal
from weakref import WeakValueDictionary
from datetime import datetime, time, date

import pyhdb.cesu8
from pyhdb.exceptions import InterfaceError
from pyhdb._compat import PY2, PY3, with_metaclass, iter_range, int_types, \
    string_types, byte_type, text_type


by_type_code = WeakValueDictionary()
by_python_type = WeakValueDictionary()

PY26 = PY2 and sys.version_info[1] == 6

class TypeMeta(type):
    """
    Meta class for type classes.
    """

    @staticmethod
    def _add_type_to_type_code_mapping(type_class, code):
        if not 0 <= code <= 127:
            raise InterfaceError(
                "%s type code must be between 0 and 127" %
                type_class.__name__
            )
        by_type_code[code] = type_class

    def __new__(cls, name, bases, attrs):
        type_class = super(TypeMeta, cls).__new__(cls, name, bases, attrs)

        # populate by_type_code mapping
        if hasattr(type_class, "code"):
            if isinstance(type_class.code, (tuple, list)):
                for code in type_class.code:
                    TypeMeta._add_type_to_type_code_mapping(type_class, code)
            else:
                TypeMeta._add_type_to_type_code_mapping(
                    type_class, type_class.code
                )

        # populate by_python_type mapping
        if hasattr(type_class, "python_type"):
            if isinstance(type_class.python_type, (tuple, list)):
                for typ in type_class.python_type:
                    by_python_type[typ] = type_class
            else:
                by_python_type[type_class.python_type] = type_class

        return type_class

class Type(with_metaclass(TypeMeta, object)):
    pass

class NoneType(Type):

    python_type = None.__class__

    @classmethod
    def to_sql(cls, self):
        return text_type("NULL")

class _IntType(Type):

    @classmethod
    def from_resultset(cls, payload):
        if payload.read(1) == b"\x01":
            return cls.struct.unpack(payload.read(cls.struct.size))[0]
        else:
            # Value is Null
            return None

class TinyInt(_IntType):

    code = 1
    struct = struct.Struct("B")

class SmallInt(_IntType):

    code = 2
    struct = struct.Struct("h")

class Int(_IntType):

    code = 3
    python_type = int_types
    struct = struct.Struct("i")

    @classmethod
    def to_sql(cls, value):
        return text_type(value)

class BigInt(_IntType):

    code = 4
    struct = struct.Struct("l")

class Decimal(Type):

    code = 5
    python_type = decimal.Decimal

    @classmethod
    def from_resultset(cls, payload):
        payload = bytearray(payload.read(16))
        payload.reverse()

        if payload[0] == 0x70:
            return None

        sign = payload[0] >> 7
        exponent = ((payload[0] & 0x7F) << 7) | ((payload[1] & 0xFE) >> 1)
        exponent = exponent - 6176
        mantissa = (payload[1] & 0x01) << 112

        x = 104
        for i in iter_range(2, 16):
            mantissa = mantissa | ((payload[i]) << x)
            x -= 8

        number = pow(-1, sign) * decimal.Decimal(10) ** exponent * mantissa
        return number

    @classmethod
    def to_sql(cls, value):
        return text_type(value)

class Real(Type):

    code = 6
    struct = struct.Struct("<f")

    @classmethod
    def from_resultset(cls, payload):
        payload = payload.read(8)
        if payload == b"\xFF\xFF\xFF\xFF":
            return None
        return cls.struct.unpack(payload)[0]

class Double(_IntType):

    code = 7
    python_type = float
    struct = struct.Struct("<d")

    @classmethod
    def from_resultset(cls, payload):
        payload = payload.read(8)
        if payload == b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF":
            return None
        return cls.struct.unpack(payload)[0]

    @classmethod
    def to_sql(cls, value):
        return text_type(value)

class String(Type):

    code = (8, 9, 10, 11, 29, 30)
    python_type = string_types

    ESCAPE_REGEX = re.compile(r"[\']")
    ESCAPE_MAP = {"'": "''"}

    @staticmethod
    def get_length(payload):
        length_indicator = struct.unpack('B', payload.read(1))[0]
        if length_indicator <= 245:
            length = length_indicator
        elif length_indicator == 246:
            length = struct.unpack('h', payload.read(2))[0]
        elif length_indicator == 247:
            length = struct.unpack('i', payload.read(4))[0]
        elif length_indicator == 255:
            return None
        else:
            raise InterfaceError("Unknown length inidcator")
        return length

    @classmethod
    def from_resultset(cls, payload):
        length = String.get_length(payload)
        if length is None:
            return None
        return payload.read(length).decode('cesu-8')

    @classmethod
    def to_sql(cls, value):
        return "'%s'" % cls.ESCAPE_REGEX.sub(
            lambda match: cls.ESCAPE_MAP.get(match.group(0)),
            value
        )

class Binary(Type):

    code = (12, 13, 33)
    python_type = byte_type

    @classmethod
    def from_resultset(cls, payload):
        length = String.get_length(payload)
        if length is None:
            return None
        return byte_type(payload.read(length))

    @classmethod
    def to_sql(cls, value):
        if PY26:
            value = bytes(value)
        value = binascii.hexlify(value)
        if PY3:
            value = value.decode('ascii')
        return "'%s'" % value

class Date(Type):

    code = 14
    python_type = date
    struct = struct.Struct("hbh")

    @classmethod
    def from_resultset(cls, payload):
        payload = bytearray(payload.read(4))
        if not payload[1] & 0x80:
            return None

        year = payload[0] | (payload[1] & 0x3F) << 8
        month = payload[2] + 1
        day = payload[3]
        return date(year, month, day)

    @classmethod
    def to_sql(cls, value):
        return "'%s'" % value.isoformat()

class Time(Type):

    code = 15
    python_type = time
    struct = struct.Struct("bbH")

    @classmethod
    def from_resultset(cls, payload):
        hour, minute, millisec = cls.struct.unpack(payload.read(4))
        if not hour & 0x80:
            return None

        hour = hour & 0x7f
        second, millisec = divmod(millisec, 1000)
        return time(hour, minute, second, millisec * 1000)

    @classmethod
    def to_sql(cls, value):
        return "'%s'" % value.strftime("%H:%M:%S")

class Timestamp(Type):

    code = 16
    python_type = datetime

    @classmethod
    def from_resultset(cls, payload):
        date = Date.from_resultset(payload)
        time = Time.from_resultset(payload)

        if date is None or time is None:
            return None

        return datetime.combine(date, time)

    @classmethod
    def to_sql(cls, value):
        return "'%s.%s'" % (
            value.strftime("%Y-%m-%d %H:%M:%S"),
            value.microsecond
        )

def escape(value):
    """
    Escape a single value.
    """

    if isinstance(value, (tuple, list)):
        return "(" + ", ".join([escape(arg) for arg in value]) + ")"
    else:
        typ = by_python_type.get(value.__class__)
        if typ is None:
            raise InterfaceError(
                "Unsupported python input: %s (%s)" % (value, value.__class__)
            )

        return typ.to_sql(value)

def escape_values(values):
    """
    Escape multiple values from a list, tuple or dict.
    """
    if isinstance(values, (tuple, list)):
        return tuple([escape(value) for value in values])
    elif isinstance(values, dict):
        return dict([
            (key, escape(value)) for (key, value) in values.items()
        ])
    else:
        raise InterfaceError("escape_values expects list, tuple or dict")
