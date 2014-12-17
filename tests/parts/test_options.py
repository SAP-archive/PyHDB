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

from io import BytesIO

import pytest

from pyhdb.exceptions import InterfaceError
from pyhdb.protocol.parts import OptionPart

def test_not_pack_none_value_items():
    class DummyOptionPart(OptionPart):
        kind = 126

        option_definition = {
            # Identifier, (Value, Type)
            "int_field": (1, 3),
            "bigint_field": (2, 4),
            "bool_field": (3, 28)
        }

    arguments, payload = DummyOptionPart({
        "int_field": 123456789,
        "bigint_field": None,
        "bool_field": True
    }).pack_data()
    assert arguments == 2

def test_unknown_option_is_not_packable():
    class DummyOptionPart(OptionPart):
        kind = 126

        option_definition = {
            # Identifier, (Value, Type)
            "int_field": (1, 3),
        }

    with pytest.raises(InterfaceError) as excinfo:
        DummyOptionPart({
            "unknown_option": 12345
        }).pack_data()

    assert "Unknown option identifier" in excinfo.exconly()

class TestOptionPartBooleanType():

    class DummyOptionPart(OptionPart):
        kind = 126

        option_definition = {
            # Identifier, (Value, Type)
            "bool_field": (1, 28)
        }

    def test_pack_true(self):
        arguments, payload = self.DummyOptionPart({
            "bool_field": True
        }).pack_data()
        assert arguments == 1
        assert payload == b"\x01\x1C\x01"

    def test_pack_false(self):
        arguments, payload = self.DummyOptionPart({
            "bool_field": False
        }).pack_data()
        assert arguments == 1
        assert payload == b"\x01\x1C\x00"

    def test_unpack_true(self):
        options, = self.DummyOptionPart.unpack_data(
            1,
            BytesIO(b"\x01\x1C\x01")
        )
        assert options == {"bool_field": True}

    def test_unpack_false(self):
        options, = self.DummyOptionPart.unpack_data(
            1,
            BytesIO(b"\x01\x1C\x00")
        )
        assert options == {"bool_field": False}

class TestOptionPartInt():

    class DummyOptionPart(OptionPart):
        kind = 126

        option_definition = {
            # Identifier, (Value, Type)
            "int_field": (1, 3)
        }
    def test_pack(self):
        arguments, payload = self.DummyOptionPart({
            "int_field": 123456
        }).pack_data()
        assert arguments == 1
        assert payload == b"\x01\x03\x40\xE2\x01\x00"

    def test_unpack(self):
        options, = self.DummyOptionPart.unpack_data(
            1,
            BytesIO(b"\x01\x03\x40\xE2\x01\x00")
        )
        assert options == {"int_field": 123456}

class TestOptionPartBigInt():

    class DummyOptionPart(OptionPart):
        kind = 126

        option_definition = {
            # Identifier, (Value, Type)
            "bigint_field": (1, 4)
        }

    def test_pack(self):
        arguments, payload = self.DummyOptionPart({
            "bigint_field": 2**32
        }).pack_data()
        assert arguments == 1
        assert payload == b"\x01\x04\x00\x00\x00\x00\x01\x00\x00\x00"

    def test_unpack(self):
        options, = self.DummyOptionPart.unpack_data(
            1,
            BytesIO(b"\x01\x04\x00\x00\x00\x00\x01\x00\x00\x00")
        )
        assert options == {"bigint_field": 2**32}

class TestOptionPartString():

    class DummyOptionPart(OptionPart):
        kind = 126

        option_definition = {
            # Identifier, (Value, Type)
            "string_field": (1, 29)
        }

    def test_pack(self):
        arguments, payload = self.DummyOptionPart({
            "string_field": u"Hello World"
        }).pack_data()
        assert arguments == 1
        assert payload == b"\x01\x1d\x0b\x00\x48\x65\x6c\x6c" \
                          b"\x6f\x20\x57\x6f\x72\x6c\x64"

    def test_unpack(self):
        options, = self.DummyOptionPart.unpack_data(
            1,
            BytesIO(
                b"\x01\x1d\x0b\x00\x48\x65\x6c\x6c" \
                b"\x6f\x20\x57\x6f\x72\x6c\x64"
            )
        )
        assert options == {"string_field": u"Hello World"}
