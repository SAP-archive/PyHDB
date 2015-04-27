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
from io import BytesIO
###
from pyhdb.protocol.parts import Part, PART_MAPPING
from pyhdb.exceptions import InterfaceError


class DummyPart(Part):
    """
    Dummy part definition for testing purposes.
    This part contains a defined number of zeros.
    """

    kind = 127

    def __init__(self, zeros=10):
        self.zeros = zeros

    def pack_data(self):
        return self.zeros, b"\x00" * self.zeros

    @classmethod
    def unpack_data(cls, argument_count, payload):
        payload = payload.read(argument_count)
        assert payload == b"\x00" * argument_count
        return argument_count,


class TestBasePart(object):

    @staticmethod
    def test_pack_dummy_part():
        part = DummyPart(10)
        assert part.zeros == 10

        packed = part.pack(0)
        header = packed[0:16]
        assert header[0:1] == b"\x7f"
        assert header[1:2] == b"\x00"
        assert header[2:4] == b"\x0a\x00"
        assert header[4:8] == b"\x00\x00\x00\x00"
        assert header[8:12] == b"\x0a\x00\x00\x00"
        assert header[12:16] == b"\x00\x00\x00\x00"

        payload = packed[16:]
        assert len(payload) == 16
        assert payload == b"\x00" * 16

    @staticmethod
    def test_unpack_single_dummy_part():
        packed = BytesIO(
            b'\x7F\x00\x0A\x00\x00\x00\x00\x00\x0A\x00\x00\x00\x00'
            b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        )

        unpacked = tuple(DummyPart.unpack_from(packed, 1))
        assert len(unpacked) == 1

        unpacked = unpacked[0]
        assert isinstance(unpacked, DummyPart)
        assert unpacked.zeros == 10

    @staticmethod
    def test_unpack_multiple_dummy_parts():
        packed = BytesIO(
            b"\x7f\x00\x0a\x00\x00\x00\x00\x00\x0a\x00\x00\x00\xc8\xff\x01"
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
            b"\x00\x00\x7f\x00\x0e\x00\x00\x00\x00\x00\x0e\x00\x00\x00\xa8"
            b"\xff\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
            b"\x00\x00\x00\x00\x7f\x00\x12\x00\x00\x00\x00\x00\x12\x00\x00"
            b"\x00\x68\xff\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        )

        unpacked = tuple(Part.unpack_from(packed, 3))
        assert len(unpacked) == 3
        assert isinstance(unpacked[0], DummyPart)
        assert unpacked[0].zeros == 10

        assert isinstance(unpacked[1], DummyPart)
        assert unpacked[1].zeros == 14

        assert isinstance(unpacked[2], DummyPart)
        assert unpacked[2].zeros == 18

    @staticmethod
    def test_invalid_part_header_raises_exception():
        packed = BytesIO(
            b"\xbb\xff\xaa\x00\x00\x00\x00\x0a\x00\x00\x00\x00\x00\x00\x00"
        )
        with pytest.raises(InterfaceError):
            tuple(Part.unpack_from(packed, 1))

    @staticmethod
    def test_unpack_unkown_part_raises_exception():
        packed = BytesIO(
            b"\x80\x00\x0a\x00\x00\x00\x00\x00\x0a\x00\x00\x00\x00"
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        )

        with pytest.raises(InterfaceError):
            tuple(Part.unpack_from(packed, 1))


class TestPartMetaClass(object):

    @staticmethod
    def test_part_kind_mapping():
        assert 125 not in PART_MAPPING

        class Part125(Part):
            kind = 125
        assert PART_MAPPING[125] == Part125

    @staticmethod
    def test_part_without_kind_attribute_will_be_not_in_mapping():
        assert 123 not in PART_MAPPING

        class Part123(Part):
            # No kind attribute
            pass
        assert 123 not in PART_MAPPING
        assert Part123 not in PART_MAPPING.values()

    @staticmethod
    def test_part_kind_out_of_range_raises_exception():

        with pytest.raises(InterfaceError):
            class OutOfRangePart(Part):
                kind = 255
            assert OutOfRangePart not in PART_MAPPING.values()

    @staticmethod
    def test_part_class_mapping_updates_after_class_left_scope():
        assert 124 not in PART_MAPPING

        class Part124(Part):
            kind = 124
        assert PART_MAPPING[124] == Part124

        del Part124
        import gc
        gc.collect()

        assert 124 not in PART_MAPPING
