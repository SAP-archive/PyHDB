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

import pytest
import mock
from io import BytesIO

from pyhdb.client import Connection
from pyhdb.protocol.base import Message, RequestSegment, Part, part_mapping
from pyhdb.exceptions import InterfaceError


class DummySegment(RequestSegment):
    """Used as pseudo segment instance for some tests"""
    @staticmethod
    def pack(payload, **kwargs):
        payload.write(b"\x00" * 10)


class TestBaseMessage(object):

    def test_message_init_without_segment(self):
        connection = Connection("localhost", 30015, "Fuu", "Bar")
        msg = Message(connection)
        assert msg.segments == []

    def test_message_init_with_single_segment(self):
        connection = Connection("localhost", 30015, "Fuu", "Bar")

        request = RequestSegment(0)
        msg = Message(connection, request)
        assert msg.segments == [request]

    def test_message_init_with_multiple_segments_as_list(self):
        connection = Connection("localhost", 30015, "Fuu", "Bar")

        request_1 = RequestSegment(0)
        request_2 = RequestSegment(1)
        msg = Message(connection, [request_1, request_2])
        assert msg.segments == [request_1, request_2]

    def test_message_init_with_multiple_segments_as_tuple(self):
        connection = Connection("localhost", 30015, "Fuu", "Bar")

        request_1 = RequestSegment(0)
        request_2 = RequestSegment(1)
        msg = Message(connection, (request_1, request_2))
        assert msg.segments == (request_1, request_2)

    def test_message_use_last_session_id(self):
        connection = Connection("localhost", 30015, "Fuu", "Bar")
        connection.session_id = 1

        msg = Message(connection)
        assert msg.session_id == connection.session_id

        connection.session_id = 5
        assert msg.session_id == connection.session_id

    @mock.patch('pyhdb.client.Connection.get_next_packet_count',
                return_value=0)
    def test_message_keep_packet_count(self, get_next_packet_count):
        connection = Connection("localhost", 30015, "Fuu", "Bar")

        msg = Message(connection)
        assert msg.packet_count == 0

        # Check two time packet count of the message
        # but the get_next_packet_count method of connection
        # should only called once.
        assert msg.packet_count == 0
        get_next_packet_count.assert_called_once_with()

    @pytest.mark.parametrize("autocommit", [False, True])
    def test_payload_pack(self, autocommit):
        connection = Connection("localhost", 30015, "Fuu", "Bar", autocommit=autocommit)

        msg = Message(connection, [DummySegment(None)])
        payload = BytesIO()
        msg.build_payload(payload)

        assert payload.getvalue() == b"\x00" * 10

    def test_pack(self):
        connection = Connection("localhost", 30015, "Fuu", "Bar")

        msg = Message(connection, [DummySegment(None)])
        payload = msg.pack()
        packed = payload.getvalue()
        assert isinstance(packed, bytes)

        # Session id
        assert packed[0:8] == b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"

        # Packet count
        assert packed[8:12] == b"\x00\x00\x00\x00"

        # var part length
        assert packed[12:16] == b"\x0A\x00\x00\x00"

        # var part size
        assert packed[16:20] == b"\xE0\xFF\x01\x00"

        # no of segments
        assert packed[20:22] == b"\x01\x00"

        # reserved
        assert packed[22:32] == b"\x00" * 10

        # payload
        assert packed[32:42] == b"\x00" * 10


class TestReceivedMessage(object):

    def test_message_use_received_session_id(self):
        connection = Connection("localhost", 30015, "Fuu", "Bar")
        msg = Message(connection)
        msg._session_id = 12345

        assert msg.session_id == 12345

    @mock.patch('pyhdb.client.Connection.get_next_packet_count',
                return_value=0)
    def test_message_use_received_packet_count(self, get_next_packet_count):
        connection = Connection("localhost", 30015, "Fuu", "Bar")
        msg = Message(connection)
        msg._packet_count = 12345

        assert msg.packet_count == 12345
        assert not get_next_packet_count.called


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

    def test_pack_dummy_part(self):
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

    def test_unpack_single_dummy_part(self):
        packed = BytesIO(
            b'\x7F\x00\x0A\x00\x00\x00\x00\x00\x0A\x00\x00\x00\x00'
            b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        )

        unpacked = tuple(DummyPart.unpack_from(packed, 1))
        assert len(unpacked) == 1

        unpacked = unpacked[0]
        assert isinstance(unpacked, DummyPart)
        assert unpacked.zeros == 10

    def test_unpack_multiple_dummy_parts(self):
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

    def test_invalid_part_header_raises_exception(self):
        packed = BytesIO(
            b"\xbb\xff\xaa\x00\x00\x00\x00\x0a\x00\x00\x00\x00\x00\x00\x00"
        )
        with pytest.raises(InterfaceError):
            tuple(Part.unpack_from(packed, 1))

    def test_unpack_unkown_part_raises_exception(self):
        packed = BytesIO(
            b"\x80\x00\x0a\x00\x00\x00\x00\x00\x0a\x00\x00\x00\x00"
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        )

        with pytest.raises(InterfaceError):
            tuple(Part.unpack_from(packed, 1))


class TestPartMetaClass(object):

    def test_part_kind_mapping(self):
        assert 125 not in part_mapping
        class Part125(Part):
            kind = 125
        assert part_mapping[125] == Part125

    def test_part_without_kind_attribute_will_be_not_in_mapping(self):
        assert 123 not in part_mapping
        class Part123(Part):
            # No kind attribute
            pass
        assert 123 not in part_mapping

    def test_part_kind_out_of_range_raises_exception(self):
        with pytest.raises(InterfaceError):
            class OutOfRangePart(Part):
                kind = 255

    def test_part_class_mapping_updates_after_class_left_scope(self):
        assert 124 not in part_mapping
        class Part124(Part):
            kind = 124
        assert part_mapping[124] == Part124

        del Part124
        import gc
        gc.collect()

        assert 124 not in part_mapping
