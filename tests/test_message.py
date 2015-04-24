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
###
from pyhdb.connection import Connection
from pyhdb.protocol.segments import RequestSegment
from pyhdb.protocol.message import RequestMessage, ReplyMessage


class DummySegment(RequestSegment):
    """Used as pseudo segment instance for some tests"""
    @staticmethod
    def pack(payload, **kwargs):
        payload.write(b"\x00" * 10)


class TestRequestRequestMessage(object):
    """Test RequestMessage class"""
    @staticmethod
    def test_request_message_init_without_segment():
        connection = Connection("localhost", 30015, "Fuu", "Bar")
        msg = RequestMessage.new(connection)
        assert msg.segments == []

    @staticmethod
    def test_request_message_init_with_single_segment():
        connection = Connection("localhost", 30015, "Fuu", "Bar")

        request_seg = RequestSegment(0)
        msg = RequestMessage.new(connection, request_seg)
        assert msg.segments == [request_seg]

    @staticmethod
    def test_request_message_init_with_multiple_segments_as_list():
        connection = Connection("localhost", 30015, "Fuu", "Bar")

        request_seg_1 = RequestSegment(0)
        request_seg_2 = RequestSegment(1)
        msg = RequestMessage.new(connection, [request_seg_1, request_seg_2])
        assert msg.segments == [request_seg_1, request_seg_2]

    @staticmethod
    def test_request_message_init_with_multiple_segments_as_tuple():
        connection = Connection("localhost", 30015, "Fuu", "Bar")

        request_seg_1 = RequestSegment(0)
        request_seg_2 = RequestSegment(1)
        msg = RequestMessage.new(connection, (request_seg_1, request_seg_2))
        assert msg.segments == (request_seg_1, request_seg_2)

    @staticmethod
    def test_request_message_use_last_session_id():
        connection = Connection("localhost", 30015, "Fuu", "Bar")
        connection.session_id = 1

        msg = RequestMessage.new(connection)
        assert msg.session_id == connection.session_id

        connection.session_id = 5
        assert msg.session_id == connection.session_id

    @mock.patch('pyhdb.connection.Connection.get_next_packet_count', return_value=0)
    def test_request_message_keep_packet_count(self, get_next_packet_count):
        connection = Connection("localhost", 30015, "Fuu", "Bar")

        msg = RequestMessage.new(connection)
        assert msg.packet_count == 0

        # Check two time packet count of the message
        # but the get_next_packet_count method of connection
        # should only called once.
        assert msg.packet_count == 0
        get_next_packet_count.assert_called_once_with()

    @pytest.mark.parametrize("autocommit", [False, True])
    def test_payload_pack(self, autocommit):
        connection = Connection("localhost", 30015, "Fuu", "Bar", autocommit=autocommit)

        msg = RequestMessage.new(connection, [DummySegment(None)])
        payload = BytesIO()
        msg.build_payload(payload)

        assert payload.getvalue() == b"\x00" * 10

    @staticmethod
    def test_pack():
        connection = Connection("localhost", 30015, "Fuu", "Bar")

        msg = RequestMessage.new(connection, [DummySegment(None)])
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


class TestReplyRequestMessage(object):

    @staticmethod
    def test_message_use_received_session_id():
        connection = Connection("localhost", 30015, "Fuu", "Bar")
        connection.session_id = 12345
        msg = ReplyMessage(connection.session_id, connection.get_next_packet_count())

        assert msg.session_id == 12345

    @mock.patch('pyhdb.connection.Connection.get_next_packet_count', return_value=0)
    def test_message_use_received_packet_count(self, get_next_packet_count):
        connection = Connection("localhost", 30015, "Fuu", "Bar")
        connection.packet_count = 12345
        msg = ReplyMessage(connection.session_id, connection.packet_count)

        assert msg.packet_count == 12345
        assert not get_next_packet_count.called
