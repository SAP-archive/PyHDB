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

import io
import struct
###
from pyhdb.protocol import constants
from pyhdb.protocol.headers import MessageHeader
from pyhdb.protocol.segments import ReplySegment
from pyhdb.lib.tracing import trace


class BaseMessage(object):
    """
    Message - basic frame for sending to and receiving data from HANA db.
    """
    header_struct = struct.Struct('qiIIhb9x')  # I8 I4 UI4 UI4 I2 I1 x[9]
    header_size = header_struct.size
    assert header_size == constants.general.MESSAGE_HEADER_SIZE  # Ensures that the constant defined there is correct!
    __tracing_attrs__ = ['header', 'segments']

    def __init__(self, session_id, packet_count, segments=(), autocommit=False, header=None):
        self.session_id = session_id
        self.packet_count = packet_count
        self.autocommit = autocommit
        self.segments = segments if isinstance(segments, (list, tuple)) else (segments, )
        self.header = header


class RequestMessage(BaseMessage):
    def build_payload(self, payload):
        """ Build payload of message. """
        for segment in self.segments:
            segment.pack(payload, commit=self.autocommit)

    def pack(self):
        """ Pack message to binary stream. """
        payload = io.BytesIO()
        # Advance num bytes equal to header size - the header is written later
        # after the payload of all segments and parts has been written:
        payload.seek(self.header_size, io.SEEK_CUR)

        # Write out payload of segments and parts:
        self.build_payload(payload)

        packet_length = len(payload.getvalue()) - self.header_size
        self.header = MessageHeader(self.session_id, self.packet_count, packet_length, constants.MAX_SEGMENT_SIZE,
                                    num_segments=len(self.segments), packet_options=0)
        packed_header = self.header_struct.pack(*self.header)

        # Go back to begining of payload for writing message header:
        payload.seek(0)
        payload.write(packed_header)
        payload.seek(0, io.SEEK_END)

        trace(self)

        return payload

    @classmethod
    def new(cls, connection, segments=()):
        """Return a new request message instance - extracts required data from connection object
        :param connection: connection object
        :param segments: a single segment instance, or a list/tuple of segment instances
        :returns: RequestMessage instance
        """
        return cls(connection.session_id, connection.get_next_packet_count(), segments,
                   autocommit=connection.autocommit)


class ReplyMessage(BaseMessage):
    """Reply message class"""
    @classmethod
    def unpack_reply(cls, header, payload):
        """Take already unpacked header and binary payload of received request reply and creates message instance
        :param header: a namedtuple header object providing header information
        :param payload: payload (BytesIO instance) of message
        """
        reply = cls(
            header.session_id, header.packet_count,
            segments=tuple(ReplySegment.unpack_from(payload, expected_segments=header.num_segments)),
            header=header
        )
        trace(reply)
        return reply

    @classmethod
    def header_from_raw_header_data(cls, raw_header):
        """Unpack binary message header data obtained as a reply from HANA
        :param raw_header: binary string containing message header data
        :returns: named tuple for easy access of header data
        """
        try:
            header = MessageHeader(*cls.header_struct.unpack(raw_header))
        except struct.error:
            raise Exception("Invalid message header received")
        return header
