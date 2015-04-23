# Copyright 2014 SAP SE
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
from pyhdb.protocol.constants.general import MAX_MESSAGE_SIZE
from pyhdb.protocol.segments import ReplySegment


class Message(object):
    """
    Message - basic frame for sending to and receiving data from HANA db.
    """
    header_struct = struct.Struct('qiIIhb9B')  # I8 I4 UI4 UI4 I2 I1 B[9]
    header_size = header_struct.size

    _session_id = None
    _packet_count = None

    def __init__(self, connection, segments=None):
        self.connection = connection

        if segments is None:
            self.segments = []
        elif isinstance(segments, (list, tuple)):
            self.segments = segments
        else:
            self.segments = [segments]

    @property
    def session_id(self):
        """
        Identifer for session.
        """
        if self._session_id is not None:
            return self._session_id
        return self.connection.session_id

    @property
    def packet_count(self):
        """
        Sequence number for message inside of session.
        """
        if self._packet_count is None:
            self._packet_count = self.connection.get_next_packet_count()
        return self._packet_count

    def build_payload(self, payload):
        """ Build payload of message. """
        for segment in self.segments:
            segment.pack(payload, commit=self.connection.autocommit)

    def pack(self):
        """ Pack message to binary stream. """
        payload = io.BytesIO()
        # Advance num bytes equal to header size - the header is written later
        # after the payload of all segments and parts has been written:
        msg_header_size = self.header_struct.size
        payload.seek(msg_header_size, io.SEEK_CUR)

        # Write out payload of segments and parts:
        self.build_payload(payload)

        packet_length = len(payload.getvalue()) - msg_header_size
        total_space = MAX_MESSAGE_SIZE - self.header_size
        count_of_segments = len(self.segments)

        header = self.header_struct.pack(
            self.session_id,
            self.packet_count,
            packet_length,
            total_space,
            count_of_segments,
            *[0] * 10    # Reserved
        )
        # Go back to begining of payload for writing message header:
        payload.seek(0)
        payload.write(header)
        payload.seek(0, io.SEEK_END)
        return payload

    def send(self):
        """
        Send message over connection and returns the reply message.
        """
        payload = self.pack()
        # from pyhdb.lib.stringlib import humanhexlify
        # print humanhexlify(payload.getvalue())


        return self.connection._send_message(payload.getvalue())

    @classmethod
    def unpack_reply(cls, connection, header, payload):
        """
        Takes already unpacked header and binary payload of received
        reply and creates Message object.
        """
        reply = cls(
            connection,
            tuple(ReplySegment.unpack_from(
                payload, expected_segments=header[4]
            ))
        )
        reply._session_id = header[0]
        reply._packet_count = header[1]
        return reply
