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
import logging
from io import BytesIO
###
from pyhdb.protocol.constants import part_kinds
from pyhdb.protocol.constants.general import MAX_MESSAGE_SIZE, MESSAGE_HEADER_SIZE
from pyhdb.protocol.parts import Part

MAX_SEGMENT_SIZE = MAX_MESSAGE_SIZE - MESSAGE_HEADER_SIZE

recv_log = logging.getLogger('receive')
debug = recv_log.debug


class BaseSegment(object):
    """
    Base class for request and reply segments
    """
    header_struct = struct.Struct('<iihhb')  # I4 I4 I2 I2 I1
    header_size = header_struct.size
    segment_kind = None

    def __init__(self, parts=None):
        if parts is None:
            self.parts = []
        elif isinstance(parts, (list, tuple)):
            self.parts = parts
        else:
            self.parts = [parts]

    @property
    def offset(self):
        return 0

    @property
    def number(self):
        return 1

    def build_payload(self, payload):
        """Build payload of all parts and write them into the payload buffer"""
        remaining_size = MAX_SEGMENT_SIZE - self.header_size

        for part in self.parts:
            part_payload = part.pack(remaining_size)
            payload.write(part_payload)
            remaining_size -= len(part_payload)

    def pack(self, payload, **kwargs):

        # remember position in payload object:
        segment_payload_start_pos = payload.tell()

        # Advance num bytes equal to header size. The header is written later
        # after the payload of all segments and parts has been written:
        payload.seek(self.header_size, io.SEEK_CUR)

        # Write out payload of parts:
        self.build_payload(payload)
        payload_length = payload.tell() - segment_payload_start_pos  # calc length of parts payload

        header = self.header_struct.pack(
            payload_length,
            self.offset,
            len(self.parts),
            self.number,
            self.segment_kind
        ) + self.pack_additional_header(**kwargs)

        # Go back to beginning of payload header for writing segment header:
        payload.seek(segment_payload_start_pos)
        payload.write(header)
        # Put file pointer at the end of the bffer so that next segment can be appended:
        payload.seek(0, io.SEEK_END)

    def pack_additional_header(self, **kwargs):
        raise NotImplemented


class RequestSegment(BaseSegment):
    """
    Request segment class - used for sending request messages to HANA db
    """
    segment_kind = 1
    request_header_struct = struct.Struct('bbb8x')  # I1 I1 I1 B[8]
    header_size = BaseSegment.header_struct.size + request_header_struct.size

    def __init__(self, message_type, parts=None):
        super(RequestSegment, self).__init__(parts)
        self.message_type = message_type

    @property
    def command_options(self):
        return 0

    def pack_additional_header(self, **kwargs):
        return self.request_header_struct.pack(
            self.message_type,
            int(kwargs.get('commit', 0)),
            self.command_options
        )


class ReplySegment(BaseSegment):
    """
    Reqply segment class - used when receiving messages from HANA db
    """
    segment_kind = 2
    reply_header_struct = struct.Struct('<bh8B')  # I1 I2 B[8]
    header_size = BaseSegment.header_struct.size + reply_header_struct.size

    def __init__(self, function_code, parts=None):
        super(ReplySegment, self).__init__(parts)
        self.function_code = function_code

    @classmethod
    def unpack_from(cls, payload, expected_segments):
        num_segments = 0

        while num_segments < expected_segments:
            try:
                base_segment_header = cls.header_struct.unpack(
                    payload.read(13)
                )
            except struct.error:
                raise Exception("No valid segment header")

            # Read additional header fields
            try:
                segment_header = \
                    base_segment_header + cls.reply_header_struct.unpack(
                        payload.read(11)
                    )
            except struct.error:
                raise Exception("No valid reply segment header")

            msg = 'Segment Header (%d/%d, 24 bytes): segmentlength: %d, ' \
                  'segmentofs: %d, noofparts: %d, segmentno: %d, rserved: %d,' \
                  ' segmentkind: %d, functioncode: %d'
            debug(msg, num_segments+1, expected_segments, *segment_header[:7])
            if expected_segments == 1:
                # If we just expects one segment than we can take the full
                # payload. This also a workaround of an internal bug.
                segment_payload_size = -1
            else:
                segment_payload_size = segment_header[0] - cls.header_size

            # Determinate segment payload
            pl = payload.read(segment_payload_size)
            segment_payload = BytesIO(pl)
            debug('Read %d bytes payload segment %d', len(pl), num_segments + 1)

            num_segments += 1

            if base_segment_header[4] == 2:  # Reply segment
                yield ReplySegment.unpack(segment_header, segment_payload)
            elif base_segment_header[4] == 5:  # Error segment
                error = ReplySegment.unpack(segment_header, segment_payload)
                if error.parts[0].kind == part_kinds.ROWSAFFECTED:
                    raise Exception("Rows affected %s" % (error.parts[0].values,))
                elif error.parts[0].kind == part_kinds.ERROR:
                    raise error.parts[0].errors[0]
            else:
                raise Exception("Invalid reply segment")

    @classmethod
    def unpack(cls, header, payload):
        """
        Takes unpacked header and payload of segment and
        create ReplySegment object.
        """
        return cls(
            header[6],
            tuple(Part.unpack_from(payload, expected_parts=header[2]))
        )
