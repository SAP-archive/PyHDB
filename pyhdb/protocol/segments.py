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
from pyhdb.compat import iter_range
from pyhdb.protocol import constants
from pyhdb.protocol.parts import Part
from pyhdb.protocol.headers import RequestSegmentHeader, ReplySegmentHeader
from pyhdb.protocol.constants import segment_kinds


logger = logging.getLogger('pyhdb')
debug = logger.debug


class BaseSegment(object):
    """
    Base class for request and reply segments
    """
    base_header_struct_fmt = '<iihh'  # I4 I4 I2 I2
    segment_kind = None
    __tracing_attrs__ = ['header', 'parts']

    def __init__(self, parts=None, header=None):
        if parts is None:
            self.parts = []
        elif isinstance(parts, (list, tuple)):
            self.parts = parts
        else:
            self.parts = [parts]
        self.header = header


class RequestSegment(BaseSegment):
    """
    Request segment class - used for sending request messages to HANA db
    """
    segment_kind = segment_kinds.REQUEST
    header_struct = struct.Struct(BaseSegment.base_header_struct_fmt + 'bbbb8x')  # + I1 I1 I1 I1 x[8]
    header_size = header_struct.size
    MAX_SEGMENT_PAYLOAD_SIZE = constants.MAX_SEGMENT_SIZE - header_size

    def __init__(self, message_type, parts=None, header=None):
        super(RequestSegment, self).__init__(parts, header)
        self.message_type = message_type

    @property
    def command_options(self):
        return 0

    @property
    def offset(self):
        return 0

    @property
    def number(self):
        return 1

    def build_payload(self, payload):
        """Build payload of all parts and write them into the payload buffer"""
        remaining_size = self.MAX_SEGMENT_PAYLOAD_SIZE

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

        # Generate payload of parts:
        self.build_payload(payload)

        segment_length = payload.tell() - segment_payload_start_pos  # calc length of parts payload
        self.header = RequestSegmentHeader(segment_length, self.offset, len(self.parts), self.number, self.segment_kind,
                                           self.message_type, int(kwargs.get('commit', 0)), self.command_options)
        packed_header = self.header_struct.pack(*self.header)

        # Go back to beginning of payload header for writing segment header:
        payload.seek(segment_payload_start_pos)
        payload.write(packed_header)
        # Put file pointer at the end of the bffer so that next segment can be appended:
        payload.seek(0, io.SEEK_END)


class ReplySegment(BaseSegment):
    """
    Reqply segment class - used when receiving messages from HANA db
    """
    segment_kind = segment_kinds.REPLY
    header_struct = struct.Struct(BaseSegment.base_header_struct_fmt + 'bxh8x')  # basesize + I1 x I2 x[8]
    header_size = header_struct.size

    def __init__(self, function_code, parts=None, header=None):
        super(ReplySegment, self).__init__(parts, header)
        self.function_code = function_code

    @classmethod
    def unpack_from(cls, payload, expected_segments):

        for num_segment in iter_range(expected_segments):
            try:
                segment_header = ReplySegmentHeader(*cls.header_struct.unpack(payload.read(cls.header_size)))
            except struct.error:
                raise Exception("No valid segment header")

            debug('%s (%d/%d): %s', cls.__name__, num_segment + 1, expected_segments, str(segment_header))
            if expected_segments == 1:
                # If we just expects one segment than we can take the full payload.
                # This also a workaround of an internal bug (Which bug?)
                segment_payload_size = -1
            else:
                segment_payload_size = segment_header.segment_length - cls.header_size

            # Determinate segment payload
            pl = payload.read(segment_payload_size)
            segment_payload = BytesIO(pl)
            debug('Read %d bytes payload segment %d', len(pl), num_segment + 1)

            parts = tuple(Part.unpack_from(segment_payload, expected_parts=segment_header.num_parts))
            segment = cls(segment_header.function_code, parts, header=segment_header)

            if segment_header.segment_kind == segment_kinds.REPLY:
                yield segment
            elif segment_header.segment_kind == segment_kinds.ERROR:
                error = segment
                if error.parts[0].kind == part_kinds.ROWSAFFECTED:
                    raise Exception("Rows affected %s" % (error.parts[0].values,))
                elif error.parts[0].kind == part_kinds.ERROR:
                    raise error.parts[0].errors[0]
            else:
                raise Exception("Invalid reply segment")
