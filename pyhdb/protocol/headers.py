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

import collections
import struct
###
from pyhdb.protocol.constants import type_codes


MessageHeader = collections.namedtuple(
    'MessageHeader', 'session_id, packet_count, payload_length, varpartsize, num_segments, packet_options')


RequestSegmentHeader = collections.namedtuple(
    'RequestSegmentHeader',
    'segment_length, segment_offset, num_parts, segment_number, segment_kind, message_type, commit, command_options')


ReplySegmentHeader = collections.namedtuple(
    'ReplySegmentHeader',
    'segment_length, segment_offset, num_parts, segment_number, segment_kind, function_code')


PartHeader = collections.namedtuple(
    'PartHeader',
    'part_kind, part_attributes, argument_count, bigargument_count, payload_size, remaining_buffer_size')


class BaseLobheader(object):
    """Base LobHeader class"""
    BLOB_TYPE = 1
    CLOB_TYPE = 2
    NCLOB_TYPE = 3

    LOB_TYPES = {type_codes.BLOB: BLOB_TYPE, type_codes.CLOB: CLOB_TYPE, type_codes.NCLOB: NCLOB_TYPE}

    # Bit masks for LOB options (field 2 in header):
    LOB_OPTION_ISNULL = 0x01
    LOB_OPTION_DATAINCLUDED = 0x02
    LOB_OPTION_LASTDATA = 0x04

    OPTIONS_STR = {
        LOB_OPTION_ISNULL: 'isnull',
        LOB_OPTION_DATAINCLUDED: 'data_included',
        LOB_OPTION_LASTDATA: 'last_data'
    }


class WriteLobHeader(BaseLobheader):
    """Write-LOB header structure used when sending data to Hana.
    Total header size is 10 bytes.
    Note that the lob data does not come immediately after the lob header but AFTER all rowdata headers
    have been written to the part header!!!

    00: TYPECODE: I1
    01: OPTIONS: I1       Options that further refine the descriptor
    02: LENGTH: I4        Length of bytes of data that follows
    06: POSITION: I4      Position P of the lob data in the part (startinb at the beginning of the part)
    ...
    P:  LOB data
    """
    header_struct = struct.Struct('<BBII')


class ReadLobHeader(BaseLobheader):
    """
    Read-LOB header structure used when receiving data from Hana.
    (incomplete in Command Network Protocol Reference docs):
    Total header size is 32 bytes. The first columns denotes the offset:

    00: TYPE:    I1       Type of data
    01: OPTIONS: I1       Options that further refine the descriptor
    -> no further data to be read for LOB if options->is_null is true
    02: RESERVED: I2      (ignore this)
    04: CHARLENGTH: I8    Length of string (for asci and unicode)
    12: BYTELENGTH: I8    Number of bytes of LOB
    20: LOCATORID: B8     8 bytes serving as locator id for LOB
    28: CHUNKLENGTH: I4   Number of bytes of LOB chunk in this result set
    32: LOB data if CHUNKLENGTH > 0
    """
    header_struct_part1 = struct.Struct('<BB')       # read blob type and 'options' field
    header_struct_part2 = struct.Struct('<2sQQ8sI')  # only read if blob is not null (see options field)

    def __init__(self, payload):
        """Parse LOB header from payload"""
        raw_header_p1 = payload.read(self.header_struct_part1.size)
        self.lob_type, self.options = self.header_struct_part1.unpack(raw_header_p1)

        if not self.isnull():
            raw_header_p2 = payload.read(self.header_struct_part2.size)
            header = self.header_struct_part2.unpack(raw_header_p2)
            (reserved, self.char_length, self.byte_length, self.locator_id, self.chunk_length) = header

            # Set total_lob_length attribute differently for binary and character lobs:
            self.total_lob_length = self.byte_length if self.lob_type == self.BLOB_TYPE else self.char_length

    def isnull(self):
        return bool(self.options & self.LOB_OPTION_ISNULL)

    def __str__(self):
        """Return a string of properly formatted header values"""
        O = self.OPTIONS_STR
        options = [O[o] for o in sorted(self.OPTIONS_STR.keys()) if o & self.options]
        options_str = ', '.join(options)
        value = 'type: %d, options %d (%s)' % (self.lob_type, self.options, options_str)
        if not self.isnull():
            value += ', charlength: %d, bytelength: %d, locator_id: %r, chunklength: %d' % \
                (self.char_length, self.byte_length, self.locator_id, self.chunk_length)
        return '<ReadLobHeader %s>' % value
