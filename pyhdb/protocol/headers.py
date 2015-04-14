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

import struct


class LobHeader(object):
    """
    LOB header structure (incomplete in Command Network Protocol Reference docs):
    Total header size is 32 bytes. The first columns denotes the offset:

    00: TYPE:    I1       Type of data
    01: OPTIONS: I1       Options that further refine the descriptor
    -> no further data to be read for LOB if options->isNull is true
    02: RESERVED: I2      (ignore this)
    04: CHARLENGTH: I8    Length of string (for asci and unicode)
    12: BYTELENGTH: I8    Number of bytes of LOB
    20: LOCATORID: B8     8 bytes serving as locator id for LOB
    28: CHUNKLENGTH: I4   Number of bytes of LOB chunk in this result set
    32: LOB data if CHUNKLENGTH > 0
    """
    BLOB_TYPE = 1
    CLOB_TYPE = 2
    NCLOB_TYPE = 3

    # Bit masks for LOB options (field 2 in header):
    LOB_OPTION_ISNULL = 0x01
    LOB_OPTION_DATAINCLUDED = 0x02
    LOB_OPTION_LASTDATA = 0x04

    OPTIONS_STR = {
        LOB_OPTION_ISNULL: 'isnull',
        LOB_OPTION_DATAINCLUDED: 'data_included',
        LOB_OPTION_LASTDATA: 'last_data'
    }

    blob_struct_part1 = struct.Struct('<BB')       # read blob type and 'options' field
    blob_struct_part2 = struct.Struct('<2sQQ8sI')  # only read if blob is not null (see options field)

    def __init__(self, payload):
        """Parse LOB header from payload"""
        raw_header_p1 = payload.read(self.blob_struct_part1.size)
        self.lob_type, self.options = self.blob_struct_part1.unpack(raw_header_p1)

        if not self.isnull():
            raw_header_p2 = payload.read(self.blob_struct_part2.size)
            header = self.blob_struct_part2.unpack(raw_header_p2)
            (reserved, self.char_length, self.byte_length, self.locator_id, self.chunk_length) = header
            # print 'raw lob header: %s' % pyhdb.lib.allhexlify(raw_header_p1 + raw_header_p2)

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
        return value
