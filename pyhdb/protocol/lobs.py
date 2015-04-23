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

import io
import logging
from headers import ReadLobHeader
from pyhdb.protocol.message import Message
from pyhdb.protocol.segments import RequestSegment
from pyhdb.protocol.constants import message_types, type_codes
from pyhdb.protocol.parts import ReadLobRequest

recv_log = logging.getLogger('receive')

SEEK_SET = io.SEEK_SET
SEEK_CUR = io.SEEK_CUR
SEEK_END = io.SEEK_END


def from_payload(type_code, payload, connection):
    """Generator function to create lob from payload.
    Depending on lob type a BLOB, CLOB, or NCLOB instance will be returned.
    """
    lob_header = ReadLobHeader(payload)
    if lob_header.isnull():
        lob = None
    else:
        data = payload.read(lob_header.chunk_length)
        # print 'raw lob data: %r' % data
        _LobClass = LOB_TYPE_CODE_MAP[type_code]
        lob = _LobClass.from_payload(data, lob_header, connection)
        recv_log.debug('Lob Header %s' % str(lob))
    return lob


class Lob(object):
    """Base class for all LOB classes"""

    EXTRA_NUM_ITEMS_TO_READ_AFTER_SEEK = 1024
    type_code = None
    _IO_Class = None

    @classmethod
    def from_payload(cls, lob_header, payload_data, connection):
        raise NotImplemented()

    def __init__(self, init_value='', lob_header=None, connection=None):
        self.data = self._init_io_container(init_value)
        self.lob_header = lob_header
        self.connection = connection
        self.data.seek(0)
        self._lob_length = len(self.data.getvalue())
        # assert self._lob_length == self.lob_header.chunk_length  # just to be sure ;-)

    def _init_io_container(self, init_value):
        raise NotImplemented()

    def tell(self):
        """Return position of pointer in lob buffer"""
        return self.data.tell()

    def seek(self, offset, whence=SEEK_SET):
        """Seek pointer in lob data buffer to requested position.
        Might trigger further loading of data from the database if the pointer is beyond currently read data.
        """
        # A nice trick is to (ab)use BytesIO.seek() to go to the desired position for easier calculation.
        # This will not add any data to the buffer however - very convenient!
        new_pos = self.data.seek(offset, whence)
        missing_bytes_to_read = new_pos - self._lob_length
        if missing_bytes_to_read > 0:
            # Trying to seek beyond currently available LOB data, so need to load some more first.

            # We are smart here: (at least trying...):
            #         If a user sets a certain file position s/he probably wants to read data from
            #         there. So already read some extra data to avoid yet another immediate
            #         reading step. Try with EXTRA_NUM_ITEMS_TO_READ_AFTER_SEEK additional items (bytes/chars).
            self._read_missing_lob_data_from_db(self._lob_length,
                                                missing_bytes_to_read + self.EXTRA_NUM_ITEMS_TO_READ_AFTER_SEEK)
            # reposition file pointer a originally desired position:
            self.data.seek(new_pos)
        return new_pos

    def read(self, n=-1):
        """Read up to n items (bytes/chars) from the lob and return them.
        If n is -1 then all available data is returned.
        Might trigger further loading of data from the database if the number of items requested for
        reading is larger than what is currently buffered.
        """
        pos = self.tell()
        num_items_to_read = n if n != -1 else self.lob_header.total_lob_length - pos
        # calculate the position of the file pointer after data was read:
        new_pos = min(pos + num_items_to_read, self.lob_header.total_lob_length)

        if new_pos > self._lob_length:
            missing_num_items_to_read = new_pos - self._lob_length
            self._read_missing_lob_data_from_db(self._lob_length, missing_num_items_to_read)
        # reposition file pointer to original position as reading in IO buffer might have changed it
        self.seek(pos, SEEK_SET)
        return self.data.read(n)

    def _read_missing_lob_data_from_db(self, readoffset, readlength):
        """Read LOB request part from database"""
        lob_data = self._make_read_lob_request(readoffset, readlength)
        # make sure we really got as many bytes as requested:
        assert readlength == len(lob_data)

        # jump to end of data, and append new to it:
        self.data.seek(0, SEEK_END)
        self.data.write(lob_data)
        self._lob_length = len(self.data.getvalue())

    def _make_read_lob_request(self, readoffset, readlength):
        """Make low level request to HANA database (READLOBREQUEST).
        Compose request message with proper parameters and read lob data from second part object of reply.
        """
        self.connection._check_closed()

        request = Message.new_request(
            self.connection,
            RequestSegment(
                message_types.READLOB,
                (ReadLobRequest(self.lob_header.locator_id, readoffset, readlength),)
            )
        ).send()
        response = self.connection.send_request(request)

        # The segment of the message contains two parts.
        # 1) StatementContext -> ignored for now
        # 2) ReadLobReply -> contains some header information and actual LOB data
        data_part = response.segments[0].parts[1]
        # return actual lob container (BytesIO/TextIO):
        return data_part.data

    def getvalue(self):
        """Return all currently available lob data (might be shorter than the one in the database)"""
        return self.data.getvalue()

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, str(self.lob_header))

    def __str__(self):
        """Convert lob into its string/unicode format"""
        return self.encode()

    def encode(self):
        """Encode lob data into binary format"""
        raise NotImplemented()


class Blob(Lob):
    """Instance of this class will be returned for a BLOB object in a db result"""
    type_code = type_codes.BLOB

    @classmethod
    def from_payload(cls, payload_data, lob_header, connection):
        return cls(payload_data, lob_header, connection)

    def _init_io_container(self, init_value):
        if isinstance(init_value, io.BytesIO):
            return init_value
        return io.BytesIO(init_value)

    def encode(self):
        return self.getvalue()


class _CharLob(Lob):
    encoding = None

    @classmethod
    def from_payload(cls, payload_data, lob_header, connection):
        unicode_value = payload_data.decode(cls.encoding)
        return cls(unicode_value, lob_header, connection)

    def _init_io_container(self, init_value):
        if isinstance(init_value, io.StringIO):
            return init_value
        if isinstance(init_value, str):
            init_value = init_value.decode(self.encoding)
        return io.StringIO(init_value)

    def encode(self):
        return self.getvalue().encode(self.encoding)


class Clob(_CharLob):
    """Instance of this class will be returned for a CLOB object in a db result"""
    type_code = type_codes.CLOB
    encoding = 'ascii'


class NClob(_CharLob):
    """Instance of this class will be returned for a NCLOB object in a db result"""
    type_code = type_codes.NCLOB
    encoding = 'utf8'


LOB_TYPE_CODE_MAP = {
    type_codes.BLOB: Blob,
    type_codes.CLOB: Clob,
    type_codes.NCLOB: NClob,
}
