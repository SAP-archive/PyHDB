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

import io
import logging
from pyhdb.protocol.headers import ReadLobHeader
from pyhdb.protocol.message import RequestMessage
from pyhdb.protocol.segments import RequestSegment
from pyhdb.protocol.constants import message_types, type_codes
from pyhdb.protocol.parts import ReadLobRequest
from pyhdb.compat import PY2, PY3, byte_type

if PY2:
    # Depending on the Python version we use different underlying containers for CLOB strings
    import StringIO
    import cStringIO
    CLOB_STRING_IO_CLASSES = (StringIO.StringIO, cStringIO.InputType, cStringIO.OutputType)

    def CLOB_STRING_IO(init_value):
        # factory function to obtain a read-writable StringIO object
        # (not possible when directly instantiated with initial value ...)
        c = cStringIO.StringIO()
        c.write(init_value)
        c.seek(0)
        return c
else:
    CLOB_STRING_IO_CLASSES = (io.StringIO, )
    CLOB_STRING_IO = io.StringIO


logger = logging.getLogger('pyhdb')

SEEK_SET = io.SEEK_SET
SEEK_CUR = io.SEEK_CUR
SEEK_END = io.SEEK_END


def from_payload(type_code, payload, connection):
    """Generator function to create lob from payload.
    Depending on lob type a BLOB, CLOB, or NCLOB instance will be returned.
    This function is usually called from types.*LobType.from_resultset()
    """
    lob_header = ReadLobHeader(payload)
    if lob_header.isnull():
        lob = None
    else:
        data = payload.read(lob_header.chunk_length)
        _LobClass = LOB_TYPE_CODE_MAP[type_code]
        lob = _LobClass.from_payload(data, lob_header, connection)
        logger.debug('Lob Header %r' % lob)
    return lob


class Lob(object):
    """Base class for all LOB classes"""

    EXTRA_NUM_ITEMS_TO_READ_AFTER_SEEK = 1024
    type_code = None
    encoding = None

    @classmethod
    def from_payload(cls, payload_data, lob_header, connection):
        enc_payload_data = cls._decode_lob_data(payload_data)
        return cls(enc_payload_data, lob_header, connection)

    @classmethod
    def _decode_lob_data(cls, payload_data):
        return payload_data.decode(cls.encoding) if cls.encoding else payload_data

    def __init__(self, init_value='', lob_header=None, connection=None):
        self.data = self._init_io_container(init_value)
        self.data.seek(0)
        self._lob_header = lob_header
        self._connection = connection
        self._current_lob_length = len(self.data.getvalue())

    @property
    def length(self):
        """Return total length of a lob.
        If a lob was received from the database the length denotes the final absolute length of the lob even if
        not all data has yet been read from the database.
        For a lob constructed from local data length represents the amount of data currently stored in it.
        """
        if self._lob_header:
            return self._lob_header.total_lob_length
        else:
            return self._current_lob_length

    def __len__(self):
        return self.length

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
        self.data.seek(offset, whence)
        new_pos = self.data.tell()
        missing_bytes_to_read = new_pos - self._current_lob_length
        if missing_bytes_to_read > 0:
            # Trying to seek beyond currently available LOB data, so need to load some more first.

            # We are smart here: (at least trying...):
            #         If a user sets a certain file position s/he probably wants to read data from
            #         there. So already read some extra data to avoid yet another immediate
            #         reading step. Try with EXTRA_NUM_ITEMS_TO_READ_AFTER_SEEK additional items (bytes/chars).

            # jump to the end of the current buffer and read the new data:
            self.data.seek(0, SEEK_END)
            self.read(missing_bytes_to_read + self.EXTRA_NUM_ITEMS_TO_READ_AFTER_SEEK)
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
        num_items_to_read = n if n != -1 else self.length - pos
        # calculate the position of the file pointer after data was read:
        new_pos = min(pos + num_items_to_read, self.length)

        if new_pos > self._current_lob_length:
            missing_num_items_to_read = new_pos - self._current_lob_length
            self._read_missing_lob_data_from_db(self._current_lob_length, missing_num_items_to_read)
        # reposition file pointer to original position as reading in IO buffer might have changed it
        self.seek(pos, SEEK_SET)
        return self.data.read(n)

    def _read_missing_lob_data_from_db(self, readoffset, readlength):
        """Read LOB request part from database"""
        logger.debug('Reading missing lob data from db. Offset: %d, readlength: %d' % (readoffset, readlength))
        lob_data = self._make_read_lob_request(readoffset, readlength)

        # make sure we really got as many items (not bytes!) as requested:
        enc_lob_data = self._decode_lob_data(lob_data)
        assert readlength == len(enc_lob_data), 'expected: %d, received; %d' % (readlength, len(enc_lob_data))

        # jump to end of data, and append new and properly decoded data to it:
        # import pdb;pdb.set_trace()
        self.data.seek(0, SEEK_END)
        self.data.write(enc_lob_data)
        self._current_lob_length = len(self.data.getvalue())

    def _make_read_lob_request(self, readoffset, readlength):
        """Make low level request to HANA database (READLOBREQUEST).
        Compose request message with proper parameters and read lob data from second part object of reply.
        """
        self._connection._check_closed()

        request = RequestMessage.new(
            self._connection,
            RequestSegment(
                message_types.READLOB,
                (ReadLobRequest(self._lob_header.locator_id, readoffset, readlength),)
            )
        )
        response = self._connection.send_request(request)

        # The segment of the message contains two parts.
        # 1) StatementContext -> ignored for now
        # 2) ReadLobReply -> contains some header information and actual LOB data
        data_part = response.segments[0].parts[1]
        # return actual lob container (BytesIO/TextIO):
        return data_part.data

    def getvalue(self):
        """Return all currently available lob data (might be shorter than the one in the database)"""
        return self.data.getvalue()

    def __str__(self):
        """Return string format - might fail for unicode data not representable as string"""
        return self.data.getvalue()

    def __repr__(self):
        if self._lob_header:
            return '<%s length: %d (currently loaded from hana: %d)>' % \
                   (self.__class__.__name__, len(self), self._current_lob_length)
        else:
            return '<%s length: %d>' % (self.__class__.__name__, len(self))

    def encode(self):
        """Encode lob data into binary format"""
        raise NotImplemented()


class Blob(Lob):
    """Instance of this class will be returned for a BLOB object in a db result"""
    type_code = type_codes.BLOB

    def _init_io_container(self, init_value):
        if isinstance(init_value, io.BytesIO):
            return init_value
        return io.BytesIO(init_value)

    def encode(self):
        return self.getvalue()


class _CharLob(Lob):
    encoding = None

    def encode(self):
        return self.getvalue().encode(self.encoding)


class Clob(_CharLob):
    """Instance of this class will be returned for a CLOB object in a db result"""
    type_code = type_codes.CLOB
    encoding = 'ascii'

    def __unicode__(self):
        """Convert lob into its unicode format"""
        return self.data.getvalue().decode(self.encoding)

    def _init_io_container(self, init_value):
        """Initialize container to hold lob data.
        Here either a cStringIO or a io.StringIO class is used depending on the Python version.
        For CLobs ensure that an initial unicode value only contains valid ascii chars.
        """
        if isinstance(init_value, CLOB_STRING_IO_CLASSES):
            # already a valid StringIO instance, just use it as it is
            v = init_value
        else:
            # works for strings and unicodes. However unicodes must only contain valid ascii chars!
            if PY3:
                # a io.StringIO also accepts any unicode characters, but we must be sure that only
                # ascii chars are contained. In PY2 we use a cStringIO class which complains by itself
                # if it catches this case, so in PY2 no extra check needs to be performed here.
                init_value.encode('ascii')  # this is just a check, result not needed!
            v = CLOB_STRING_IO(init_value)
        return v


class NClob(_CharLob):
    """Instance of this class will be returned for a NCLOB object in a db result"""
    type_code = type_codes.NCLOB
    encoding = 'utf8'

    def __unicode__(self):
        """Convert lob into its unicode format"""
        return self.data.getvalue()

    def _init_io_container(self, init_value):
        if isinstance(init_value, io.StringIO):
            return init_value

        if PY2 and isinstance(init_value, str):
            # io.String() only accepts unicode values, so do necessary conversion here:
            init_value = init_value.decode(self.encoding)
        if PY3 and isinstance(init_value, byte_type):
            init_value = init_value.decode(self.encoding)

        return io.StringIO(init_value)


LOB_TYPE_CODE_MAP = {
    type_codes.BLOB: Blob,
    type_codes.CLOB: Clob,
    type_codes.NCLOB: NClob,
}
