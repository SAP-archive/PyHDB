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
from headers import LobHeader

recv_log = logging.getLogger('receive')

SEEK_SET = io.SEEK_SET
SEEK_CUR = io.SEEK_CUR
SEEK_END = io.SEEK_END


def from_payload(payload, connection):
    """Generator function to create lob from payload.
    Depending on lob type a BLOB, CLOB, or NCLOB instance will be returned.
    """
    lob_header = LobHeader(payload)
    data = payload.read(lob_header.chunk_length) if not lob_header.isnull() else None
    # print 'raw lob data: %r' % data
    LobClass = LOB_TYPE_MAP[lob_header.lob_type]
    lob = LobClass(connection, lob_header, init_value=data)
    recv_log.debug('Lob Header %s' % str(lob))
    return lob


class Lob(object):
    """Base class for all LOB classes"""

    IO_Class = io.BytesIO  # should be overridden in subclass

    def __init__(self, connection, lob_header, init_value=''):
        self.connection = connection
        self.lob_header = lob_header
        self.data = self.IO_Class(init_value)
        self.data.seek(0)
        if not self.isnull:
            self._lob_length = len(self.data.getvalue())
            assert self._lob_length == self.lob_header.chunk_length  # just to be sure ;-)

    @property
    def isnull(self):
        return self.lob_header.isnull()

    def seek(self, offset, whence=SEEK_SET):
        if not self.isnull:
            self.data.seek(offset, whence)          # todo: handle case that data is not yet read from db

    def tell(self):
        return self.data.tell() if not self.isnull else None

    def read(self, n=-1):
        """Read up to n bytes from the lob and return them.
        If n is -1 then all available data is returned.
        """
        if self.isnull:
            return None

        pos = self.tell()
        bytes_to_read = n if n != -1 else self.lob_header.byte_length - pos
        # calculate the position of the file pointer after data was read:
        new_pos = min(pos + bytes_to_read, self.lob_header.byte_length)

        if new_pos > self._lob_length:
            missing_bytes_to_read = new_pos - self._lob_length
            self._read_missing_lob_data_from_db(self._lob_length, missing_bytes_to_read)
        # reposition file pointer to original position as reading in IO buffer might have changed it
        self.seek(pos, SEEK_SET)
        return self.data.read(n)

    def _read_missing_lob_data_from_db(self, readoffset, readlength):
        """Read LOB request part from database"""
        cursor = self.connection.cursor()
        readlobreply_part = cursor._read_lob_request(self.lob_header.locator_id, readoffset, readlength)
        # make sure we really got as many bytes as requested:
        assert readlength == len(readlobreply_part.data)

        # jump to end of data, and append new to it:
        self.data.seek(0, SEEK_END)
        self.data.write(readlobreply_part.data)
        self._lob_length = len(self.data.getvalue())

    def getvalue(self):
        if self.isnull:
            return None
        return self.data.getvalue() if not self.isnull else None

    def __str__(self):
        return '%s: %s' % (self.__class__.__name__, str(self.lob_header))


class Blob(Lob):
    """Instance of this class will be returned for a BLOB object in a db result"""
    IO_Class = io.BytesIO


class Clob(Lob):
    """Instance of this class will be returned for a CLOB object in a db result"""
    IO_Class = io.StringIO


class NClob(Lob):
    """Instance of this class will be returned for a NCLOB object in a db result"""
    IO_Class = io.StringIO


LOB_TYPE_MAP = {
    LobHeader.BLOB_TYPE: Blob,
    LobHeader.CLOB_TYPE: Clob,
    LobHeader.NCLOB_TYPE: NClob,
}
