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
import os
import socket
import struct
import threading
import logging
###
from pyhdb.auth import AuthManager
from pyhdb.cursor import Cursor
from pyhdb.exceptions import Error, OperationalError, ConnectionTimedOutError
from pyhdb.protocol.segments import RequestSegment
from pyhdb.protocol.message import RequestMessage, ReplyMessage
from pyhdb.protocol.parts import ClientId, ConnectOptions
from pyhdb.protocol.constants import message_types, function_codes, DEFAULT_CONNECTION_OPTIONS

INITIALIZATION_BYTES = bytearray([
    255, 255, 255, 255, 4, 20, 0, 4, 1, 0, 0, 1, 1, 1
])

logger = logging.getLogger('pyhdb')
debug = logger.debug
version_struct = struct.Struct('<bH')


class Connection(object):
    """
    Database connection class
    """
    def __init__(self, host, port, user, password, autocommit=False, timeout=None):
        self.host = host
        self.port = port
        self.user = user

        self.autocommit = autocommit
        self.product_version = None
        self.protocol_version = None

        self.session_id = -1
        self.packet_count = -1

        self._socket = None
        self._timeout = timeout
        self._auth_manager = AuthManager(self, user, password)
        # It feels like the RLock has a poorer performance
        self._socket_lock = threading.RLock()
        self._packet_count_lock = threading.Lock()

    def __repr__(self):
        return '<Hana connection host=%s port=%s user=%s>' % (self.host, self.port, self.user)

    def _open_socket_and_init_protocoll(self):
        self._socket = socket.create_connection((self.host, self.port), self._timeout)

        # Initialization Handshake
        self._socket.sendall(INITIALIZATION_BYTES)

        response = self._socket.recv(8)
        if len(response) != 8:
            raise Exception("Connection failed")

        self.product_version = version_struct.unpack(response[0:3])
        self.protocol_version = version_struct.unpack_from(response[3:8])

    def send_request(self, message):
        """Send message request to HANA db and return reply message
        :param message: Instance of Message object containing segments and parts of a HANA db request
        :returns: Instance of reply Message object
        """
        payload = message.pack()  # obtain BytesIO instance
        return self.__send_message_recv_reply(payload.getvalue())

    def __send_message_recv_reply(self, packed_message):
        """
        Private method to send packed message and receive the reply message.
        :param packed_message: a binary string containing the entire message payload
        """
        payload = io.BytesIO()
        try:
            with self._socket_lock:
                self._socket.sendall(packed_message)

                # Read first message header
                raw_header = self._socket.recv(32)
                header = ReplyMessage.header_from_raw_header_data(raw_header)

                # from pyhdb.lib.stringlib import allhexlify
                # print 'Raw msg header:', allhexlify(raw_header)
                msg = 'Message header (32 bytes): sessionid: %d, packetcount: %d, length: %d, size: %d, noofsegm: %d'
                debug(msg, *(header[:5]))

                # Receive complete message payload
                while payload.tell() < header.payload_length:
                    _payload = self._socket.recv(header.payload_length - payload.tell())
                    if not _payload:
                        break   # jump out without any warning??
                    payload.write(_payload)

                debug('Read %d bytes payload from socket', payload.tell())

                # Keep session id of connection up to date
                if self.session_id != header.session_id:
                    self.session_id = header.session_id
                    self.packet_count = -1
        except socket.timeout:
            raise ConnectionTimedOutError()
        except (IOError, OSError) as error:
            raise OperationalError("Lost connection to HANA server (%r)" % error)

        payload.seek(0)  # set pointer position to beginning of buffer
        return ReplyMessage.unpack_reply(header, payload)

    def get_next_packet_count(self):
        with self._packet_count_lock:
            self.packet_count += 1
            return self.packet_count

    def connect(self):
        with self._socket_lock:
            if self._socket is not None:
                # Socket already established
                return

            self._open_socket_and_init_protocoll()

            # Perform the authenication handshake and get the part
            # with the agreed authentication data
            agreed_auth_part = self._auth_manager.perform_handshake()

            request = RequestMessage.new(
                self,
                RequestSegment(
                    message_types.CONNECT,
                    (
                        agreed_auth_part,
                        ClientId(
                            "pyhdb-%s@%s" % (os.getpid(), socket.getfqdn())
                        ),
                        ConnectOptions(DEFAULT_CONNECTION_OPTIONS)
                    )
                )
            )
            self.send_request(request)

    def close(self):
        with self._socket_lock:
            if self._socket is None:
                raise Error("Connection already closed")

            try:
                request = RequestMessage.new(
                    self,
                    RequestSegment(message_types.DISCONNECT)
                )
                reply = self.send_request(request)
                if reply.segments[0].function_code != \
                   function_codes.DISCONNECT:
                    raise Error("Connection wasn't closed correctly")
            finally:
                self._socket.close()
                self._socket = None

    @property
    def closed(self):
        return self._socket is None

    def _check_closed(self):
        if self.closed:
            raise Error("Connection closed")

    def cursor(self):
        """Return a new Cursor Object using the connection."""
        self._check_closed()

        return Cursor(self)

    def commit(self):
        self._check_closed()

        request = RequestMessage.new(
            self,
            RequestSegment(message_types.COMMIT)
        )
        self.send_request(request)

    def rollback(self):
        self._check_closed()

        request = RequestMessage.new(
            self,
            RequestSegment(message_types.ROLLBACK)
        )
        self.send_request(request)

    @property
    def timeout(self):
        if self._socket:
            return self._socket.gettimeout()
        return self._timeout

    @timeout.setter
    def timeout(self, value):
        self._timeout = value
        if self._socket:
            self._socket.settimeout(value)

    # Methods for compatibility with hdbclient
    def getautocommit(self):
        return self.autocommit

    def setautocommit(self, auto=True):
        self.autocommit = auto

    def isconnected(self):
        return self._socket is not None
