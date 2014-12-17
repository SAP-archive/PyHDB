import os
import socket
import struct
import threading
from io import BytesIO

from pyhdb.auth import AuthManager
from pyhdb.cursor import Cursor
from pyhdb.exceptions import Error, OperationalError, ConnectionTimedOutError
from pyhdb.protocol.base import Message, RequestSegment
from pyhdb.protocol.parts import Authentication, ClientId, ConnectOptions
from pyhdb.protocol.constants import message_types, function_codes, \
    DEFAULT_CONNECTION_OPTIONS

INITIALIZATION_BYTES = bytearray([
    255, 255, 255, 255, 4, 20, 0, 4, 1, 0, 0, 1, 1, 1
])

version_struct = struct.Struct('<bH')

class Connection(object):

    def __init__(self, host, port, user, password, autocommit=False,
                 timeout=None):
        self.host = host
        self.port = port

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

    def _open_socket_and_init_protocoll(self):
        self._socket = socket.create_connection(
            (self.host, self.port), self._timeout
        )

        # Initialization Handshake
        self._socket.sendall(INITIALIZATION_BYTES)

        response = self._socket.recv(8)
        if len(response) != 8:
            raise Exception("Connection failed")

        self.product_version = version_struct.unpack(response[0:3])
        self.protocol_version = version_struct.unpack_from(response[3:8])

    def _send_message(self, packed_message):
        """
        Private method to send packed message and receive
        the reply message.
        """
        try:
            with self._socket_lock:
                self._socket.sendall(packed_message)

                # Read first message header
                header = self._socket.recv(32)
                try:
                    header = Message.struct.unpack(header)
                except struct.error:
                    raise Exception("Invalid message header received")

                # Receive complete message payload
                payload = b""
                received = 0
                while received < header[2]:
                    _payload = self._socket.recv(header[2] - received)
                    if not _payload:
                        break
                    payload += _payload
                    received += len(_payload)

                payload = BytesIO(payload)

                # Keep session id of connection up to date
                if self.session_id != header[0]:
                    self.session_id = header[0]
                    self.packet_count = -1
        except socket.timeout:
            raise ConnectionTimedOutError()
        except (IOError, OSError) as error:
            raise OperationalError(
                "Lost connection to HANA server (%r)" % error
            )

        return Message.unpack_reply(self, header, payload)

    def Message(self, *args, **kwargs):
        return Message(self, *args, **kwargs)

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

            reply = self.Message(
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
            ).send()

    def close(self):
        with self._socket_lock:
            if self._socket is None:
                raise Error("Connection already closed")

            try:
                reply = self.Message(
                    RequestSegment(message_types.DISCONNECT)
                ).send()

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

        self.Message(
            RequestSegment(message_types.COMMIT)
        ).send()

    def rollback(self):
        self._check_closed()

        self.Message(
            RequestSegment(message_types.ROLLBACK)
        ).send()

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
