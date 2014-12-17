import os
import struct
import hashlib
import hmac
from io import BytesIO

from pyhdb.protocol.base import RequestSegment, Part
from pyhdb.protocol.constants import message_types
from pyhdb.protocol.parts import Authentication, Fields
from pyhdb._compat import iter_range

CLIENT_PROOF_SIZE = 32
CLIENT_KEY_SIZE = 64

class AuthManager(object):

    def __init__(self, connection, user, password):
        self.connection = connection
        self.user = user
        self.password = password

        self.method = b"SCRAMSHA256"
        self.client_key = os.urandom(CLIENT_KEY_SIZE)
        self.client_proof = None

    def perform_handshake(self):
        response = self.connection.Message(
            RequestSegment(
                message_types.AUTHENTICATE,
                Authentication(self.user, {self.method: self.client_key})
            )
        ).send()

        auth_part = response.segments[0].parts[0]
        if self.method not in auth_part.methods:
            raise Exception(
                "Only unknown authentication methods available: %s" %
                b",".join(auth_part.methods.keys())
            )

        salt, server_key = Fields.unpack_data(
            BytesIO(auth_part.methods[self.method])
        )

        self.client_proof = self.calculate_client_proof([salt], server_key)
        return Authentication(self.user, {'SCRAMSHA256': self.client_proof})

    def calculate_client_proof(self, salts, server_key):
        proof = b"\x00"
        proof += struct.pack('b', len(salts))

        for salt in salts:
            proof += struct.pack('b', CLIENT_PROOF_SIZE)
            proof += self.scramble_salt(salt, server_key)

        return proof

    def scramble_salt(self, salt, server_key):
        msg = salt + server_key + self.client_key

        key = hashlib.sha256(
            hmac.new(
                self.password.encode('cesu-8'), salt, hashlib.sha256
            ).digest()
        ).digest()
        key_hash = hashlib.sha256(key).digest()

        sig = hmac.new(
            key_hash, msg, hashlib.sha256
        ).digest()

        return self._xor(sig, key)

    def _xor(self, a, b):
        a = bytearray(a)
        b = bytearray(b)
        result = bytearray(len(a))
        for i in iter_range(len(a)):
            result[i] += a[i] ^ b[i]
        return bytes(result)
