import pytest

from pyhdb.auth import AuthManager
from pyhdb.protocol.base import RequestSegment, Part
from pyhdb.protocol.constants import message_types, part_kinds
from pyhdb.protocol.parts import Authentication

@pytest.fixture
def auth_manager():
    manager = AuthManager(None, "TestUser", "secret")
    manager.client_key = b"\xed\xbd\x7c\xc8\xb2\xf2\x64\x89\xd6\x5a\x7c\xd5" \
                         b"\x1e\x27\xf2\xe7\x3f\xca\x22\x7d\x1a\xb6\xaa\xfc" \
                         b"\xac\x0f\x42\x8c\xa4\xd8\xe1\x0c\x19\xe3\xe3\x8f" \
                         b"\x3a\xac\x51\x07\x5e\x67\xbb\xe5\x2f\xdb\x61\x03" \
                         b"\xa7\xc3\x4c\x8a\x70\x90\x8e\xd5\xbe\x0b\x35\x42" \
                         b"\x70\x5f\x73\x8c"
    return manager

class TestSCRAMSHA256(object):

    # Test disabled:
    # Init request is not accessable anymore
    #
    # def test_init_request(self, auth_manager):
    #     request = auth_manager.get_initial_request()
    #     assert isinstance(request, RequestSegment)
    #     assert request.message_type == message_types.AUTHENTICATE
    #     assert len(request.parts) == 1
    #
    #     part = request.parts[0]
    #     assert isinstance(part, Part)
    #     assert part.kind == Authentication.kind
    #     assert part.user == "TestUser"
    #     assert part.methods == {
    #         b"SCRAMSHA256": auth_manager.client_key
    #     }

    def test_calculate_client_proof(self, auth_manager):
        salt = b"\x80\x96\x4f\xa8\x54\x28\xae\x3a\x81\xac" \
               b"\xd3\xe6\x86\xa2\x79\x33"
        server_key = b"\x41\x06\x51\x50\x11\x7e\x45\x5f\xec\x2f\x03\xf6" \
                     b"\xf4\x7c\x19\xd4\x05\xad\xe5\x0d\xd6\x57\x31\xdc" \
                     b"\x0f\xb3\xf7\x95\x4d\xb6\x2c\x8a\xa6\x7a\x7e\x82" \
                     b"\x5e\x13\x00\xbe\xe9\x75\xe7\x45\x18\x23\x8c\x9a"

        client_proof = auth_manager.calculate_client_proof(
            [salt], server_key
        )
        assert client_proof == \
            b"\x00\x01\x20\xe4\x7d\x8f\x24\x48\x55\xb9\x2d\xc9\x66\x39\x5d" \
            b"\x0d\x28\x25\x47\xb5\x4d\xfd\x09\x61\x4d\x44\x37\x4d\xf9\x4f" \
            b"\x29\x3c\x1a\x02\x0e"
