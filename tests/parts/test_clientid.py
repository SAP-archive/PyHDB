from io import BytesIO
from pyhdb.protocol.parts import ClientId

def test_pack_data():
    part = ClientId("bla@example.com")
    arguments, payload = part.pack_data()
    assert arguments == 1
    assert payload == "bla@example.com".encode('cesu-8')

def test_unpack_data():
    client_id = ClientId.unpack_data(1, BytesIO(b"bla@example.com"))
    assert client_id == "bla@example.com"
