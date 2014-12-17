from io import BytesIO
from pyhdb.protocol.parts import RowsAffected

def test_unpack_one_value():
    values = RowsAffected.unpack_data(
        1,
        BytesIO(b"\x01\x00\x00\x00")
    )
    assert values == ((1,),)

def test_unpack_multiple_values():
    values = RowsAffected.unpack_data(
        3,
        BytesIO(b"\x01\x00\x00\x00\x02\x00\x00\x00\x03\x00\x00\x00")
    )
    assert values == ((1, 2, 3),)
