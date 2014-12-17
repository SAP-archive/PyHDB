import pytest
from pyhdb.protocol.base import RequestSegment
from pyhdb.exceptions import ProgrammingError, DatabaseError

@pytest.mark.hanatest
def test_invalid_request(connection):
    message = connection.Message(
        RequestSegment(2)
    )

    with pytest.raises(DatabaseError):
        message.send()

@pytest.mark.hanatest
def test_invalid_sql(connection):
    cursor = connection.cursor()

    with pytest.raises(DatabaseError):
        cursor.execute("SELECT FROM DUMMY")
