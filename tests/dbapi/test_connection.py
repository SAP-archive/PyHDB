import pytest
import pyhdb

# Test for DBAPI 2.0 compliance
@pytest.mark.hanatest
def test_fixture_connection(connection):
    # Smoke test of the connection fixture
    pass

@pytest.mark.hanatest
def test_commit(connection):
    connection.commit()

@pytest.mark.hanatest
def test_rollback(connection):
    connection.rollback()

@pytest.mark.hanatest
def test_cursor(connection):
    cursor = connection.cursor()
    assert isinstance(cursor, pyhdb.cursor.Cursor)

@pytest.mark.hanatest
@pytest.mark.parametrize("method", [
    'close',
    'commit',
    'rollback',
    'cursor',
])
def test_method_raises_error_after_close(hana_system, method):
    connection = pyhdb.connect(*hana_system)
    connection.close()

    with pytest.raises(pyhdb.Error):
        getattr(connection, method)()
