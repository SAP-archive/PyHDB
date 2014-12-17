import pytest

import pyhdb

# Module globals defined by DBAPI 2.0
def test_apilevel():
    assert pyhdb.apilevel == "2.0"

def test_threadsafety():
    assert pyhdb.threadsafety == 2

def test_paramstyle():
    # TODO: Support also named format
    assert pyhdb.paramstyle == "format"

def test_exceptions():
    assert issubclass(pyhdb.Warning, Exception)
    assert issubclass(pyhdb.Error, Exception)
    assert issubclass(pyhdb.InterfaceError, pyhdb.Error)
    assert issubclass(pyhdb.DatabaseError, pyhdb.Error)
    assert issubclass(pyhdb.OperationalError, pyhdb.Error)
    assert issubclass(pyhdb.IntegrityError, pyhdb.Error)
    assert issubclass(pyhdb.InternalError, pyhdb.Error)
    assert issubclass(pyhdb.ProgrammingError, pyhdb.Error)
    assert issubclass(pyhdb.NotSupportedError, pyhdb.Error)

@pytest.mark.hanatest
def test_connect_constructors(hana_system):
    connection = pyhdb.connect(*hana_system)
    assert isinstance(connection, pyhdb.client.Connection)
    connection.close()
