import pytest
from pyhdb.client import Connection

def test_getautocommit():
    connection = Connection("localhost", 30015, "Fuu", "Bar")
    assert not connection.getautocommit()

    connection.autocommit = True
    assert connection.getautocommit()

def test_setautocommit():
    connection = Connection("localhost", 30015, "Fuu", "Bar")

    connection.setautocommit(False)
    assert not connection.autocommit

    connection.setautocommit(True)
    assert connection.autocommit

@pytest.mark.hanatest
def test_isconnected(hana_system):
    connection = Connection(*hana_system)
    assert not connection.isconnected()

    connection.connect()
    assert connection.isconnected()

    connection.close()
    assert not connection.isconnected()
