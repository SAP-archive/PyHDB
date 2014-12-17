# Test additional features of pyhdb.Connection

import pytest

from pyhdb.client import Connection

@pytest.mark.hanatest
def test_initial_timeout(connection):
    assert connection.timeout == None
    assert connection._timeout == None
    assert connection._socket.gettimeout() == None

@pytest.mark.hanatest
def test_set_timeout_in_init(hana_system):
    connection = Connection(*hana_system, timeout=10)
    assert connection.timeout == 10
    assert connection._timeout == 10

@pytest.mark.hanatest
def test_socket_use_init_timeout(hana_system):
    connection = Connection(*hana_system, timeout=10)
    connection.connect()

    assert connection._socket.gettimeout() == 10
    connection.close()

@pytest.mark.hanatest
def test_set_timeout_update_socket_setting(connection):
    assert connection.timeout == None

    connection.timeout = 10
    assert connection.timeout == 10
    assert connection._socket.gettimeout() == 10

def test_set_timeout_without_socket():
    connection = Connection("localhost", 30015, "Fuu", "Bar")
    connection.timeout = 10
    assert connection.timeout == 10
