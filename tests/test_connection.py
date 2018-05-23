# Copyright 2014, 2015 SAP SE.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http: //www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

# Test additional features of pyhdb.Connection

import os
import pytest
import mock

import tests.helper
from tests.helper import connection_mock_timeout

from pyhdb.connection import Connection
from pyhdb.exceptions import ConnectionTimedOutError
import pyhdb


@pytest.mark.hanatest
def test_reconnect_cursor(hana_system):
    connection = Connection(*hana_system, reconnect=True)
    connection.connect()

    # break connection
    connection_mock_timeout(connection)

    # try dummy query
    cursor = connection.cursor()
    cursor.execute('''SELECT 1 FROM DUMMY''')

    assert connection.isconnected()


@pytest.mark.hanatest
def test_reconnect_execute(hana_system):
    connection = Connection(*hana_system, reconnect=True)
    connection.connect()
    cursor = connection.cursor()

    # break connection
    connection_mock_timeout(connection)

    # try dummy query
    cursor.execute('''SELECT 1 FROM DUMMY''')

    assert connection.isconnected()


@pytest.mark.hanatest
def test_initial_timeout(connection):
    assert connection.timeout is None
    assert connection._timeout is None
    assert connection._socket.gettimeout() is None


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
    assert connection.timeout is None

    connection.timeout = 10
    assert connection.timeout == 10
    assert connection._socket.gettimeout() == 10


def test_set_timeout_without_socket():
    connection = Connection("localhost", 30015, "Fuu", "Bar")
    connection.timeout = 10
    assert connection.timeout == 10


def test_make_connection_from_pytest_ini():
    if not os.path.isfile('pytest.ini'):
        pytest.skip("Requires pytest.ini file")
    connection = pyhdb.connect.from_ini('pytest.ini')
