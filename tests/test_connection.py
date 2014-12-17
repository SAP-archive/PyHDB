# Copyright 2014 SAP SE.
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
