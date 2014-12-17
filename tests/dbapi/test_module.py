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
