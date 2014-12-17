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
