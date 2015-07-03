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

import pytest

import pyhdb
import tests.helper

TABLE = 'PYHDB_TEST_1'
TABLE_FIELDS = 'TEST VARCHAR(255)'


@pytest.fixture
def test_table(request, connection):
    """Fixture to create table for testing, and dropping it after test run"""
    tests.helper.create_table_fixture(request, connection, TABLE, TABLE_FIELDS)


@pytest.mark.hanatest
class TestIsolationBetweenConnections(object):

    def test_commit(self, request, hana_system, test_table):
        connection_1 = pyhdb.connect(*hana_system)
        connection_2 = pyhdb.connect(*hana_system)

        def _close():
            connection_1.close()
            connection_2.close()
        request.addfinalizer(_close)

        cursor1 = connection_1.cursor()
        cursor1.execute(
            'INSERT INTO PYHDB_TEST_1 VALUES(%s)', ('connection_1',)
        )
        cursor1.execute("SELECT * FROM PYHDB_TEST_1")
        assert cursor1.fetchall() == [('connection_1',)]

        cursor2 = connection_2.cursor()
        cursor2.execute("SELECT * FROM PYHDB_TEST_1")
        assert cursor2.fetchall() == []

        connection_1.commit()

        cursor2.execute("SELECT * FROM PYHDB_TEST_1")
        assert cursor2.fetchall() == [('connection_1',)]

    def test_rollback(self, request, hana_system, test_table):
        connection_1 = pyhdb.connect(*hana_system)
        connection_2 = pyhdb.connect(*hana_system)

        def _close():
            connection_1.close()
            connection_2.close()
        request.addfinalizer(_close)

        cursor1 = connection_1.cursor()
        cursor1.execute(
            'INSERT INTO PYHDB_TEST_1 VALUES(%s)', ('connection_1',)
        )
        cursor1.execute("SELECT * FROM PYHDB_TEST_1")
        assert cursor1.fetchall() == [('connection_1',)]

        cursor2 = connection_2.cursor()
        cursor2.execute("SELECT * FROM PYHDB_TEST_1")
        assert cursor2.fetchall() == []

        connection_1.rollback()

        cursor1.execute("SELECT * FROM PYHDB_TEST_1")
        assert cursor1.fetchall() == []

    def test_auto_commit(self, request, hana_system, test_table):
        connection_1 = pyhdb.connect(*hana_system, autocommit=True)
        connection_2 = pyhdb.connect(*hana_system, autocommit=True)

        def _close():
            connection_1.close()
            connection_2.close()
        request.addfinalizer(_close)

        cursor1 = connection_1.cursor()
        cursor1.execute(
            'INSERT INTO PYHDB_TEST_1 VALUES(%s)', ('connection_1',)
        )
        cursor1.execute("SELECT * FROM PYHDB_TEST_1")
        assert cursor1.fetchall() == [('connection_1',)]

        cursor2 = connection_2.cursor()
        cursor2.execute("SELECT * FROM PYHDB_TEST_1")
        assert cursor2.fetchall() == [('connection_1',)]

    def test_select_for_update(self, connection, test_table):
        cursor = connection.cursor()
        cursor.execute("INSERT INTO PYHDB_TEST_1 VALUES(%s)", ('test',))
        connection.commit()

        cursor.execute("SELECT * FROM PYHDB_TEST_1 FOR UPDATE")
        assert cursor.fetchall() == [('test',)]

