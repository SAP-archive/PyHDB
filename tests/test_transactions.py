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


def exists_table(connection, name):
    cursor = connection.cursor()
    cursor.execute(
        'SELECT 1 FROM "SYS"."TABLES" WHERE "TABLE_NAME" = %s',
        (name,)
    )
    return cursor.fetchone() is not None


@pytest.fixture
def test_table_1(request, connection):
    cursor = connection.cursor()
    if exists_table(connection, "PYHDB_TEST_1"):
        cursor.execute('DROP TABLE "PYHDB_TEST_1"')

    assert not exists_table(connection, "PYHDB_TEST_1")
    cursor.execute('CREATE TABLE "PYHDB_TEST_1" ("TEST" VARCHAR(255))')
    if not exists_table(connection, "PYHDB_TEST_1"):
        pytest.skip("Couldn't create table PYHDB_TEST_1")
        return

    def _close():
        cursor.execute('DROP TABLE "PYHDB_TEST_1"')
    request.addfinalizer(_close)


@pytest.mark.hanatest
class TestIsolationBetweenConnections():

    def test_commit(self, request, hana_system, test_table_1):
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
        assert cursor1.fetchall() == [('connection_1',),]

        cursor2 = connection_2.cursor()
        cursor2.execute("SELECT * FROM PYHDB_TEST_1")
        assert cursor2.fetchall() == []

        connection_1.commit()

        cursor2.execute("SELECT * FROM PYHDB_TEST_1")
        assert cursor2.fetchall() == [('connection_1',),]

    def test_rollback(self, request, hana_system, test_table_1):
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
        assert cursor1.fetchall() == [('connection_1',),]

        cursor2 = connection_2.cursor()
        cursor2.execute("SELECT * FROM PYHDB_TEST_1")
        assert cursor2.fetchall() == []

        connection_1.rollback()

        cursor1.execute("SELECT * FROM PYHDB_TEST_1")
        assert cursor1.fetchall() == []

    def test_auto_commit(self, request, hana_system, test_table_1):
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
        assert cursor1.fetchall() == [('connection_1',),]

        cursor2 = connection_2.cursor()
        cursor2.execute("SELECT * FROM PYHDB_TEST_1")
        assert cursor2.fetchall() == [('connection_1',),]
