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

from pyhdb.cursor import format_operation
from pyhdb.exceptions import ProgrammingError

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

@pytest.mark.parametrize("parameters", [
    None,
    (),
    []
])
def test_format_operation_without_parameters(parameters):
    operation = "SELECT * FROM TEST WHERE fuu = 'bar'"
    assert format_operation(operation, parameters) == operation

def test_format_operation_with_parameters():
    assert format_operation(
        "INSERT INTO TEST VALUES(%s)", ("'Hello World'",)
    ) == "INSERT INTO TEST VALUES('''Hello World''')"

@pytest.mark.hanatest
def test_cursor_fetch_without_execution(connection):
    cursor = connection.cursor()
    with pytest.raises(ProgrammingError):
        cursor.fetchone()

@pytest.mark.hanatest
def test_cursor_fetchall_single_row(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT 1 FROM DUMMY")

    result = cursor.fetchall()
    assert result == [(1,)]

@pytest.mark.hanatest
def test_cursor_fetchall_multiple_rows(connection):
    cursor = connection.cursor()
    cursor.execute('SELECT "VIEW_NAME" FROM "PUBLIC"."VIEWS" LIMIT 10')

    result = cursor.fetchall()
    assert len(result) == 10

@pytest.mark.hanatest
def test_cursor_insert_commit(connection, test_table_1):
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM PYHDB_TEST_1")
    assert cursor.fetchone() == (0,)

    cursor.execute("INSERT INTO PYHDB_TEST_1 VALUES('Hello World')")
    assert cursor.rowcount == 1

    cursor.execute("SELECT COUNT(*) FROM PYHDB_TEST_1")
    assert cursor.fetchone() == (1,)
    connection.commit()

def exists_table(connection, name):
    cursor = connection.cursor()
    cursor.execute(
        'SELECT 1 FROM "SYS"."TABLES" WHERE "TABLE_NAME" = %s',
        (name,)
    )
    return cursor.fetchone() is not None

@pytest.mark.hanatest
def test_cursor_create_and_drop_table(connection):
    cursor = connection.cursor()

    if exists_table(connection, "PYHDB_TEST_1"):
        cursor.execute('DROP TABLE "PYHDB_TEST_1"')

    assert not exists_table(connection, "PYHDB_TEST_1")
    cursor.execute('CREATE TABLE "PYHDB_TEST_1" ("TEST" VARCHAR(255))')
    assert exists_table(connection, "PYHDB_TEST_1")

    cursor.execute('DROP TABLE "PYHDB_TEST_1"')

@pytest.mark.hanatest
def test_received_last_resultset_part_resets_after_execute(connection):
    # The private attribute was not reseted to False after
    # executing another statement
    cursor = connection.cursor()

    cursor.execute("SELECT 1 FROM DUMMY")
    # Result is very small we got everything direct into buffer
    assert cursor._received_last_resultset_part

    cursor.execute("SELECT VIEW_NAME FROM PUBLIC.VIEWS")
    # Result is not small enouth for single resultset part
    assert not cursor._received_last_resultset_part

@pytest.mark.hanatest
def test_execute_tidies_buffer(connection):
    cursor = connection.cursor()

    # We execute something but don't use fetchone
    cursor.execute("SELECT 123 FROM DUMMY")
    assert len(cursor._buffer) == 1
    assert cursor._buffer[0] == (123,)

    # We correct cleanup the buffer will keep the wrong row
    cursor.execute("SELECT 456 FROM DUMMY")
    assert len(cursor._buffer) == 1
    assert cursor._buffer[0] == (456,)

@pytest.mark.hanatest
@pytest.mark.parametrize("method", [
    'fetchone',
    'fetchall',
    'fetchmany',
])
def test_fetch_raises_error_after_close(connection, method):
    cursor = connection.cursor()
    cursor.close()

    with pytest.raises(ProgrammingError):
        getattr(cursor, method)()

@pytest.mark.hanatest
def test_execute_raises_error_after_close(connection):
    cursor = connection.cursor()
    cursor.close()

    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT TEST FROM DUMMY")

@pytest.mark.hanatest
def test_cursor_description_after_execution(connection):
    cursor = connection.cursor()
    assert cursor.description == None

    cursor.execute("SELECT 'Hello World' AS TEST FROM DUMMY")
    assert cursor.description == ((u'TEST', 9, None, 11, 0, None, 0),)

@pytest.mark.hanatest
def test_cursor_executemany(connection, test_table_1):
    cursor = connection.cursor()

    cursor.executemany(
        "INSERT INTO PYHDB_TEST_1 VALUES(%s)",
        (
            ("Statement 1",),
            ("Statement 2",)
        )
    )

    cursor.execute("SELECT * FROM PYHDB_TEST_1")
    result = cursor.fetchall()
    assert result == [('Statement 1',), ('Statement 2',)]
