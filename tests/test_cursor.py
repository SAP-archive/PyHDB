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

from pyhdb.cursor import format_operation
from pyhdb.exceptions import ProgrammingError
import tests.helper

TABLE = 'PYHDB_TEST_1'
TABLE_FIELDS = 'TEST VARCHAR(255)'


@pytest.fixture
def test_table_1(request, connection):
    """Fixture to create table for testing, and dropping it after test run"""
    tests.helper.create_table_fixture(request, connection, TABLE, TABLE_FIELDS)


@pytest.fixture
def content_table_1(request, connection):
    """Additional fixture to test_table_1, inserts some rows for testing"""
    cursor = connection.cursor()
    cursor.execute("insert into PYHDB_TEST_1 values('row1')")
    cursor.execute("insert into PYHDB_TEST_1 values('row2')")
    cursor.execute("insert into PYHDB_TEST_1 values('row3')")


@pytest.mark.parametrize("parameters", [
    None,
    (),
    []
])
def test_format_operation_without_parameters(parameters):
    """Test that providing no parameter produces correct result."""
    operation = "SELECT * FROM TEST WHERE fuu = 'bar'"
    assert format_operation(operation, parameters) == operation


def test_format_operation_with_positional_parameters():
    """Test that correct number of parameters produces correct result."""
    assert format_operation(
        "INSERT INTO TEST VALUES(%s, %s)", ('Hello World', 2)
    ) == "INSERT INTO TEST VALUES('Hello World', 2)"


def test_format_operation_with_too_few_positional_parameters_raises():
    """Test that providing too few parameters raises exception"""
    with pytest.raises(ProgrammingError):
        format_operation("INSERT INTO TEST VALUES(%s, %s)", ('Hello World',))


def test_format_operation_with_too_many_positional_parameters_raises():
    """Test that providing too many parameters raises exception"""
    with pytest.raises(ProgrammingError):
        format_operation("INSERT INTO TEST VALUES(%s)", ('Hello World', 2))


def test_format_operation_with_named_parameters():
    """format_operation() is used for Python style parameter expansion"""
    assert format_operation(
        "INSERT INTO TEST VALUES(%(name)s, %(val)s)",
        {'name': 'Hello World', 'val': 2}
    ) == "INSERT INTO TEST VALUES('Hello World', 2)"


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


# Test cases for different parameter style expansion
#
# paramstyle 	Meaning
# ---------------------------------------------------------
# 1) qmark       Question mark style, e.g. ...WHERE name=?
# 2) numeric     Numeric, positional style, e.g. ...WHERE name=:1
# 3) named       Named style, e.g. ...WHERE name=:name  -> NOT IMPLEMENTED !!
# 4) format 	   ANSI C printf format codes, e.g. ...WHERE name=%s
# 5) pyformat    Python extended format codes, e.g. ...WHERE name=%(name)s

@pytest.mark.hanatest
def test_cursor_execute_with_params1(connection, test_table_1, content_table_1):
    """Test qmark parameter expansion style - uses cursor.prepare*() methods"""
    # Note: use fetchall() to check that only one row gets returned
    cursor = connection.cursor()

    sql = 'select test from PYHDB_TEST_1 where test=?'
    # correct way:
    assert cursor.execute(sql, ['row2']).fetchall() == [('row2',)]
    # invalid - extra unexpected parameter
    with pytest.raises(ProgrammingError):
        cursor.execute(sql, ['row2', 'extra']).fetchall()


@pytest.mark.hanatest
def test_cursor_execute_with_params2(connection, test_table_1, content_table_1):
    """Test numeric parameter expansion style - uses cursor.prepare() methods"""
    # Note: use fetchall() to check that only one row gets returned
    cursor = connection.cursor()

    sql = 'select test from PYHDB_TEST_1 where test=?'
    # correct way:
    assert cursor.execute(sql, ['row2']).fetchall() == [('row2',)]
    # invalid - extra unexpected parameter
    with pytest.raises(ProgrammingError):
        cursor.execute(sql, ['row2', 'extra']).fetchall()


@pytest.mark.hanatest
def test_cursor_execute_with_params4(connection, test_table_1, content_table_1):
    """Test format (positional) parameter expansion style"""
    # Uses prepare_operation method
    cursor = connection.cursor()

    sql = 'select test from PYHDB_TEST_1 where test=%s'
    # correct way:
    assert cursor.execute(sql, ['row2']).fetchall() == [('row2',)]
    # invalid - extra unexpected parameter
    with pytest.raises(ProgrammingError):
        cursor.execute(sql, ['row2', 'extra']).fetchall()


@pytest.mark.hanatest
def test_cursor_execute_with_params5(connection, test_table_1, content_table_1):
    """Test pyformat (named) parameter expansion style"""
    # Note: use fetchall() to check that only one row gets returned
    cursor = connection.cursor()

    sql = 'select test from {} where test=%(test)s'.format(TABLE)
    # correct way:
    assert cursor.execute(sql, {'test': 'row2'}).fetchall() == [('row2',)]
    # also correct way, additional dict value should just be ignored
    assert cursor.execute(sql, {'test': 'row2', 'd': 2}).fetchall() == \
        [('row2',)]


@pytest.mark.hanatest
def test_cursor_insert_commit(connection, test_table_1):
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM %s" % TABLE)
    assert cursor.fetchone() == (0,)

    cursor.execute("INSERT INTO %s VALUES('Hello World')" % TABLE)
    assert cursor.rowcount == 1

    cursor.execute("SELECT COUNT(*) FROM %s" % TABLE)
    assert cursor.fetchone() == (1,)
    connection.commit()


@pytest.mark.hanatest
def test_cursor_create_and_drop_table(connection):
    cursor = connection.cursor()

    if tests.helper.exists_table(connection, TABLE):
        cursor.execute('DROP TABLE "%s"' % TABLE)

    assert not tests.helper.exists_table(connection, TABLE)
    cursor.execute('CREATE TABLE "%s" ("TEST" VARCHAR(255))' % TABLE)
    assert tests.helper.exists_table(connection, TABLE)

    cursor.execute('DROP TABLE "%s"' % TABLE)


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
    assert cursor.description is None

    cursor.execute("SELECT 'Hello World' AS TEST FROM DUMMY")
    assert cursor.description == ((u'TEST', 9, None, 11, 0, None, 0),)


@pytest.mark.hanatest
def test_cursor_executemany_python_expansion(connection, test_table_1):
    cursor = connection.cursor()

    cursor.executemany(
        "INSERT INTO {} VALUES(%s)".format(TABLE),
        (
            ("Statement 1",),
            ("Statement 2",)
        )
    )

    cursor.execute("SELECT * FROM %s" % TABLE)
    result = cursor.fetchall()
    assert result == [('Statement 1',), ('Statement 2',)]


@pytest.mark.hanatest
def test_cursor_executemany_hana_expansion(connection, test_table_1):
    cursor = connection.cursor()

    cursor.executemany(
        "INSERT INTO %s VALUES(:1)" % TABLE,
        (
            ("Statement 1",),
            ("Statement 2",)
        )
    )

    cursor.execute("SELECT * FROM %s" % TABLE)
    result = cursor.fetchall()
    assert result == [('Statement 1',), ('Statement 2',)]
