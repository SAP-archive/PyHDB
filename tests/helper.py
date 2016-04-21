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


def exists_table(connection, table):
    """Check whether table exists
    :param table: name of table
    :returns: bool
    """
    cursor = connection.cursor()
    cursor.execute('SELECT 1 FROM "SYS"."TABLES" WHERE "TABLE_NAME" = %s', (table,))
    return cursor.fetchone() is not None


def create_table_fixture(request, connection, table, table_fields, column_table=False):
    """
    Create table fixture for unittests
    :param request: pytest request object
    :param connection: connection object
    :param table: name of table
    :param table_fields: string with comma separated field definitions, e.g. "name VARCHAR(5), fblob blob"
    """
    cursor = connection.cursor()
    if exists_table(connection, table):
        cursor.execute('DROP table "%s"' % table)

    assert not exists_table(connection, table)
    table_type = "COLUMN" if column_table else ""
    cursor.execute('CREATE %s table "%s" (%s)' % (table_type, table, table_fields))
    if not exists_table(connection, table):
        pytest.skip("Couldn't create table %s" % table)
        return

    def _close():
        cursor.execute('DROP table "%s"' % table)
    request.addfinalizer(_close)

@pytest.fixture
def procedure_add2_fixture(request, connection):
    cursor = connection.cursor()
    # create temporary procedure
    try:
        cursor.execute("""create procedure PYHDB_PROC_ADD2 (in a int, in b int, out c int, out d char)
        language sqlscript
        reads sql data as
        begin
            c := :a + :b;
            d := 'A';
        end""")
    except:
        # procedure probably already existed
        pass

    def _close():
        try:
            cursor.execute("""DROP PROCEDURE PYHDB_PROC_ADD2""")
        except:
            # procedure didnt exist
            pass

    request.addfinalizer(_close)

@pytest.fixture
def procedure_with_result_fixture(request, connection):
    cursor = connection.cursor()
    # create temporary procedure
    try:
        cursor.execute("""CREATE PROCEDURE PYHDB_PROC_WITH_RESULT (OUT OUTVAR INTEGER)
            AS
            BEGIN
              SELECT 2015 INTO OUTVAR FROM DUMMY;
            END""")
    except:
        # procedure probably already existed
        pass

    def _close():
        try:
            cursor.execute("""DROP PROCEDURE PYHDB_PROC_WITH_RESULT""")
        except:
            # procedure didnt exist
            pass

    request.addfinalizer(_close)

@pytest.fixture
def procedure_with_execution_warning(request, connection):
    cursor = connection.cursor()
    # create temporary procedure
    try:
        cursor.execute("""CREATE PROCEDURE PROCEDURE_WITH_EXECUTION_WARNING(
                        )
                        LANGUAGE SQLSCRIPT
                        SQL SECURITY INVOKER
                        AS
                        BEGIN
                            exec 'create table PROCEDURE_WITH_EXECUTION_WARNING_TABLE(id int)';
        END""")
    except:
        # procedure probably already existed
        pass

    def _close():
        try:
            cursor.execute("""DROP PROCEDURE PROCEDURE_WITH_EXECUTION_WARNING""")
            cursor.execute("""DROP TABLE PROCEDURE_WITH_EXECUTION_WARNING_TABLE""")
        except:
            # procedure didnt exist
            pass

    request.addfinalizer(_close)
