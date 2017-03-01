# -*- coding: utf-8 -*-

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

import tests.helper

from tests.helper import procedure_add2_fixture, procedure_with_result_fixture
from pyhdb.resultrow import ResultRow
# #############################################################################################################
#                         Basic Stored Procedure test
# #############################################################################################################

# ### One or more scalar parameters, OUT or INOUT
@pytest.mark.hanatest
def test_PROC_ADD2(connection, procedure_add2_fixture):
    cursor = connection.cursor()

    sql_to_prepare = 'call PYHDB_PROC_ADD2 (?, ?, ?, ?)'
    params = [1, 2, None, None]
    params = {'A':2, 'B':5, 'C':None, 'D': None}
    psid = cursor.prepare(sql_to_prepare)
    ps = cursor.get_prepared_statement(psid)
    cursor.execute_prepared(ps, [params])
    result = cursor.fetchall()
    assert result == [ResultRow((), (7, 'A'))]

@pytest.mark.hanatest
def test_proc_with_results(connection, procedure_with_result_fixture):
    cursor = connection.cursor()

    # prepare call
    psid = cursor.prepare("CALL PYHDB_PROC_WITH_RESULT(?)")
    ps = cursor.get_prepared_statement(psid)

    # execute prepared statement
    cursor.execute_prepared(ps, [{'OUTVAR': 0}])
    result = cursor.fetchall()

    assert result == [ResultRow((), (2015,))]

    cursor.close()
