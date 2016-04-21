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
from pyhdb.protocol.message import RequestMessage
from pyhdb.protocol.segments import RequestSegment
from pyhdb.exceptions import DatabaseError, Warning

import tests.helper
from tests.helper import procedure_with_execution_warning
import warnings

@pytest.mark.hanatest
def test_invalid_request(connection):
    request = RequestMessage.new(
        connection,
        RequestSegment(2)
    )

    with pytest.raises(DatabaseError):
        connection.send_request(request)


@pytest.mark.hanatest
def test_invalid_sql(connection):
    cursor = connection.cursor()

    with pytest.raises(DatabaseError):
        cursor.execute("SELECT FROM DUMMY")

@pytest.mark.hanatest
def test_PROCEDURE_WITH_EXECUTION_WARNING(connection, procedure_with_execution_warning):
    cursor = connection.cursor()

    sql_to_prepare = 'call PROCEDURE_WITH_EXECUTION_WARNING ()'
    params = []
    psid = cursor.prepare(sql_to_prepare)
    ps = cursor.get_prepared_statement(psid)
    
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        cursor.execute_prepared(ps, [params])
        assert issubclass(w[-1].category, Warning)
        assert "Not recommended feature: DDL statement" in str(w[-1].message)
