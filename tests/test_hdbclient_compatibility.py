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
from pyhdb.connection import Connection


def test_getautocommit():
    connection = Connection("localhost", 30015, "Fuu", "Bar")
    assert not connection.getautocommit()

    connection.autocommit = True
    assert connection.getautocommit()


def test_setautocommit():
    connection = Connection("localhost", 30015, "Fuu", "Bar")

    connection.setautocommit(False)
    assert not connection.autocommit

    connection.setautocommit(True)
    assert connection.autocommit


@pytest.mark.hanatest
def test_isconnected(hana_system):
    connection = Connection(*hana_system)
    assert not connection.isconnected()

    connection.connect()
    assert connection.isconnected()

    connection.close()
    assert not connection.isconnected()
