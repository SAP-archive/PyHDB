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

from io import BytesIO
from pyhdb.protocol.parts import RowsAffected


def test_unpack_one_value():
    values = RowsAffected.unpack_data(
        1,
        BytesIO(b"\x01\x00\x00\x00")
    )
    assert values == ((1,),)


def test_unpack_multiple_values():
    values = RowsAffected.unpack_data(
        3,
        BytesIO(b"\x01\x00\x00\x00\x02\x00\x00\x00\x03\x00\x00\x00")
    )
    assert values == ((1, 2, 3),)
