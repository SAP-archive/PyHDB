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

from io import BytesIO
from pyhdb.protocol.parts import ClientId
from pyhdb.protocol.segments import MAX_SEGMENT_SIZE


def test_pack_data():
    part = ClientId("bla@example.com")
    arguments, payload = part.pack_data(MAX_SEGMENT_SIZE)
    assert arguments == 1
    assert payload == "bla@example.com".encode('cesu-8')


def test_unpack_data():
    client_id = ClientId.unpack_data(1, BytesIO(b"bla@example.com"))
    assert client_id == "bla@example.com"
