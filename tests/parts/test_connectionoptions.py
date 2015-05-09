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
from pyhdb.protocol.parts import ConnectOptions
from pyhdb.protocol.segments import MAX_SEGMENT_SIZE


def test_pack_default_connection_options():
    options = {
        "connection_id": None,
        "complete_array_execution": True,
        "client_locale": "en_US",
        "supports_large_bulk_operations": None,
        "large_number_of_parameters_support": None,
        "system_id": None,
        "select_for_update_supported": False,
        "client_distribution_mode": 0,
        "engine_data_format_version": None,
        "distribution_protocol_version": 0,
        "split_batch_commands": True,
        "use_transaction_flags_only": None,
        "row_and_column_optimized_format": None,
        "ignore_unknown_parts": None,
        "data_format_version": 1,
        "data_format_version2": 1
    }

    arguments, payload = ConnectOptions(options).pack_data(MAX_SEGMENT_SIZE)
    assert arguments == 8
    # Test note: We can test again the cncatenated hex string
    # because sometimes the order of the dict elements is different

    # Contains complete_array_execution
    assert b"\x02\x1C\x01" in payload

    # Contains client_locale
    assert b"\x03\x1D\x05\x00\x65\x6E\x5F\x55\x53" in payload

    # Contains select_for_update_supported
    assert b"\x0E\x1C\x00" in payload

    # Contains client_distribution_mode
    assert b"\x0F\x03\x00\x00\x00\x00" in payload

    # Contains distribution_protocol_version
    assert b"\x11\x03\x00\x00\x00\x00" in payload

    # Contains split_batch_commands
    assert b"\x12\x1C\x01" in payload

    # Contains data_format_version
    assert b"\x0C\x03\x01\x00\x00\x00" in payload

    # Contains data_format_version2
    assert b"\x17\x03\x01\x00\x00\x00" in payload

    # There is nothing more
    assert len(payload) == 42


def test_unpack_default_connection_options():
    packed = BytesIO(
        b"\x03\x1d\x05\x00\x65\x6e\x5f\x55\x53\x0f\x03\x00\x00\x00\x00\x17"
        b"\x03\x01\x00\x00\x00\x0c\x03\x01\x00\x00\x00\x02\x1c\x01\x11\x03"
        b"\x00\x00\x00\x00\x0e\x1c\x00\x12\x1c\x01"
    )

    options, = ConnectOptions.unpack_data(8, packed)
    assert options == {
        "complete_array_execution": True,
        "client_locale": "en_US",
        "select_for_update_supported": False,
        "client_distribution_mode": 0,
        "distribution_protocol_version": 0,
        "split_batch_commands": True,
        "data_format_version": 1,
        "data_format_version2": 1
    }
