# Copyright 2014, 2015 SAP SE
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

DEFAULT_CONNECTION_OPTIONS = {
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

from pyhdb.protocol.constants.general import MAX_MESSAGE_SIZE, MAX_SEGMENT_SIZE
