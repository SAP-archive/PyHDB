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

# Part kinds

COMMAND = 3               # SQL Command Data
RESULTSET = 5             # Tabular result set data
ERROR = 6                 # Error information
STATEMENTID = 10          # Prepared statement identifier
ROWSAFFECTED = 12         # Number of affected rows
RESULTSETID = 13          # Result set identifier
TOPOLOGYINFORMATION = 15  # Topoloygy information
READLOBREQUEST = 17       # Request for reading (part of) a lob
READLOBREPLY = 18         # Reply of request for reading (part of) a lob
WRITELOBREQUEST = 28      # Request of data of WRITELOB message
WRITELOBREPLY = 30        # Reply data of WRITELOB message
PARAMETERS = 32           # Parameter data
AUTHENTICATION = 33       # Authentication data
CLIENTID = 35             # (undocumented) client id
STATEMENTCONTEXT = 39     # Statement visibility context
OUTPUTPARAMETERS = 41     # Output parameter data
CONNECTOPTIONS = 42       # Connect options
FETCHSIZE = 45            # Numbers of rows to fetch
PARAMETERMETADATA = 47    # Parameter metadata (type and length information)
RESULTSETMETADATA = 48    # Result set metadata (type, length, and name information)
TRANSACTIONFLAGS = 64     # Transaction handling flags
