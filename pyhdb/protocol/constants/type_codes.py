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

# Type codes (obtained from HANA SQL command network protocol reference)

# Name and Value               # Description                              Support Level
NULL = 0                       # NULL value                               -
TINYINT = 1                    # TINYINT                                  1
SMALLINT = 2                   # SMALLINT                                 1
INT = 3                        # INTEGER                                  1
BIGINT = 4                     # BIGINT                                   1
DECIMAL = 5                    # DECIMAL, and DECIMAL(p,s)                1
REAL = 6                       # REAL                                     1
DOUBLE = 7                     # DOUBLE                                   1
CHAR = 8                       # CHAR                                     1
VARCHAR = 9                    # VARCHAR                                  1
NCHAR = 10                     # NCHAR (Unicode character type)           1
NVARCHAR = 11                  # NVARCHAR (Unicode character type)        1
BINARY = 12                    # BINARY                                   1
VARBINARY = 13                 # VARBINARY                                1
DATE = 14                      # DATE (deprecated type)                   1 (deprecated with 3)
TIME = 15                      # TIME (deprecated type)                   1 (deprecated with 3)
TIMESTAMP = 16                 # TIMESTAMP (millisecond precision)        1 (deprecated with 3)
TIME_TZ = 17                   # Reserved, do not use                     -
TIME_LTZ = 18                  # Reserved, do not use                     -
TIMESTAMP_TZ = 19              # Reserved, do not use                     -
TIMESTAMP_LTZ = 20             # Reserved, do not use                     -
INTERVAL_YM = 21               # Reserved, do not use                     -
INTERVAL_DS = 22               # Reserved, do not use                     -
ROWID = 23                     # Reserved, do not use                     -
UROWID = 24                    # Reserved, do not use                     -
CLOB = 25                      # Character Large Object                   1
NCLOB = 26                     # Unicode Character Large Object           1
BLOB = 27                      # Binary Large Object                      1
BOOLEAN = 28                   # Reserved, do not use                     -
STRING = 29                    # Character string                         1
NSTRING = 30                   # Unicode character string                 1
BLOCATOR = 31                  # Binary locator                           1
NLOCATOR = 32                  # Unicode character locator                1
BSTRING = 33                   # Binary string                            1
DECIMAL_DIGIT_ARRAY = 34       # Reserved, do not use                     -
VARCHAR2 = 35                  # VARCHAR                                  -
VARCHAR3 = 36                  # VARCHAR                                  -
NVARCHAR3 = 37                 # NVARCHAR                                 -
VARBINARY3 = 38                # VARBINARY                                -
VARGROUP = 39                  # Reserved, do not use                     -
TINYINT_NOTNULL = 40           # Reserved, do not use                     -
SMALLINT_NOTNULL = 41          # Reserved, do not use                     -
INT_NOTNULL = 42               # Reserved, do not use                     -
BIGINT_NOTNULL = 43            # Reserved, do not use                     -
ARGUMENT = 44                  # Reserved, do not use                     -
TABLE = 45                     # Reserved, do not use                     -
CURSOR = 46                    # Reserved, do not use                     -
SMALLDECIMAL = 47              # SMALLDECIMAL data type                   -
ABAPITAB = 48                  # ABAPSTREAM procedure parameter           1
ABAPSTRUCT = 49                # ABAP structure procedure parameter       1
ARRAY = 50                     # Reserved, do not use                     -
TEXT = 51                      # TEXT data type                           3
SHORTTEXT = 52                 # SHORTTEXT data type                      3
FIXEDSTRING = 53               # Reserved, do not use                     -
FIXEDPOINTDECIMAL = 54         # Reserved, do not use                     -
ALPHANUM = 55                  # ALPHANUM data type                       3
TLOCATOR = 56                  # Reserved, do not use                     -
LONGDATE = 61                  # TIMESTAMP data type                      3
SECONDDATE = 62                # TIMESTAMP type with second precision     3
DAYDATE = 63                   # DATE data type                           3
SECONDTIME = 64                # TIME data type                           3
CSDATE = 65                    # Reserved, do not use                     -
CSTIME = 66                    # Reserved, do not use                     -
BLOB_DISK = 71                 # Reserved, do not use                     -
CLOB_DISK = 72                 # Reserved, do not use                     -
NCLOB_DISK = 73                # Reserved, do not use                     -
GEOMETRY = 74                  # Reserved, do not use                     -
POINT = 75                     # Reserved, do not use                     -
FIXED16 = 76                   # Reserved, do not use                     -
BLOB_HYBRID = 77               # Reserved, do not use                     -
CLOB_HYBRID = 78               # Reserved, do not use                     -
NCLOB_HYBRID = 79              # Reserved, do not use                     -
POINTZ = 80                    # Reserved, do not use                     -

