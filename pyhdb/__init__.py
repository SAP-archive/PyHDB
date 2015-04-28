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

import os
import logging
import logging.config

this_dir = os.path.dirname(__file__)
logging.config.fileConfig(os.path.join(this_dir, 'logging.conf'))

from pyhdb.exceptions import *
from pyhdb.connection import Connection
from pyhdb.protocol.lobs import Blob, Clob, NClob

apilevel = "2.0"
threadsafety = 2
paramstyle = "numeric"
tracing = os.environ.get('HDB_TRACE', 'FALSE').upper() in ('TRUE', '1')


def connect(host, port, user, password, autocommit=False):
    conn = Connection(host, port, user, password, autocommit)
    conn.connect()
    return conn
