# Copyright 2014 SAP SE
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

from pyhdb.client import Connection
from pyhdb.exceptions import *

apilevel = "2.0"
threadsafety = 2
paramstyle = "qmark"


def connect(host, port, user, password, autocommit=False):
    connection = Connection(host, port, user, password, autocommit)
    connection.connect()
    return connection
