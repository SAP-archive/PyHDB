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

from pyhdb.exceptions import *
from pyhdb.connection import Connection
from pyhdb.protocol.lobs import Blob, Clob, NClob
from pyhdb.compat import configparser

apilevel = "2.0"
threadsafety = 2
paramstyle = "numeric"
tracing = os.environ.get('HDB_TRACE', 'FALSE').upper() in ('TRUE', '1')


def connect(host, port, user, password, autocommit=False):
    conn = Connection(host, port, user, password, autocommit)
    conn.connect()
    return conn


def from_ini(ini_file, section=None):
    """
    Make connection to database by reading connection parameters from an ini file.
    :param ini_file: Name of ini file, e.g. 'pytest.ini'
    :param section: specify alternative section in ini file. Section 'hana' and 'pytest' will be searched by default
    :return: connection object

    Example:
        [pytest]
        hana_host = 10.97.76.24
        hana_hostname = mo-2384d0f48.mo.sap.corp
        hana_port = 30015
        hana_user = D037732
        hana_password = Abcd1234

    For historical reasons a 'hana_' prefix is allowed, but will be removed automatically.
    """
    if not os.path.exists(ini_file):
        raise RuntimeError('Could not find ini file %s' % ini_file)
    cp = configparser.ConfigParser()
    cp.read(ini_file)
    if not cp.sections():
        raise RuntimeError('Could not find any section in ini file %s' % ini_file)
    if section:
        sec_list = [section]
    elif len(cp.sections()) == 1:
        # no section specified - check if there is a single/unique section in the ini file:
        sec_list = cp.sections()
    else:
        # ini_file has more than one section, so try some default names:
        sec_list = ['hana', 'pytest']

    for sec in sec_list:
        try:
            param_values = cp.items(sec)
        except configparser.NoSectionError:
            continue
        params = dict(param_values)
        break
    else:
        raise RuntimeError('Could not guess which section to use for hana credentials from %s' % ini_file)

    # Parameters can be named like 'hana_user' (e.g. pytest.ini) or just 'user' (other ini's).
    # Remove the 'hana_' prefix so that parameter names match the arguments of the pyhdb.connect() function.
    # Also remove invalid keys from clean_params (like 'hostname' etc).

    def rm_prefix(param):
        return param[5:] if param.startswith('hana_') else param

    valid_keys = ('host', 'port', 'user', 'password')
    clean_params = {'%s' % rm_prefix(key): val for key, val in params.items() if rm_prefix(key) in valid_keys}

    # make actual connection:
    return connect(**clean_params)


# Add from_ini() as attribute to the connect method, so to use it do: pyhdb.connect.from_ini(ini_file)
connect.from_ini = from_ini
# ... and cleanup the local namespace:
del from_ini
