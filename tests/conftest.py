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

from collections import namedtuple

import pytest
import pyhdb

HANASystem = namedtuple(
    'HANASystem', ['host', 'port', 'user', 'password']
)


def _get_option(config, key):
    return config.getoption(key) or config.inicfg.get(key)


@pytest.fixture(scope="session")
def hana_system(request):
    return hana_system_with_config(request.config)


def hana_system_with_config(config):
    host = _get_option(config, 'hana_host')
    port = _get_option(config, 'hana_port') or 30015
    user = _get_option(config, 'hana_user')
    password = _get_option(config, 'hana_password')
    return HANASystem(host, port, user, password)


@pytest.fixture()
def connection(request, hana_system):
    connection = pyhdb.connect(*hana_system)

    def _close():
        connection.close()

    request.addfinalizer(_close)
    return connection


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "hanatest: mark test to run only with SAP HANA system"
    )


def pytest_addoption(parser):
    parser.addoption(
        "--hana-host",
        help="Address of SAP HANA system for integration tests"
    )
    parser.addoption(
        "--hana-port", type=int,
        help="Port of SAP HANA system"
    )
    parser.addoption(
        "--hana-user",
        help="User for SAP HANA system"
    )
    parser.addoption(
        "--hana-password",
        help="Password for SAP HANA user"
    )
    parser.addoption(
        "--no-hana",
        action="store_true",
        help="Specify this option to omit all tests interacting with a HANA database"
    )


def pytest_report_header(config):
    hana = hana_system_with_config(config)
    if hana.host is None:
        return [
            "WARNING: No SAP HANA host defined for integration tests"
        ]
    else:
        return [
            "SAP HANA test system",
            "  Host: %s:%s" % (hana.host, hana.port),
            "  User: %s" % hana.user
        ]


def pytest_runtest_setup(item):
    try:
        hana_marker = item.get_closest_marker("hanatest")
    except AttributeError:
        hana_marker = item.get_marker("hanatest")

    if hana_marker is not None:
        if item.config.getoption("--no-hana"):
            pytest.skip("Test requires SAP HANA system are omitted due to command line option")
        else:
            hana = hana_system_with_config(item.config)
            if hana.host is None:
                pytest.skip("Test requires SAP HANA system")
