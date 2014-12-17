import random
import datetime
from decimal import Decimal, getcontext

import pytest

@pytest.mark.hanatest
def test_dummy_sql_int(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT 1 FROM DUMMY")

    result = cursor.fetchone()
    assert result == (1,)

@pytest.mark.hanatest
def test_dummy_sql_decimal(connection):
    getcontext().prec = 36

    cursor = connection.cursor()
    cursor.execute("SELECT -312313212312321.1245678910111213142 FROM DUMMY")

    result = cursor.fetchone()
    assert result == (Decimal('-312313212312321.1245678910111213142'),)

@pytest.mark.hanatest
def test_dummy_sql_string(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT 'Hello World' FROM DUMMY")

    result = cursor.fetchone()
    assert result == ("Hello World",)

@pytest.mark.hanatest
def test_dummy_sql_long_string(connection):
    test_string = '%030x' % random.randrange(16**300)

    cursor = connection.cursor()
    cursor.execute("SELECT '%s' FROM DUMMY" % test_string)

    result = cursor.fetchone()
    assert result == (test_string,)

@pytest.mark.hanatest
def test_dummy_sql_binary(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT X'FF00FFA3B5' FROM DUMMY")

    result = cursor.fetchone()
    assert result == (b"\xFF\x00\xFF\xA3\xB5",)

@pytest.mark.hanatest
def test_dummy_sql_current_time(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT current_time FROM DUMMY")

    result = cursor.fetchone()
    assert isinstance(result[0], datetime.time)

@pytest.mark.hanatest
def test_dummy_sql_to_time(connection):
    now = datetime.datetime.now().time()

    cursor = connection.cursor()
    cursor.execute("SELECT to_time(%s) FROM DUMMY", (now,))

    result = cursor.fetchone()

    # No support of microsecond
    assert result[0] == now.replace(microsecond=0)

@pytest.mark.hanatest
def test_dummy_sql_current_date(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT current_date FROM DUMMY")

    result = cursor.fetchone()
    assert isinstance(result[0], datetime.date)

@pytest.mark.hanatest
def test_dummy_sql_to_date(connection):
    today = datetime.date.today()

    cursor = connection.cursor()
    cursor.execute("SELECT to_date(%s) FROM DUMMY", (today,))

    result = cursor.fetchone()
    assert result[0] == today

@pytest.mark.hanatest
def test_dummy_sql_current_timestamp(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT current_timestamp FROM DUMMY")

    result = cursor.fetchone()
    assert isinstance(result[0], datetime.datetime)

@pytest.mark.hanatest
def test_dummy_sql_to_timestamp(connection):
    now = datetime.datetime.now()
    now = now.replace(microsecond=123000)

    cursor = connection.cursor()
    cursor.execute("SELECT to_timestamp(%s) FROM DUMMY", (now,))

    result = cursor.fetchone()
    assert result[0] == now

@pytest.mark.hanatest
def test_dummy_sql_without_result(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT 1 FROM DUMMY WHERE 1 != 1")

    result = cursor.fetchone()
    assert result == None
