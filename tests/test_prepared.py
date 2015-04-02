# -*- coding: utf-8 -*-

import ConfigParser

import pyhdb

from collections import namedtuple

HANASystem = namedtuple('HANASystem', ['host', 'port', 'user', 'password'])

# get connection parameters
with open('../pytest.ini') as fp:
    cp = ConfigParser.ConfigParser()
    cp.readfp(fp)
    host = cp.get('pytest', 'hana-host')
    port = cp.get('pytest', 'hana-port') or 30015
    user = cp.get('pytest', 'hana-user')
    password = cp.get('pytest', 'hana-password')

# open connection
connection = pyhdb.connect(*HANASystem(host, port, user, password))

# get cursor
cursor = connection.cursor()

# test direct
# sql = 'select SCHEMA_NAME, TABLE_NAME from TABLES'
#cursor.execute(sql)
#r = cursor.fetchall()

# prepare statement
sql_to_prepare = 'select top ? * from test.employees where emp_no > ?'
statement_id = cursor.prepare(sql_to_prepare)

# check results
cursor.prepared_statement()

ps = cursor.prepared_statement(statement_id)
print ps.param()


