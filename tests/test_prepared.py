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
#sql = 'select SCHEMA_NAME, TABLE_NAME from TABLES'
#sql = 'select top 5 * from test.employees where emp_no > 10500'
#cursor.execute(sql)
#r = cursor.fetchall()
#for line in r:
#    print line

# prepare w. placeholders
#sql_to_prepare = 'select top ? * from test.employees where emp_no > 10500'
#sql_to_prepare = 'select top 3 * from test.employees where emp_no > ?'
sql_to_prepare = 'select top ? * from test.employees where emp_no > ?'

statement_id = cursor.prepare(sql_to_prepare)
#print 'sid raw:', statement_id

# prepare named
# sql_to_prepare = 'select top $top1 * from test.employees where emp_no > $empno'
# statement_id = cursor.prepare(sql_to_prepare)
## DatabaseError: cannot use parameter variable: TOP1: line 1 col 12 (at pos 11)


ps = cursor.prepared_statement(statement_id)
print 'sid:', ps.statement_xid

# sql 1
#ps.set_parameter_value(1, 3)

# sql 2
#ps.set_parameter_value(1, 10500)

# sql 3
ps.set_parameter_value(1, 5)
ps.set_parameter_value(2, 11500)

for p in ps.parameters:
    print p

cursor.execute_prepared(ps)

r = cursor.fetchall()

print len(r)

if len(r) < 33:
    for line in r:
        print line

