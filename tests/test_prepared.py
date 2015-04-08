# -*- coding: utf-8 -*-

import pyhdb

import ConfigParser
from collections import namedtuple
import datetime

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

# prepare named
# sql_to_prepare = 'select top $top1 * from test.employees where emp_no > $empno'
# statement_id = cursor.prepare(sql_to_prepare)
## DatabaseError: cannot use parameter variable: TOP1: line 1 col 12 (at pos 11)

# prepare w. placeholders
#sql_to_prepare = 'select top ? * from test.employees where emp_no > 10500'
#sql_to_prepare = 'select top 3 * from test.employees where emp_no > ?'
#sql_to_prepare = 'select top ? * from test.employees where emp_no > ? order by first_name'
sql_to_prepare = "select top ? * from test.employees where emp_no > ? and first_name like ? order by birth_date"
#sql_to_prepare = "select top ? * from test.employees where emp_no > ? and first_name like ? order by birth_date"
#sql_to_prepare = 'insert into test.employees values (?,?,?,?,?,?)'
#sql_to_prepare = 'insert into test.departments values (?,?)'

statement_id = cursor.prepare(sql_to_prepare)
ps = cursor.prepared_statement(statement_id)
print 'sid:', ps.statement_xid, statement_id
for p in ps.parameters:
    print p

# sql 1
#ps.set_parameter_value(1, 3)

# sql 2
#ps.set_parameter_value(1, 10500)

# sql 3
#ps.set_parameters([3, 5])
#ps.set_parameter_value(1, 5)
#ps.set_parameter_value(2, 11500)
cursor.execute_prepared(ps, [7, 10500, 'Aamer'])
#cursor.execute_prepared(ps, [500, datetime.date(1001, 1, 1), 'Lao', 'Tse', 'M', datetime.date(1002, 2, 2)])
#cursor.execute_prepared(ps, [500, 728655, 'Lao', 'Tse', 'M', 735689])
#cursor.execute_prepared(ps, ['t001', 'TestDep1'])
'''
sql_to_prepare = 'select top ? * from test.employees where birth_date = ?'
statement_id = cursor.prepare(sql_to_prepare)
ps = cursor.prepared_statement(statement_id)
cursor.execute_prepared(ps, [3, datetime.date(1952, 2, 1)])
'''


r = cursor.fetchall()
print cursor.rowcount
for line in r:
    print line
#
#r = cursor.rowcount
#print r
