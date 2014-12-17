from pyhdb.client import Connection
from pyhdb.exceptions import *

apilevel = "2.0"
threadsafety = 2
paramstyle = "format"

def connect(host, port, user, password, autocommit=False):
    connection = Connection(host, port, user, password, autocommit)
    connection.connect()
    return connection
