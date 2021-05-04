SAP HANA Database Client for Python
===================================

Important Notice
----------------

.. image:: https://img.shields.io/badge/STATUS-NOT%20CURRENTLY%20MAINTAINED-red.svg?longCache=true&style=flat


This public repository is read-only and no longer maintained.

The active maintained alternative is SAP HANA Python Client: https://pypi.org/project/hdbcli/

A pure Python client for the SAP HANA Database based on the `SAP HANA Database SQL Command Network Protocol <http://help.sap.com/hana/SAP_HANA_SQL_Command_Network_Protocol_Reference_en.pdf>`_.

pyhdb supports Python 2.7, 3.3, 3.4, 3.5, 3.6 and also PyPy on Linux, OSX and Windows. It implements a large part of the `DBAPI Specification v2.0 (PEP 249) <http://legacy.python.org/dev/peps/pep-0249/>`_.

Table of contents
-----------------

* `Install <#install>`_
* `Getting started <#getting-started>`_
* `Establish a database connection <#establish-a-database-connection>`_
* `Cursor object <#cursor-object>`_
* `Large Objects (LOBs) <#lobs>`_
* `Stored Procedures <#stored-procedures>`_
* `Transaction handling <#transaction-handling>`_
* `Contribute <#contribute>`_

Install
-------

Install from Python Package Index:

.. code-block:: bash

    $ pip install pyhdb

Install from GitHub via pip:

.. code-block:: bash

    $ pip install git+https://github.com/SAP/pyhdb.git

You can also install the latest version direct from a cloned git repository.

.. code-block:: bash

    $ git clone https://github.com/SAP/pyhdb.git
    $ cd pyhdb
    $ python setup.py install


Getting started
---------------

If you do not have access to a SAP HANA server, go to the `SAP HANA Developer Center <http://scn.sap.com/community/developer-center/hana>`_ and choose one of the options to `get your own trial SAP HANA Server <http://scn.sap.com/docs/DOC-31722>`_.

For using PyHDB with hanatrial instance, follow `this guide <http://scn.sap.com/community/developer-center/hana/blog/2015/04/24/try-hanatrial-using-python-or-nodejs>`_.

The basic pyhdb usage is common to database adapters implementing the `DBAPI 2.0 interface (PEP 249) <http://legacy.python.org/dev/peps/pep-0249/>`_. The following example shows how easy it's to use the pyhdb module.

.. code-block:: pycon

    >>> import pyhdb
    >>> connection = pyhdb.connect(
        host="example.com",
        port=30015,
        user="user",
        password="secret"
    )

    >>> cursor = connection.cursor()
    >>> cursor.execute("SELECT 'Hello Python World' FROM DUMMY")
    >>> cursor.fetchone()
    (u"Hello Python World",)

    >>> connection.close()

Establish a database connection
-------------------------------

The function ``pyhdb.connect`` creates a new database session and returns a new ``Connection`` instance.
Please note that port isn't the instance number of you SAP HANA database. The SQL port of your SAP
HANA is made up of ``3<instance-number>15`` for example the port of the default instance number ``00`` is ``30015``.

Currently pyhdb only supports the user and password authentication method. If you need another
authentication method like SAML or Kerberos than please open a GitHub issue. Also there is currently
no support of encrypted network communication between client and database.

Cursor object
-------------

With the method ``cursor`` of your ``Connection`` object you create a new ``Cursor`` object.
This object is able to execute SQL statements and fetch one or multiple rows of the resultset from the database.

Example select
^^^^^^^^^^^^^^

.. code-block:: pycon

    >>> cursor = connection.cursor()
    >>> cursor.execute("SELECT SCHEMA_NAME, TABLE_NAME FROM TABLES")


After you executed a statement you can fetch one or multiple rows from the resultset.


.. code-block:: pycon

    >>> cursor.fetchone()
    (u'SYS', u'DUMMY')

    >>> cursor.fetchmany(3)
    [(u'SYS', u'DUMMY'), (u'SYS', u'PROCEDURE_DATAFLOWS'), (u'SYS', u'PROCEDURE_MAPPING')]

You can also fetch all rows from your resultset.

.. code-block:: pycon

    >>> cursor.fetchall()
    [(u'SYS', u'DUMMY'), (u'SYS', u'PROCEDURE_DATAFLOWS'), (u'SYS', u'PROCEDURE_MAPPING'), ...]


Example Create table
^^^^^^^^^^^^^^^^^^^^

With the execute method you can also execute DDL statements like ``CREATE TABLE``.

.. code-block:: pycon

    >>> cursor.execute('CREATE TABLE PYHDB_TEST("NAMES" VARCHAR (255) null)')


Example insert
^^^^^^^^^^^^^^

You can also execute DML Statements with the execute method like ``INSERT`` or ``DELETE``. The Cursor
attribute ``rowcount`` contains the number of affected rows by the last statement.

.. code-block:: pycon

    >>> cursor.execute("INSERT INTO PYHDB_TEST VALUES('Hello Python World')")
    >>> cursor.rowcount
    1


LOBs
^^^^

Three different types of LOBs are supported and corresponding LOB classes have been implemented:
* Blob - binary LOB data
* Clob - string LOB data containing only ascii characters
* NClob - string (unicode for Python 2.x) LOB data containing any valid unicode character

LOB instance provide a file-like interface (similar to StringIO instances) for accessing LOB data.
For HANA LOBs lazy loading of the actual data is implemented behind the scenes. An initial select statement for a LOB
only loads the first 1024 bytes on the client:

 .. code-block:: pycon

    >>> mylob = cursor.execute('select myclob from mytable where id=:1', [some_id]).fetchone()[0]
    >>> mylob
    <Clob length: 2000 (currently loaded from hana: 1024)>

By calling the read(<num-chars>)-method more data will be loaded from the database once <num-chars> exceeds the number
of currently loaded data:

 .. code-block:: pycon

    >>> myload.read(1500)   # -> returns the first 1500 chars, by loading extra 476 chars from the db
    >>> mylob
    <Clob length: 2000 (currently loaded from hana: 1500)>
    >>> myload.read()   # -> returns the last 500 chars by loading them from the db
    >>> mylob
    <Clob length: 2000 (currently loaded from hana: 2000)>

Using the ``seek()`` methods it is possible to point the file pointer position within the LOB to arbitrary positions.
``tell()`` will return the current position.


LOBs can be written to the database via ``insert`` or ``update``-statemens with LOB data provided either
as strings or LOB instances:

 .. code-block:: pycon

    >>> from pyhdb import NClob
    >>> nclob_data = u'朱の子ましける日におえつかうまつ'
    >>> nclob = NClob(nclob_data)
    >>> cursor.execute('update mynclob set nclob_1=:1, nclob_2=:2 where id=:3', [nclob, nclob_data, myid])

.. note:: Currently LOBs can only be written in the database for sizes up to 128k (entire amount of data provided in one
          ``update`` or ``insert`` statement). This constraint will be removed in one of the next releases of PyHDB.
          This limitation does however not apply when reading LOB data from the database.


Stored Procedures
^^^^^^^^^^^^^^^^^

Rudimentary support for Stored Procedures call, scalar parameters only:

The script shall call the stored procedure PROC_ADD2 (source below):

 .. code-block:: pycon

    >>> sql_to_prepare = 'call PROC_ADD2 (?, ?, ?, ?)'
    >>> params = {'A':2, 'B':5, 'C':None, 'D': None}
    >>> psid = cursor.prepare(sql_to_prepare)
    >>> ps = cursor.get_prepared_statement(psid)
    >>> cursor.execute_prepared(ps, [params])
    >>> result = cursor.fetchall()
    >>> for l in result:
    >>>     print l

from the stored procedure:

 .. code-block:: sql

    create procedure PROC_ADD2 (in a int, in b int, out c int, out d char)
    language sqlscript
    reads sql data as
    begin
        c := :a + :b;
        d := 'A';
    end

Transaction handling
--------------------

Please note that all cursors created from the same connection are not isolated. Any change done by one
cursor is immediately visible to all other cursors from same connection. Cursors created from different
connections are isolated as the connection based on the normal transaction handling.

The connection objects provides to method ``commit`` which commit any pending transaction of the
connection. The method ``rollback`` undo all changes since the last commit.

Contribute
----------

If you found bugs or have other issues than you are welcome to create a GitHub Issue. If you have
questions about usage or something similar please create a `Stack Overflow <http://stackoverflow.com/>`_
Question with tag `pyhdb <http://stackoverflow.com/questions/tagged/pyhdb>`_.

Run tests
^^^^^^^^^

pyhdb provides a test suite which covers the most use-cases and protocol parts. To run the test suite
you need the ``pytest`` and ``mock`` package. Afterwards just run ``py.test`` inside of the root
directory of the repository.

.. code-block:: bash

    $ pip install pytest mock
    $ py.test

You can also test different python version with ``tox``.

.. code-block:: bash

    $ pip install tox
    $ tox

Tracing
^^^^^^^

For debugging purposes it is sometimes useful to get detailed tracing information about packages sent to hana and
those received from the database. There are two ways to turn on the print out of tracing information:

1. Set the environment variable HDB_TRACING=1 before starting Python, e.g. (bash-syntax!):

.. code-block:: bash

   $ HDB_TRACE=1 python

2. Import the pyhdb module and set ``pyhdb.tracing = True``

Then perform some statements on the database and enjoy the output.

To get tracing information when running pytest provide the ``-s`` option:

.. code-block:: bash

    $ HDB_TRACE=1 py.test -s


ToDos
^^^^^

* Allow execution of stored database procedure
* Support of ``SELECT FOR UPDATE``
* Authentication methods

  * SAML
  * Kerberos
