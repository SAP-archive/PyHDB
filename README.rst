SAP HANA Database Client for Python
===================================

A pure Python client for the SAP HANA Database based on the `SAP HANA Database SQL Command Network Protocol <http://help.sap.com/hana/SAP_HANA_SQL_Command_Network_Protocol_Reference_en.pdf>`_.

pyhdb supports Python 2.6, 2.7, 3.3, 3.4 and also PyPy on Linux, OSX and Windows. It implements a large part of the `DBAPI Specification v2.0 (PEP 249) <http://legacy.python.org/dev/peps/pep-0249/>`_.

Table of contents
-----------------

* `Install <#install>`_
* `Getting started <#getting-started>`_
* `Establish a database connection <#establish-a-database-connection>`_
* `Cursor object <#cursor-object>`_
* `Transaction handling <#transaction-handling>`_
* `Contribute <#contribute>`_

Install
-------

Install from Python Package Index:

.. code-block:: bash

    $ pip install pyhdb

You can also install the latest version direct from a cloned git repository.

.. code-block:: bash

    $ git clone https://github.com/SAP/pyhdb.git
    $ cd pyhdb
    $ python setup.py install


Getting started
---------------

If you do not have access to a SAP HANA server, go to the `SAP HANA Developer Center <http://scn.sap.com/community/developer-center/hana>`_ and choose one of the options to `get your own trial SAP HANA Server <http://scn.sap.com/docs/DOC-31722>`_.

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

The function ``pyhdb.connect`` creates a new database session and returns a new ``Connection`` instance. Please note that port isn't the instance number of you SAP HANA database. The SQL port of your SAP HANA is made up of ``3<instance-number>15`` for example the port of the default instance number ``00`` is ``30015``.

Currently pyhdb only supports the user and password authentication method. If you need another authentication method like SAML or Kerberos than please open a GitHub issue. Also there is currently no support of encrypted network communication between client and database.

Cursor object
-------------

With the method ``cursor`` of your ``Connection`` object you create a new ``Cursor`` object. This object is able to execute SQL statements and fetch one or multiple rows of the resultset from the database.

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

You can also execute DML Statements with the execute method like ``INSERT`` or ``DELETE``. The Cursor attribute ``rowcount`` contains the number of affected rows by the last statement.

.. code-block:: pycon

    >>> cursor.execute("INSERT INTO PYHDB_TEST VALUES('Hello Python World')")
    >>> cursor.rowcount
    1

Transaction handling
--------------------

Please note that all cursors created from the same connection are not isolated. Any change done by one cursor is immediately visible to all other cursors from same connection. Cursors created from different connections are isolated as the connection based on the normal transaction handling.

The connection objects provides to method ``commit`` which commit any pending transaction of the connection. The method ``rollback`` undo all changes since the last commit.

Contribute
----------

If you found bugs or have other issues than you are welcome to create a GitHub Issue. If you have questions about usage or something similar please create a `Stack Overflow <http://stackoverflow.com/>`_ Question with tag `pyhdb <http://stackoverflow.com/questions/tagged/pyhdb>`_.

Run tests
^^^^^^^^^

pyhdb provides a test suite which covers the most use-cases and protocol parts. To run the test suite you need the ``pytest`` and ``mock`` package. Afterwards just run ``py.test`` inside of the root directory of the repository.

.. code-block:: bash

    $ pip install pytest mock
    $ py.test

You can also test different python version with ``tox``.

.. code-block:: bash

    $ pip install tox
    $ tox

ToDos
^^^^^

* BLOB, LOB and NLOB Support
* Allow execution of stored database procedure
* Support of ``SELECT FOR UPDATE``
* Authentication methods

  * SAML
  * Kerberos
