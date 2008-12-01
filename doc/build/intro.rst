.. _overview:

=======================
Overview / Installation
=======================

Overview
========


The SQLAlchemy SQL Toolkit and Object Relational Mapper is a comprehensive set of tools for working with databases and Python.  It has several distinct areas of functionality which can be used individually or combined together.  Its major API components, all public-facing, are illustrated below::

               +-----------------------------------------------------------+
               |             Object Relational Mapper (ORM)                |
               +-----------------------------------------------------------+
               +---------+ +------------------------------------+ +--------+
               |         | |       SQL Expression Language      | |        |
               |         | +------------------------------------+ |        |
               |         +-----------------------+ +--------------+        |
               |        Dialect/Execution        | |    Schema Management  |
               +---------------------------------+ +-----------------------+
               +----------------------+ +----------------------------------+
               |  Connection Pooling  | |              Types               |
               +----------------------+ +----------------------------------+

Above, the two most significant front-facing portions of SQLAlchemy are the **Object Relational Mapper** and the **SQL Expression Language**.  These are two separate toolkits, one building off the other.  SQL Expressions can be used independently of the ORM.  When using the ORM, the SQL Expression language is used to establish object-relational configurations as well as in querying.

Tutorials
=========

 * `Object Relational Tutorial` - This describes the richest feature of SQLAlchemy, its object relational mapper.  If you want to work with higher-level SQL which is constructed automatically for you, as well as management of Python objects, proceed to this tutorial.
 * :ref:`sqlexpression` - The core of SQLAlchemy is its SQL expression language.  The SQL Expression Language is a toolkit all its own, independent of the ORM package, which can be used to construct manipulable SQL expressions which can be programmatically constructed, modified, and executed, returning cursor-like result sets.  It's a lot more lightweight than the ORM and is appropriate for higher scaling SQL operations.  It's also heavily present within the ORM's public facing API, so advanced ORM users will want to master this language as well.

Reference Documentation
=======================


 * `Datamapping` - A comprehensive walkthrough of major ORM patterns and techniques.
 * `Session` - A detailed description of SQLAlchemy's Session object
 * `Engines` - Describes SQLAlchemy's database-connection facilities, including connection documentation and working with connections and transactions. 
 * `Connection Pools` - Further detail about SQLAlchemy's connection pool library.
 * `Metadata` - All about schema management using ``MetaData`` and ``Table`` objects; reading database schemas into your application, creating and dropping tables, constraints, defaults, sequences, indexes.
 * `Types` - Datatypes included with SQLAlchemy, their functions, as well as how to create your own types.
 * `Plugins` - Included addons for SQLAlchemy

Installing SQLAlchemy 
======================


Installing SQLAlchemy from scratch is most easily achieved with [setuptools][].  ([setuptools installation][install setuptools]). Just run this from the command-line:
    
.. sourcecode:: none

    # easy_install SQLAlchemy

This command will download the latest version of SQLAlchemy from the `Python Cheese Shop <http://pypi.python.org/pypi/SQLAlchemy>`_ and install it to your system.

* `setuptools <http://peak.telecommunity.com/DevCenter/setuptools>`_
* `install setuptools <http://peak.telecommunity.com/DevCenter/EasyInstall#installation-instructions>`_
* `pypi <http://pypi.python.org/pypi/SQLAlchemy>`_

Otherwise, you can install from the distribution using the ``setup.py`` script:

.. sourcecode:: none

    # python setup.py install

Installing a Database API 
==========================

SQLAlchemy is designed to operate with a `DB-API <http://www.python.org/doc/peps/pep-0249/>`_ implementation built for a particular database, and includes support for the most popular databases:

* Postgres:  `psycopg2 <http://www.initd.org/tracker/psycopg/wiki/PsycopgTwo>`_
* SQLite:  [pysqlite](http://initd.org/tracker/pysqlite), [sqlite3](http://docs.python.org/lib/module-sqlite3.html) (included with Python 2.5 or greater)
* MySQL:   [MySQLdb](http://sourceforge.net/projects/mysql-python)
* Oracle:  [cx_Oracle](http://www.cxtools.net/default.aspx?nav=home)
* MS-SQL, MSAccess:  [pyodbc](http://pyodbc.sourceforge.net/) (recommended), [adodbapi](http://adodbapi.sourceforge.net/)  or [pymssql](http://pymssql.sourceforge.net/)
* Firebird:  [kinterbasdb](http://kinterbasdb.sourceforge.net/)
* Informix:  [informixdb](http://informixdb.sourceforge.net/)
* DB2/Informix IDS: [ibm-db](http://code.google.com/p/ibm-db/)
* Sybase:   TODO
* MAXDB:    TODO

Checking the Installed SQLAlchemy Version
=========================================

 
This documentation covers SQLAlchemy version 0.5.  If you're working on a system that already has SQLAlchemy installed, check the version from your Python prompt like this:

.. sourcecode:: python+sql

     >>> import sqlalchemy
     >>> sqlalchemy.__version__ # doctest: +SKIP
     0.5.0

0.4 to 0.5 Migration 
=====================


Notes on what's changed from 0.4 to 0.5 is available on the SQLAlchemy wiki at [05Migration](http://www.sqlalchemy.org/trac/wiki/05Migration).
