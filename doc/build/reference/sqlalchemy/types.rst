Column and Data Types
=====================

.. module:: sqlalchemy.types

SQLAlchemy provides abstractions for most common database data types,
and a mechanism for specifying your own custom data types.

The methods and attributes of type objects are rarely used directly.
Type objects are supplied to :class:`~sqlalchemy.Table` definitions
and can be supplied as type hints to `functions` for occasions where
the database driver returns an incorrect type.

.. code-block:: python

  >>> users = Table('users', metadata,
  ...               Column('id', Integer, primary_key=True)
  ...               Column('name', String(32)))

SQLAlchemy will use the ``Integer`` and ``String(32)`` type
information when issuing a ``CREATE TABLE`` statement and will use it
again when reading back rows `SELECTed` from the database.

For more information, see the :ref:`types` tutorial.

Generic Types
-------------

Generic types specify a column that can read, write and store a
particular type of Python data.  SQLAlchemy will choose the best
database column type available on the target database when issuing a
``CREATE TABLE`` statement.  For complete control over which column
type is emitted in ``CREATE TABLE``, such as ``VARCHAR`` see `SQL
Standard Types`_ and the other sections of this chapter.

.. autoclass:: String
   :members: __init__
   :show-inheritance:


.. autoclass:: Text
   :members: __init__
   :show-inheritance:


.. autoclass:: Integer
   :members: __init__
   :show-inheritance:


.. autoclass:: SmallInteger
   :members: __init__
   :show-inheritance:


.. autoclass:: Numeric
   :members: __init__
   :show-inheritance:


.. autoclass:: Float
   :members: __init__
   :show-inheritance:


.. autoclass:: DateTime
   :members: __init__
   :show-inheritance:


.. autoclass:: Date
   :members: __init__
   :show-inheritance:


.. autoclass:: Time
   :members: __init__
   :show-inheritance:


.. autoclass:: Binary
   :members: __init__
   :show-inheritance:


.. autoclass:: Boolean
   :members: __init__
   :show-inheritance:


.. autoclass:: Unicode
   :members: __init__
   :show-inheritance:


.. autoclass:: UnicodeText
   :members: __init__
   :show-inheritance:


.. autoclass:: PickleType
   :members: __init__
   :show-inheritance:


.. autoclass:: Interval
   :members: __init__
   :show-inheritance:


SQL Standard Types
------------------

The SQL standard types always create database column types of the same
name when ``CREATE TABLE`` is issued.

.. autoclass:: INT
   :members: __init__
   :show-inheritance:


.. autoclass:: CHAR
   :members: __init__
   :show-inheritance:


.. autoclass:: VARCHAR
   :members: __init__
   :show-inheritance:


.. autoclass:: NCHAR
   :members: __init__
   :show-inheritance:


.. autoclass:: TEXT
   :members: __init__
   :show-inheritance:


.. autoclass:: FLOAT
   :members: __init__
   :show-inheritance:


.. autoclass:: NUMERIC
   :members: __init__
   :show-inheritance:


.. autoclass:: DECIMAL
   :members: __init__
   :show-inheritance:


.. autoclass:: TIMESTAMP
   :members: __init__
   :show-inheritance:


.. autoclass:: DATETIME
   :members: __init__
   :show-inheritance:


.. autoclass:: CLOB
   :members: __init__
   :show-inheritance:


.. autoclass:: BLOB
   :members: __init__
   :show-inheritance:


.. autoclass:: BOOLEAN
   :members: __init__
   :show-inheritance:


.. autoclass:: SMALLINT
   :members: __init__
   :show-inheritance:


.. autoclass:: DATE
   :members: __init__
   :show-inheritance:


.. autoclass:: TIME
   :members: __init__
   :show-inheritance:


Vendor-Specific Types
---------------------

Database-specific types are also available for import from each
database's dialect module. See the :ref:`sqlalchemy.databases`
reference.

Custom Types
------------


.. autoclass:: TypeDecorator
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:

.. autoclass:: TypeEngine
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:

.. autoclass:: AbstractType
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:

.. autoclass:: MutableType
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:

.. autoclass:: Concatenable
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:

.. autoclass:: NullType
   :show-inheritance:

