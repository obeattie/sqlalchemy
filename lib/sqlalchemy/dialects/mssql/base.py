# mssql.py

"""Support for the Microsoft SQL Server database.

Driver
------

The MSSQL dialect will work with three different available drivers:

* *pyodbc* - http://pyodbc.sourceforge.net/. This is the recommeded
  driver.

* *pymssql* - http://pymssql.sourceforge.net/

* *adodbapi* - http://adodbapi.sourceforge.net/

Drivers are loaded in the order listed above based on availability.

If you need to load a specific driver pass ``module_name`` when
creating the engine::

    engine = create_engine('mssql+module_name://dsn')

``module_name`` currently accepts: ``pyodbc``, ``pymssql``, and
``adodbapi``.

Currently the pyodbc driver offers the greatest level of
compatibility.

Connecting
----------

Connecting with create_engine() uses the standard URL approach of
``mssql://user:pass@host/dbname[?key=value&key=value...]``.

If the database name is present, the tokens are converted to a
connection string with the specified values. If the database is not
present, then the host token is taken directly as the DSN name.

Examples of pyodbc connection string URLs:

* *mssql+pyodbc://mydsn* - connects using the specified DSN named ``mydsn``.
  The connection string that is created will appear like::

    dsn=mydsn;TrustedConnection=Yes

* *mssql+pyodbc://user:pass@mydsn* - connects using the DSN named
  ``mydsn`` passing in the ``UID`` and ``PWD`` information. The
  connection string that is created will appear like::

    dsn=mydsn;UID=user;PWD=pass

* *mssql+pyodbc://user:pass@mydsn/?LANGUAGE=us_english* - connects
  using the DSN named ``mydsn`` passing in the ``UID`` and ``PWD``
  information, plus the additional connection configuration option
  ``LANGUAGE``. The connection string that is created will appear
  like::

    dsn=mydsn;UID=user;PWD=pass;LANGUAGE=us_english

* *mssql+pyodbc://user:pass@host/db* - connects using a connection string
  dynamically created that would appear like::

    DRIVER={SQL Server};Server=host;Database=db;UID=user;PWD=pass

* *mssql+pyodbc://user:pass@host:123/db* - connects using a connection
  string that is dynamically created, which also includes the port
  information using the comma syntax. If your connection string
  requires the port information to be passed as a ``port`` keyword
  see the next example. This will create the following connection
  string::

    DRIVER={SQL Server};Server=host,123;Database=db;UID=user;PWD=pass

* *mssql+pyodbc://user:pass@host/db?port=123* - connects using a connection
  string that is dynamically created that includes the port
  information as a separate ``port`` keyword. This will create the
  following connection string::

    DRIVER={SQL Server};Server=host;Database=db;UID=user;PWD=pass;port=123

If you require a connection string that is outside the options
presented above, use the ``odbc_connect`` keyword to pass in a
urlencoded connection string. What gets passed in will be urldecoded
and passed directly.

For example::

    mssql+pyodbc:///?odbc_connect=dsn%3Dmydsn%3BDatabase%3Ddb

would create the following connection string::

    dsn=mydsn;Database=db

Encoding your connection string can be easily accomplished through
the python shell. For example::

    >>> import urllib
    >>> urllib.quote_plus('dsn=mydsn;Database=db')
    'dsn%3Dmydsn%3BDatabase%3Ddb'

Additional arguments which may be specified either as query string
arguments on the URL, or as keyword argument to
:func:`~sqlalchemy.create_engine()` are:

* *auto_identity_insert* - enables support for IDENTITY inserts by
  automatically turning IDENTITY INSERT ON and OFF as required.
  Defaults to ``True``.

* *query_timeout* - allows you to override the default query timeout.
  Defaults to ``None``. This is only supported on pymssql.

* *use_scope_identity* - allows you to specify that SCOPE_IDENTITY
  should be used in place of the non-scoped version @@IDENTITY.
  Defaults to ``False``. On pymssql this defaults to ``True``, and on
  pyodbc this defaults to ``True`` if the version of pyodbc being
  used supports it.

* *has_window_funcs* - indicates whether or not window functions
  (LIMIT and OFFSET) are supported on the version of MSSQL being
  used. If you're running MSSQL 2005 or later turn this on to get
  OFFSET support. Defaults to ``False``.

* *max_identifier_length* - allows you to se the maximum length of
  identfiers supported by the database. Defaults to 128. For pymssql
  the default is 30.

* *schema_name* - use to set the schema name. Defaults to ``dbo``.

Auto Increment Behavior
-----------------------

``IDENTITY`` columns are supported by using SQLAlchemy
``schema.Sequence()`` objects. In other words::

    Table('test', mss_engine,
           Column('id', Integer,
                  Sequence('blah',100,10), primary_key=True),
           Column('name', String(20))
         ).create()

would yield::

   CREATE TABLE test (
     id INTEGER NOT NULL IDENTITY(100,10) PRIMARY KEY,
     name VARCHAR(20) NULL,
     )

Note that the ``start`` and ``increment`` values for sequences are
optional and will default to 1,1.

* Support for ``SET IDENTITY_INSERT ON`` mode (automagic on / off for
  ``INSERT`` s)

* Support for auto-fetching of ``@@IDENTITY/@@SCOPE_IDENTITY()`` on
  ``INSERT``

Collation Support
-----------------

MSSQL specific string types support a collation parameter that
creates a column-level specific collation for the column. The
collation parameter accepts a Windows Collation Name or a SQL
Collation Name. Supported types are MSChar, MSNChar, MSString,
MSNVarchar, MSText, and MSNText. For example::

    Column('login', String(32, collation='Latin1_General_CI_AS'))

will yield::

    login VARCHAR(32) COLLATE Latin1_General_CI_AS NULL

LIMIT/OFFSET Support
--------------------

MSSQL has no support for the LIMIT or OFFSET keysowrds. LIMIT is
supported directly through the ``TOP`` Transact SQL keyword::

    select.limit

will yield::

    SELECT TOP n

If the ``has_window_funcs`` flag is set then LIMIT with OFFSET
support is available through the ``ROW_NUMBER OVER`` construct. This
construct requires an ``ORDER BY`` to be specified as well and is
only available on MSSQL 2005 and later.

Nullability
-----------
MSSQL has support for three levels of column nullability. The default
nullability allows nulls and is explicit in the CREATE TABLE
construct::

    name VARCHAR(20) NULL

If ``nullable=None`` is specified then no specification is made. In
other words the database's configured default is used. This will
render::

    name VARCHAR(20)

If ``nullable`` is ``True`` or ``False`` then the column will be
``NULL` or ``NOT NULL`` respectively.

Date / Time Handling
--------------------
For MSSQL versions that support the ``DATE`` and ``TIME`` types
(MSSQL 2008+) the data type is used. For versions that do not
support the ``DATE`` and ``TIME`` types a ``DATETIME`` type is used
instead and the MSSQL dialect handles converting the results
properly. This means ``Date()`` and ``Time()`` are fully supported
on all versions of MSSQL. If you do not desire this behavior then
do not use the ``Date()`` or ``Time()`` types.

Compatibility Levels
--------------------
MSSQL supports the notion of setting compatibility levels at the
database level. This allows, for instance, to run a database that
is compatibile with SQL2000 while running on a SQL2005 database
server. ``server_version_info`` will always retrun the database
server version information (in this case SQL2005) and not the
compatibiility level information. Because of this, if running under
a backwards compatibility mode SQAlchemy may attempt to use T-SQL
statements that are unable to be parsed by the database server.

Known Issues
------------

* No support for more than one ``IDENTITY`` column per table

* pymssql has problems with binary and unicode data that this module
  does **not** work around

"""
import datetime, decimal, inspect, operator, sys

from sqlalchemy import sql, schema, exc, util
from sqlalchemy.sql import compiler, expression, operators as sql_operators, functions as sql_functions
from sqlalchemy.engine import default, base
from sqlalchemy import types as sqltypes
from decimal import Decimal as _python_Decimal


MSSQL_RESERVED_WORDS = set(['function'])


class MSNumeric(sqltypes.Numeric):
    def result_processor(self, dialect):
        if self.asdecimal:
            def process(value):
                if value is not None:
                    return _python_Decimal(str(value))
                else:
                    return value
            return process
        else:
            def process(value):
                return float(value)
            return process

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                # Not sure that this exception is needed
                return value
            else:
                if isinstance(value, decimal.Decimal):
                    sign = (value < 0 and '-' or '') 
                    if value._exp > -1:
                        return float(sign + value._int + '0' * value._exp)
                    else:
                        s = value._int.zfill(-value._exp+1)
                        pos = len(s) + value._exp
                        return sign + s[:pos] + '.' + s[pos:]
                else:
                    return value

        return process

class MSReal(sqltypes.Float):
    """A type for ``real`` numbers."""

    __visit_name__ = 'REAL'

    def __init__(self):
        super(MSReal, self).__init__(precision=24)

class MSTinyInteger(sqltypes.Integer):
    __visit_name__ = 'TINYINT'

class MSTime(sqltypes.Time):
    def __init__(self, precision=None, **kwargs):
        self.precision = precision
        super(MSTime, self).__init__()


class MSDateTime(sqltypes.DateTime):
    def bind_processor(self, dialect):
        # most DBAPIs allow a datetime.date object
        # as a datetime.
        def process(value):
            if type(value) is datetime.date:
                return datetime.datetime(value.year, value.month, value.day)
            return value
        return process
    
class MSSmallDateTime(MSDateTime):
    __visit_name__ = 'SMALLDATETIME'

class MSDateTime2(MSDateTime):
    __visit_name__ = 'DATETIME2'
    
    def __init__(self, precision=None, **kwargs):
        self.precision = precision

class MSDateTimeOffset(sqltypes.TypeEngine):
    __visit_name__ = 'DATETIMEOFFSET'
    
    def __init__(self, precision=None, **kwargs):
        self.precision = precision

class MSDateTimeAsDate(sqltypes.TypeDecorator):
    """ This is an implementation of the Date type for versions of MSSQL that
    do not support that specific type. In order to make it work a ``DATETIME``
    column specification is used and the results get converted back to just
    the date portion.

    """

    impl = sqltypes.DateTime

    def process_bind_param(self, value, dialect):
        if type(value) is datetime.date:
            return datetime.datetime(value.year, value.month, value.day)
        return value

    def process_result_value(self, value, dialect):
        if type(value) is datetime.datetime:
            return value.date()
        return value

class MSDateTimeAsTime(sqltypes.TypeDecorator):
    """ This is an implementation of the Time type for versions of MSSQL that
    do not support that specific type. In order to make it work a ``DATETIME``
    column specification is used and the results get converted back to just
    the time portion.

    """

    __zero_date = datetime.date(1900, 1, 1)

    impl = sqltypes.DateTime

    def process_bind_param(self, value, dialect):
        if type(value) is datetime.datetime:
            value = datetime.datetime.combine(self.__zero_date, value.time())
        elif type(value) is datetime.time:
            value = datetime.datetime.combine(self.__zero_date, value)
        return value

    def process_result_value(self, value, dialect):
        if type(value) is datetime.datetime:
            return value.time()
        elif type(value) is datetime.date:
            return datetime.time(0, 0, 0)
        return value


class _StringType(object):
    """Base for MSSQL string types."""

    def __init__(self, collation=None):
        self.collation = collation

    def __repr__(self):
        attributes = inspect.getargspec(self.__init__)[0][1:]
        attributes.extend(inspect.getargspec(_StringType.__init__)[0][1:])

        params = {}
        for attr in attributes:
            val = getattr(self, attr)
            if val is not None and val is not False:
                params[attr] = val

        return "%s(%s)" % (self.__class__.__name__,
                           ', '.join(['%s=%r' % (k, params[k]) for k in params]))


class MSText(_StringType, sqltypes.TEXT):
    """MSSQL TEXT type, for variable-length text up to 2^31 characters."""

    def __init__(self, *args, **kw):
        """Construct a TEXT.

        :param collation: Optional, a column-level collation for this string
          value. Accepts a Windows Collation Name or a SQL Collation Name.

        """
        collation = kw.pop('collation', None)
        _StringType.__init__(self, collation)
        sqltypes.Text.__init__(self, *args, **kw)

class MSNText(_StringType, sqltypes.UnicodeText):
    """MSSQL NTEXT type, for variable-length unicode text up to 2^30
    characters."""

    __visit_name__ = 'NTEXT'
    
    def __init__(self, *args, **kwargs):
        """Construct a NTEXT.

        :param collation: Optional, a column-level collation for this string
          value. Accepts a Windows Collation Name or a SQL Collation Name.

        """
        collation = kw.pop('collation', None)
        _StringType.__init__(self, collation)
        sqltypes.UnicodeText.__init__(self, None, **kw)


class MSString(_StringType, sqltypes.VARCHAR):
    """MSSQL VARCHAR type, for variable-length non-Unicode data with a maximum
    of 8,000 characters."""

    def __init__(self, *args, **kw):
        """Construct a VARCHAR.

        :param length: Optinal, maximum data length, in characters.

        :param convert_unicode: defaults to False.  If True, convert
          ``unicode`` data sent to the database to a ``str``
          bytestring, and convert bytestrings coming back from the
          database into ``unicode``.

          Bytestrings are encoded using the dialect's
          :attr:`~sqlalchemy.engine.base.Dialect.encoding`, which
          defaults to `utf-8`.

          If False, may be overridden by
          :attr:`sqlalchemy.engine.base.Dialect.convert_unicode`.

        :param assert_unicode:

          If None (the default), no assertion will take place unless
          overridden by :attr:`sqlalchemy.engine.base.Dialect.assert_unicode`.

          If 'warn', will issue a runtime warning if a ``str``
          instance is used as a bind value.

          If true, will raise an :exc:`sqlalchemy.exc.InvalidRequestError`.

        :param collation: Optional, a column-level collation for this string
          value. Accepts a Windows Collation Name or a SQL Collation Name.

        """
        collation = kw.pop('collation', None)
        _StringType.__init__(self, collation)
        sqltypes.VARCHAR.__init__(self, *args, **kw)

class MSNVarchar(_StringType, sqltypes.NVARCHAR):
    """MSSQL NVARCHAR type.

    For variable-length unicode character data up to 4,000 characters."""

    def __init__(self, *args, **kw):
        """Construct a NVARCHAR.

        :param length: Optional, Maximum data length, in characters.

        :param collation: Optional, a column-level collation for this string
          value. Accepts a Windows Collation Name or a SQL Collation Name.

        """
        collation = kw.pop('collation', None)
        _StringType.__init__(self, collation)
        sqltypes.NVARCHAR.__init__(self, *args, **kw)


class MSChar(_StringType, sqltypes.CHAR):
    """MSSQL CHAR type, for fixed-length non-Unicode data with a maximum
    of 8,000 characters."""

    def __init__(self, *args, **kw):
        """Construct a CHAR.

        :param length: Optinal, maximum data length, in characters.

        :param convert_unicode: defaults to False.  If True, convert
          ``unicode`` data sent to the database to a ``str``
          bytestring, and convert bytestrings coming back from the
          database into ``unicode``.

          Bytestrings are encoded using the dialect's
          :attr:`~sqlalchemy.engine.base.Dialect.encoding`, which
          defaults to `utf-8`.

          If False, may be overridden by
          :attr:`sqlalchemy.engine.base.Dialect.convert_unicode`.

        :param assert_unicode:

          If None (the default), no assertion will take place unless
          overridden by :attr:`sqlalchemy.engine.base.Dialect.assert_unicode`.

          If 'warn', will issue a runtime warning if a ``str``
          instance is used as a bind value.

          If true, will raise an :exc:`sqlalchemy.exc.InvalidRequestError`.

        :param collation: Optional, a column-level collation for this string
          value. Accepts a Windows Collation Name or a SQL Collation Name.

        """
        collation = kw.pop('collation', None)
        _StringType.__init__(self, collation)
        sqltypes.CHAR.__init__(self, *args, **kw)


class MSNChar(_StringType, sqltypes.NCHAR):
    """MSSQL NCHAR type.

    For fixed-length unicode character data up to 4,000 characters."""

    def __init__(self, *args, **kw):
        """Construct an NCHAR.

        :param length: Optional, Maximum data length, in characters.

        :param collation: Optional, a column-level collation for this string
          value. Accepts a Windows Collation Name or a SQL Collation Name.

        """
        collation = kw.pop('collation', None)
        _StringType.__init__(self, collation)
        sqltypes.NCHAR.__init__(self, *args, **kw)

class MSBinary(sqltypes.Binary):
    pass

class MSVarBinary(sqltypes.Binary):
    __visit_name__ = 'VARBINARY'

class MSImage(sqltypes.Binary):
    __visit_name__ = 'IMAGE'

class MSBit(sqltypes.TypeEngine):
    __visit_name__ = 'BIT'
    
class MSBoolean(sqltypes.Boolean):
    def result_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            return value and True or False
        return process

    def bind_processor(self, dialect):
        def process(value):
            if value is True:
                return 1
            elif value is False:
                return 0
            elif value is None:
                return None
            else:
                return value and True or False
        return process

class MSMoney(sqltypes.TypeEngine):
    __visit_name__ = 'MONEY'

class MSSmallMoney(MSMoney):
    __visit_name__ = 'SMALLMONEY'


class MSUniqueIdentifier(sqltypes.TypeEngine):
    __visit_name__ = "UNIQUEIDENTIFIER"

class MSVariant(sqltypes.TypeEngine):
    __visit_name__ = 'SQL_VARIANT'

class MSTypeCompiler(compiler.GenericTypeCompiler):
    def _extend(self, spec, type_):
        """Extend a string-type declaration with standard SQL
        COLLATE annotations.

        """

        if getattr(type_, 'collation', None):
            collation = 'COLLATE %s' % type_.collation
        else:
            collation = None

        if type_.length:
            spec = spec + "(%d)" % type_.length
        
        return ' '.join([c for c in (spec, collation)
            if c is not None])

    def visit_FLOAT(self, type_):
        precision = getattr(type_, 'precision', None)
        if precision is None:
            return "FLOAT"
        else:
            return "FLOAT(%(precision)s)" % {'precision': precision}

    def visit_REAL(self, type_):
        return "REAL"

    def visit_TINYINT(self, type_):
        return "TINYINT"

    def visit_DATETIMEOFFSET(self, type_):
        if type_.precision:
            return "DATETIMEOFFSET(%s)" % type_.precision
        else:
            return "DATETIMEOFFSET"

    def visit_TIME(self, type_):
        precision = getattr(type_, 'precision', None)
        if precision:
            return "TIME(%s)" % precision
        else:
            return "TIME"

    def visit_DATETIME2(self, type_):
        precision = getattr(type_, 'precision', None)
        if precision:
            return "DATETIME2(%s)" % precision
        else:
            return "DATETIME2"

    def visit_SMALLDATETIME(self, type_):
        return "SMALLDATETIME"

    def visit_NTEXT(self, type_):
        return self._extend("NTEXT", type_)

    def visit_TEXT(self, type_):
        return self._extend("TEXT", type_)

    def visit_VARCHAR(self, type_):
        return self._extend("VARCHAR", type_)

    def visit_CHAR(self, type_):
        return self._extend("CHAR", type_)

    def visit_NCHAR(self, type_):
        return self._extend("NCHAR", type_)

    def visit_NVARCHAR(self, type_):
        return self._extend("NVARCHAR", type_)

    def visit_binary(self, type_):
        if type_.length:
            return self.visit_BINARY(type_)
        else:
            return self.visit_IMAGE(type_)

    def visit_BINARY(self, type_):
        if type_.length:
            return "BINARY(%s)" % type_.length
        else:
            return "BINARY"

    def visit_IMAGE(self, type_):
        return "IMAGE"

    def visit_VARBINARY(self, type_):
        if type_.length:
            return "VARBINARY(%s)" % type_.length
        else:
            return "VARBINARY"

    def visit_boolean(self, type_):
        return self.visit_BIT(type_)

    def visit_BIT(self, type_):
        return "BIT"

    def visit_MONEY(self, type_):
        return "MONEY"

    def visit_SMALLMONEY(self, type_):
        return 'SMALLMONEY'

    def visit_UNIQUEIDENTIFIER(self, type_):
        return "UNIQUEIDENTIFIER"

    def visit_SQL_VARIANT(self, type_):
        return 'SQL_VARIANT'

def _has_implicit_sequence(column):
    return column.primary_key and  \
        column.autoincrement and \
        isinstance(column.type, sqltypes.Integer) and \
        not column.foreign_keys and \
        (
            column.default is None or 
            (
                isinstance(column.default, schema.Sequence) and 
                column.default.optional)
            )

def _table_sequence_column(tbl):
    if not hasattr(tbl, '_ms_has_sequence'):
        tbl._ms_has_sequence = None
        for column in tbl.c:
            if getattr(column, 'sequence', False) or _has_implicit_sequence(column):
                tbl._ms_has_sequence = column
                break
    return tbl._ms_has_sequence

class MSExecutionContext(default.DefaultExecutionContext):
    IINSERT = False
    HASIDENT = False

    def pre_exec(self):
        """Activate IDENTITY_INSERT if needed."""

        if self.compiled.isinsert:
            tbl = self.compiled.statement.table
            seq_column = _table_sequence_column(tbl)
            self.HASIDENT = bool(seq_column)
            if self.dialect.auto_identity_insert and self.HASIDENT:
                self.IINSERT = tbl._ms_has_sequence.key in self.compiled_parameters[0]
            else:
                self.IINSERT = False

            if self.IINSERT:
                self.cursor.execute("SET IDENTITY_INSERT %s ON" % 
                    self.dialect.identifier_preparer.format_table(self.compiled.statement.table))

    def handle_dbapi_exception(self, e):
        if self.IINSERT:
            try:
                self.cursor.execute("SET IDENTITY_INSERT %s OFF" % self.dialect.identifier_preparer.format_table(self.compiled.statement.table))
            except:
                pass

    def post_exec(self):
        """Disable IDENTITY_INSERT if enabled."""

        if self.compiled.isinsert and not self.executemany and self.HASIDENT and not self.IINSERT:
            if not self._last_inserted_ids or self._last_inserted_ids[0] is None:
                if self.dialect.use_scope_identity:
                    self.cursor.execute("SELECT scope_identity() AS lastrowid")
                else:
                    self.cursor.execute("SELECT @@identity AS lastrowid")
                row = self.cursor.fetchone()
                self._last_inserted_ids = [int(row[0])] + self._last_inserted_ids[1:]

        if self.IINSERT:
            self.cursor.execute("SET IDENTITY_INSERT %s OFF" % self.dialect.identifier_preparer.format_table(self.compiled.statement.table))

colspecs = {
    sqltypes.Unicode : MSNVarchar,
    sqltypes.Numeric : MSNumeric,
    sqltypes.DateTime : MSDateTime,
    sqltypes.Time : MSTime,
    sqltypes.String : MSString,
    sqltypes.Boolean : MSBoolean,
    sqltypes.Text : MSText,
    sqltypes.UnicodeText : MSNText,
    sqltypes.CHAR: MSChar,
    sqltypes.NCHAR: MSNChar,
}

ischema_names = {
    'int' : sqltypes.INTEGER,
    'bigint': sqltypes.BigInteger,
    'smallint' : sqltypes.SmallInteger,
    'tinyint' : MSTinyInteger,
    'varchar' : MSString,
    'nvarchar' : MSNVarchar,
    'char' : MSChar,
    'nchar' : MSNChar,
    'text' : MSText,
    'ntext' : MSNText,
    'decimal' : sqltypes.DECIMAL,
    'numeric' : sqltypes.NUMERIC,
    'float' : sqltypes.FLOAT,
    'datetime' : sqltypes.DATETIME,
    'datetime2' : MSDateTime2,
    'datetimeoffset' : MSDateTimeOffset,
    'date': sqltypes.DATE,
    'time': MSTime,
    'smalldatetime' : MSSmallDateTime,
    'binary' : MSBinary,
    'varbinary' : MSVarBinary,
    'bit': sqltypes.Boolean,
    'real' : MSReal,
    'image' : MSImage,
    'timestamp': sqltypes.TIMESTAMP,
    'money': MSMoney,
    'smallmoney': MSSmallMoney,
    'uniqueidentifier': MSUniqueIdentifier,
    'sql_variant': MSVariant,
}

class MSSQLCompiler(compiler.SQLCompiler):
    operators = compiler.OPERATORS.copy()
    operators.update({
        sql_operators.concat_op: '+',
        sql_operators.match_op: lambda x, y: "CONTAINS (%s, %s)" % (x, y)
    })

    functions = compiler.SQLCompiler.functions.copy()
    functions.update (
        {
            sql_functions.now: 'CURRENT_TIMESTAMP',
            sql_functions.current_date: 'GETDATE()',
            'length': lambda x: "LEN(%s)" % x,
            sql_functions.char_length: lambda x: "LEN(%s)" % x
        }
    )

    def __init__(self, *args, **kwargs):
        super(MSSQLCompiler, self).__init__(*args, **kwargs)
        self.tablealiases = {}

    def get_select_precolumns(self, select):
        """ MS-SQL puts TOP, it's version of LIMIT here """
        if select._distinct or select._limit:
            s = select._distinct and "DISTINCT " or ""
            
            if select._limit:
                if not select._offset:
                    s += "TOP %s " % (select._limit,)
                else:
                    if not self.dialect.has_window_funcs:
                        raise exc.InvalidRequestError('MSSQL does not support LIMIT with an offset')
            return s
        return compiler.SQLCompiler.get_select_precolumns(self, select)

    def limit_clause(self, select):
        # Limit in mssql is after the select keyword
        return ""

    def visit_select(self, select, **kwargs):
        """Look for ``LIMIT`` and OFFSET in a select statement, and if
        so tries to wrap it in a subquery with ``row_number()`` criterion.

        """
        if self.dialect.has_window_funcs and not getattr(select, '_mssql_visit', None) and select._offset:
            # to use ROW_NUMBER(), an ORDER BY is required.
            orderby = self.process(select._order_by_clause)
            if not orderby:
                raise exc.InvalidRequestError('MSSQL requires an order_by when using an offset.')

            _offset = select._offset
            _limit = select._limit
            select._mssql_visit = True
            select = select.column(sql.literal_column("ROW_NUMBER() OVER (ORDER BY %s)" % orderby).label("mssql_rn")).order_by(None).alias()

            limitselect = sql.select([c for c in select.c if c.key!='mssql_rn'])
            limitselect.append_whereclause("mssql_rn>%d" % _offset)
            if _limit is not None:
                limitselect.append_whereclause("mssql_rn<=%d" % (_limit + _offset))
            return self.process(limitselect, iswrapper=True, **kwargs)
        else:
            return compiler.SQLCompiler.visit_select(self, select, **kwargs)

    def _schema_aliased_table(self, table):
        if getattr(table, 'schema', None) is not None:
            if table not in self.tablealiases:
                self.tablealiases[table] = table.alias()
            return self.tablealiases[table]
        else:
            return None

    def visit_table(self, table, mssql_aliased=False, **kwargs):
        if mssql_aliased:
            return super(MSSQLCompiler, self).visit_table(table, **kwargs)

        # alias schema-qualified tables
        alias = self._schema_aliased_table(table)
        if alias is not None:
            return self.process(alias, mssql_aliased=True, **kwargs)
        else:
            return super(MSSQLCompiler, self).visit_table(table, **kwargs)

    def visit_alias(self, alias, **kwargs):
        # translate for schema-qualified table aliases
        self.tablealiases[alias.original] = alias
        kwargs['mssql_aliased'] = True
        return super(MSSQLCompiler, self).visit_alias(alias, **kwargs)

    def visit_savepoint(self, savepoint_stmt):
        util.warn("Savepoint support in mssql is experimental and may lead to data loss.")
        return "SAVE TRANSACTION %s" % self.preparer.format_savepoint(savepoint_stmt)

    def visit_rollback_to_savepoint(self, savepoint_stmt):
        return "ROLLBACK TRANSACTION %s" % self.preparer.format_savepoint(savepoint_stmt)

    def visit_column(self, column, result_map=None, **kwargs):
        if column.table is not None and \
            (not self.isupdate and not self.isdelete) or self.is_subquery():
            # translate for schema-qualified table aliases
            t = self._schema_aliased_table(column.table)
            if t is not None:
                converted = expression._corresponding_column_or_error(t, column)

                if result_map is not None:
                    result_map[column.name.lower()] = (column.name, (column, ), column.type)

                return super(MSSQLCompiler, self).visit_column(converted, result_map=None, **kwargs)

        return super(MSSQLCompiler, self).visit_column(column, result_map=result_map, **kwargs)

    def visit_binary(self, binary, **kwargs):
        """Move bind parameters to the right-hand side of an operator, where
        possible.

        """
        if isinstance(binary.left, expression._BindParamClause) and binary.operator == operator.eq \
            and not isinstance(binary.right, expression._BindParamClause):
            return self.process(expression._BinaryExpression(binary.right, binary.left, binary.operator), **kwargs)
        else:
            if (binary.operator is operator.eq or binary.operator is operator.ne) and (
                (isinstance(binary.left, expression._FromGrouping) and isinstance(binary.left.element, expression._ScalarSelect)) or \
                (isinstance(binary.right, expression._FromGrouping) and isinstance(binary.right.element, expression._ScalarSelect)) or \
                 isinstance(binary.left, expression._ScalarSelect) or isinstance(binary.right, expression._ScalarSelect)):
                op = binary.operator == operator.eq and "IN" or "NOT IN"
                return self.process(expression._BinaryExpression(binary.left, binary.right, op), **kwargs)
            return super(MSSQLCompiler, self).visit_binary(binary, **kwargs)

    def visit_insert(self, insert_stmt):
        insert_select = False
        if insert_stmt.parameters:
            insert_select = [p for p in insert_stmt.parameters.values() if isinstance(p, sql.Select)]
        if insert_select:
            self.isinsert = True
            colparams = self._get_colparams(insert_stmt)
            preparer = self.preparer

            insert = ' '.join(["INSERT"] +
                              [self.process(x) for x in insert_stmt._prefixes])

            if not colparams and not self.dialect.supports_default_values and not self.dialect.supports_empty_insert:
                raise exc.CompileError(
                    "The version of %s you are using does not support empty inserts." % self.dialect.name)
            elif not colparams and self.dialect.supports_default_values:
                return (insert + " INTO %s DEFAULT VALUES" % (
                    (preparer.format_table(insert_stmt.table),)))
            else:
                return (insert + " INTO %s (%s) SELECT %s" %
                    (preparer.format_table(insert_stmt.table),
                     ', '.join([preparer.format_column(c[0])
                               for c in colparams]),
                     ', '.join([c[1] for c in colparams])))
        else:
            return super(MSSQLCompiler, self).visit_insert(insert_stmt)

    def label_select_column(self, select, column, asfrom):
        if isinstance(column, expression.Function):
            return column.label(None)
        else:
            return super(MSSQLCompiler, self).label_select_column(select, column, asfrom)

    def for_update_clause(self, select):
        # "FOR UPDATE" is only allowed on "DECLARE CURSOR" which SQLAlchemy doesn't use
        return ''

    def order_by_clause(self, select):
        order_by = self.process(select._order_by_clause)

        # MSSQL only allows ORDER BY in subqueries if there is a LIMIT
        if order_by and (not self.is_subquery() or select._limit):
            return " ORDER BY " + order_by
        else:
            return ""


class MSDDLCompiler(compiler.DDLCompiler):
    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column) + " " + self.dialect.type_compiler.process(column.type)

        if column.nullable is not None:
            if not column.nullable or column.primary_key:
                colspec += " NOT NULL"
            else:
                colspec += " NULL"
        
        if not column.table:
            raise exc.InvalidRequestError("mssql requires Table-bound columns in order to generate DDL")
            
        seq_col = _table_sequence_column(column.table)

        # install a IDENTITY Sequence if we have an implicit IDENTITY column
        if seq_col is column:
            sequence = getattr(column, 'sequence', None)
            if sequence:
                start, increment = sequence.start or 1, sequence.increment or 1
            else:
                start, increment = 1, 1
            colspec += " IDENTITY(%s,%s)" % (start, increment)
        else:
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        return colspec

    def visit_drop_index(self, drop):
        return "\nDROP INDEX %s.%s" % (
            self.preparer.quote_identifier(drop.element.table.name),
            self.preparer.quote(self._validate_identifier(drop.element.name, False), drop.element.quote)
            )


class MSIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = compiler.IdentifierPreparer.reserved_words.union(MSSQL_RESERVED_WORDS)

    def __init__(self, dialect):
        super(MSIdentifierPreparer, self).__init__(dialect, initial_quote='[', final_quote=']')

    def _escape_identifier(self, value):
        #TODO: determine MSSQL's escaping rules
        return value

class MSDialect(default.DefaultDialect):
    name = 'mssql'
    supports_default_values = True
    supports_empty_insert = False
    auto_identity_insert = True
    execution_ctx_cls = MSExecutionContext
    text_as_varchar = False
    use_scope_identity = False
    has_window_funcs = False
    max_identifier_length = 128
    schema_name = "dbo"
    colspecs = colspecs
    ischema_names = ischema_names

    supports_unicode_binds = True

    statement_compiler = MSSQLCompiler
    ddl_compiler = MSDDLCompiler
    type_compiler = MSTypeCompiler
    preparer = MSIdentifierPreparer

    def __init__(self,
                 auto_identity_insert=True, query_timeout=None,
                 use_scope_identity=False,
                 has_window_funcs=False, max_identifier_length=None,
                 schema_name="dbo", **opts):
        self.auto_identity_insert = bool(auto_identity_insert)
        self.query_timeout = int(query_timeout or 0)
        self.schema_name = schema_name

        self.use_scope_identity = bool(use_scope_identity)
        self.has_window_funcs =  bool(has_window_funcs)
        self.max_identifier_length = int(max_identifier_length or 0) or 128
        super(MSDialect, self).__init__(**opts)

    @base.connection_memoize(('mssql', 'server_version_info'))
    def server_version_info(self, connection):
        """A tuple of the database server version.

        Formats the remote server version as a tuple of version values,
        e.g. ``(9, 0, 1399)``.  If there are strings in the version number
        they will be in the tuple too, so don't count on these all being
        ``int`` values.

        This is a fast check that does not require a round trip.  It is also
        cached per-Connection.
        """
        return connection.dialect._server_version_info(connection.connection)

    def _server_version_info(self, dbapi_con):
        """Return a tuple of the database's version number."""
        raise NotImplementedError()

    def do_begin(self, connection):
        cursor = connection.cursor()
        cursor.execute("SET IMPLICIT_TRANSACTIONS OFF")
        cursor.execute("BEGIN TRANSACTION")

    def do_release_savepoint(self, connection, name):
        pass

    @base.connection_memoize(('dialect', 'default_schema_name'))
    def get_default_schema_name(self, connection):
        query = "SELECT user_name() as user_name;"
        user_name = connection.scalar(sql.text(query))
        if user_name is not None:
            # now, get the default schema
            query = """
            SELECT default_schema_name FROM
            sys.database_principals
            WHERE name = :user_name
            AND type = 'S'
            """
            try:
                default_schema_name = connection.scalar(sql.text(query),
                                                    user_name=user_name)
                if default_schema_name is not None:
                    return default_schema_name
            except:
                pass
        return self.schema_name

    def table_names(self, connection, schema):
        from sqlalchemy.databases import information_schema as ischema
        return ischema.table_names(connection, schema)

    def uppercase_table(self, t):
        # convert all names to uppercase -- fixes refs to INFORMATION_SCHEMA for case-senstive DBs, and won't matter for case-insensitive
        t.name = t.name.upper()
        if t.schema:
            t.schema = t.schema.upper()
        for c in t.columns:
            c.name = c.name.upper()
        return t


    def has_table(self, connection, tablename, schema=None):
        import sqlalchemy.databases.information_schema as ischema

        current_schema = schema or self.get_default_schema_name(connection)
        columns = self.uppercase_table(ischema.columns)
        s = sql.select([columns],
                   current_schema
                       and sql.and_(columns.c.table_name==tablename, columns.c.table_schema==current_schema)
                       or columns.c.table_name==tablename,
                   )

        c = connection.execute(s)
        row  = c.fetchone()
        return row is not None

    def reflecttable(self, connection, table, include_columns):
        import sqlalchemy.databases.information_schema as ischema
        # Get base columns
        if table.schema is not None:
            current_schema = table.schema
        else:
            current_schema = self.get_default_schema_name(connection)

        columns = self.uppercase_table(ischema.columns)
        s = sql.select([columns],
                   current_schema
                       and sql.and_(columns.c.table_name==table.name, columns.c.table_schema==current_schema)
                       or columns.c.table_name==table.name,
                   order_by=[columns.c.ordinal_position])

        c = connection.execute(s)
        found_table = False
        while True:
            row = c.fetchone()
            if row is None:
                break
            found_table = True
            (name, type, nullable, charlen, numericprec, numericscale, default, collation) = (
                row[columns.c.column_name],
                row[columns.c.data_type],
                row[columns.c.is_nullable] == 'YES',
                row[columns.c.character_maximum_length],
                row[columns.c.numeric_precision],
                row[columns.c.numeric_scale],
                row[columns.c.column_default],
                row[columns.c.collation_name]
            )
            if include_columns and name not in include_columns:
                continue

            args = []
            for a in (charlen, numericprec, numericscale):
                if a is not None:
                    args.append(a)
            coltype = self.ischema_names.get(type, None)

            kwargs = {}
            if coltype in (MSString, MSChar, MSNVarchar, MSNChar, MSText, MSNText):
                if collation:
                    kwargs.update(collation=collation)

            if coltype == MSText or (coltype == MSString and charlen == -1):
                coltype = MSText(**kwargs)
            else:
                if coltype is None:
                    util.warn("Did not recognize type '%s' of column '%s'" %
                              (type, name))
                    coltype = sqltypes.NULLTYPE

                elif coltype in (MSNVarchar,) and charlen == -1:
                    args[0] = None
                coltype = coltype(*args, **kwargs)
            colargs = []
            if default is not None:
                colargs.append(schema.DefaultClause(sql.text(default)))

            table.append_column(schema.Column(name, coltype, nullable=nullable, autoincrement=False, *colargs))

        if not found_table:
            raise exc.NoSuchTableError(table.name)

        # We also run an sp_columns to check for identity columns:
        cursor = connection.execute("sp_columns @table_name = '%s', @table_owner = '%s'" % (table.name, current_schema))
        ic = None
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            col_name, type_name = row[3], row[5]
            if type_name.endswith("identity") and col_name in table.c:
                ic = table.c[col_name]
                ic.autoincrement = True
                # setup a psuedo-sequence to represent the identity attribute - we interpret this at table.create() time as the identity attribute
                ic.sequence = schema.Sequence(ic.name + '_identity', 1, 1)
                # MSSQL: only one identity per table allowed
                cursor.close()
                break
        if not ic is None:
            try:
                cursor = connection.execute("select ident_seed(?), ident_incr(?)", table.fullname, table.fullname)
                row = cursor.fetchone()
                cursor.close()
                if not row is None:
                    ic.sequence.start = int(row[0])
                    ic.sequence.increment = int(row[1])
            except:
                # ignoring it, works just like before
                pass

        # Add constraints
        RR = self.uppercase_table(ischema.ref_constraints)    #information_schema.referential_constraints
        TC = self.uppercase_table(ischema.constraints)        #information_schema.table_constraints
        C  = self.uppercase_table(ischema.pg_key_constraints).alias('C') #information_schema.constraint_column_usage: the constrained column
        R  = self.uppercase_table(ischema.pg_key_constraints).alias('R') #information_schema.constraint_column_usage: the referenced column

        # Primary key constraints
        s = sql.select([C.c.column_name, TC.c.constraint_type], sql.and_(TC.c.constraint_name == C.c.constraint_name,
                                                                         C.c.table_name == table.name,
                                                                         C.c.table_schema == (table.schema or current_schema)))
        c = connection.execute(s)
        for row in c:
            if 'PRIMARY' in row[TC.c.constraint_type.name] and row[0] in table.c:
                table.primary_key.add(table.c[row[0]])

        # Foreign key constraints
        s = sql.select([C.c.column_name,
                        R.c.table_schema, R.c.table_name, R.c.column_name,
                        RR.c.constraint_name, RR.c.match_option, RR.c.update_rule, RR.c.delete_rule],
                       sql.and_(C.c.table_name == table.name,
                                C.c.table_schema == (table.schema or current_schema),
                                C.c.constraint_name == RR.c.constraint_name,
                                R.c.constraint_name == RR.c.unique_constraint_name,
                                C.c.ordinal_position == R.c.ordinal_position
                                ),
                       order_by = [RR.c.constraint_name, R.c.ordinal_position])
        rows = connection.execute(s).fetchall()

        def _gen_fkref(table, rschema, rtbl, rcol):
            if rschema == current_schema and not table.schema:
                return '.'.join([rtbl, rcol])
            else:
                return '.'.join([rschema, rtbl, rcol])

        # group rows by constraint ID, to handle multi-column FKs
        fknm, scols, rcols = (None, [], [])
        for r in rows:
            scol, rschema, rtbl, rcol, rfknm, fkmatch, fkuprule, fkdelrule = r
            # if the reflected schema is the default schema then don't set it because this will
            # play into the metadata key causing duplicates.
            if rschema == current_schema and not table.schema:
                schema.Table(rtbl, table.metadata, autoload=True, autoload_with=connection)
            else:
                schema.Table(rtbl, table.metadata, schema=rschema, autoload=True, autoload_with=connection)
            if rfknm != fknm:
                if fknm:
                    table.append_constraint(schema.ForeignKeyConstraint(scols, [_gen_fkref(table, s, t, c) for s, t, c in rcols], fknm, link_to_name=True))
                fknm, scols, rcols = (rfknm, [], [])
            if not scol in scols:
                scols.append(scol)
            if not (rschema, rtbl, rcol) in rcols:
                rcols.append((rschema, rtbl, rcol))

        if fknm and scols:
            table.append_constraint(schema.ForeignKeyConstraint(scols, [_gen_fkref(table, s, t, c) for s, t, c in rcols], fknm, link_to_name=True))

