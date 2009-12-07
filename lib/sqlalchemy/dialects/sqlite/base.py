# sqlite.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
"""Support for the SQLite database.

For information on connecting using a specific driver, see the documentation
section regarding that driver.

Date and Time Types
-------------------

SQLite does not have built-in DATE, TIME, or DATETIME types, and pysqlite does not provide 
out of the box functionality for translating values between Python `datetime` objects
and a SQLite-supported format.  SQLAlchemy's own :class:`~sqlalchemy.types.DateTime`
and related types provide date formatting and parsing functionality when SQlite is used.
The implementation classes are :class:`DATETIME`, :class:`DATE` and :class:`TIME`.
These types represent dates and times as ISO formatted strings, which also nicely
support ordering.   There's no reliance on typical "libc" internals for these functions
so historical dates are fully supported.


"""

import datetime, re, time

from sqlalchemy import schema as sa_schema
from sqlalchemy import sql, exc, pool, DefaultClause
from sqlalchemy.engine import default
from sqlalchemy.engine import reflection
from sqlalchemy import types as sqltypes
from sqlalchemy import util
from sqlalchemy.sql import compiler, functions as sql_functions
from sqlalchemy.util import NoneType

from sqlalchemy.types import BLOB, BOOLEAN, CHAR, DATE, DATETIME, DECIMAL,\
                            FLOAT, INTEGER, NUMERIC, SMALLINT, TEXT, TIME,\
                            TIMESTAMP, VARCHAR
                            

class _NumericMixin(object):
    def bind_processor(self, dialect):
        type_ = self.asdecimal and str or float
        def process(value):
            if value is not None:
                return type_(value)
            else:
                return value
        return process

class _SLNumeric(_NumericMixin, sqltypes.Numeric):
    pass

class _SLFloat(_NumericMixin, sqltypes.Float):
    pass

# since SQLite has no date types, we're assuming that SQLite via ODBC
# or JDBC would similarly have no built in date support, so the "string" based logic
# would apply to all implementing dialects.
class _DateTimeMixin(object):
    _reg = None
    _storage_format = None

    def __init__(self, storage_format=None, regexp=None, **kwargs):
        if regexp is not None:
            self._reg = re.compile(regexp)
        if storage_format is not None:
            self._storage_format = storage_format

    def _result_processor(self, fn):
        rmatch = self._reg.match
        # Even on python2.6 datetime.strptime is both slower than this code
        # and it does not support microseconds.
        def process(value):
            if value is not None:
                return fn(*map(int, rmatch(value).groups(0)))
            else:
                return None
        return process

class DATETIME(_DateTimeMixin, sqltypes.DateTime):
    _reg = re.compile(r"(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)(?:\.(\d+))?")
    _storage_format = "%04d-%02d-%02d %02d:%02d:%02d.%06d"
  
    def bind_processor(self, dialect):
        datetime_datetime = datetime.datetime
        datetime_date = datetime.date
        format = self._storage_format
        def process(value):
            if value is None:
                return None
            elif isinstance(value, datetime_datetime):
                return format % (value.year, value.month, value.day,
                                 value.hour, value.minute, value.second,
                                 value.microsecond)
            elif isinstance(value, datetime_date):
                return format % (value.year, value.month, value.day,
                                 0, 0, 0, 0)
            else:
                raise TypeError("SQLite DateTime type only accepts Python "
                                "datetime and date objects as input.")
        return process

    def result_processor(self, dialect, coltype):
        return self._result_processor(datetime.datetime)

class DATE(_DateTimeMixin, sqltypes.Date):
    _reg = re.compile(r"(\d+)-(\d+)-(\d+)")
    _storage_format = "%04d-%02d-%02d"

    def bind_processor(self, dialect):
        datetime_date = datetime.date
        format = self._storage_format
        def process(value):
            if value is None:
                return None
            elif isinstance(value, datetime_date):
                return format % (value.year, value.month, value.day)
            else:
                raise TypeError("SQLite Date type only accepts Python "
                                "date objects as input.")
        return process
  
    def result_processor(self, dialect, coltype):
        return self._result_processor(datetime.date)

class TIME(_DateTimeMixin, sqltypes.Time):
    _reg = re.compile(r"(\d+):(\d+):(\d+)(?:\.(\d+))?")
    _storage_format = "%02d:%02d:%02d.%06d"

    def bind_processor(self, dialect):
        datetime_time = datetime.time
        format = self._storage_format
        def process(value):
            if value is None:
                return None
            elif isinstance(value, datetime_time):
                return format % (value.hour, value.minute, value.second,
                                 value.microsecond)
            else:
                raise TypeError("SQLite Time type only accepts Python "
                                "time objects as input.")
        return process
  
    def result_processor(self, dialect, coltype):
        return self._result_processor(datetime.time)

colspecs = {
    sqltypes.Date: DATE,
    sqltypes.DateTime: DATETIME,
    sqltypes.Float: _SLFloat,
    sqltypes.Numeric: _SLNumeric,
    sqltypes.Time: TIME,
}

ischema_names = {
    'BLOB': sqltypes.BLOB,
    'BOOL': sqltypes.BOOLEAN,
    'BOOLEAN': sqltypes.BOOLEAN,
    'CHAR': sqltypes.CHAR,
    'DATE': sqltypes.DATE,
    'DATETIME': sqltypes.DATETIME,
    'DECIMAL': sqltypes.DECIMAL,
    'FLOAT': sqltypes.FLOAT,
    'INT': sqltypes.INTEGER,
    'INTEGER': sqltypes.INTEGER,
    'NUMERIC': sqltypes.NUMERIC,
    'REAL': sqltypes.Numeric,
    'SMALLINT': sqltypes.SMALLINT,
    'TEXT': sqltypes.TEXT,
    'TIME': sqltypes.TIME,
    'TIMESTAMP': sqltypes.TIMESTAMP,
    'VARCHAR': sqltypes.VARCHAR,
}



class SQLiteCompiler(compiler.SQLCompiler):
    extract_map = compiler.SQLCompiler.extract_map.copy()
    extract_map.update({
        'month': '%m',
        'day': '%d',
        'year': '%Y',
        'second': '%S',
        'hour': '%H',
        'doy': '%j',
        'minute': '%M',
        'epoch': '%s',
        'dow': '%w',
        'week': '%W'
    })

    def visit_now_func(self, fn, **kw):
        return "CURRENT_TIMESTAMP"
    
    def visit_char_length_func(self, fn, **kw):
        return "length%s" % self.function_argspec(fn)
        
    def visit_cast(self, cast, **kwargs):
        if self.dialect.supports_cast:
            return super(SQLiteCompiler, self).visit_cast(cast)
        else:
            return self.process(cast.clause)

    def visit_extract(self, extract):
        try:
            return "CAST(STRFTIME('%s', %s) AS INTEGER)" % (
                self.extract_map[extract.field], self.process(extract.expr))
        except KeyError:
            raise exc.ArgumentError(
                "%s is not a valid extract argument." % extract.field)

    def limit_clause(self, select):
        text = ""
        if select._limit is not None:
            text +=  " \n LIMIT " + str(select._limit)
        if select._offset is not None:
            if select._limit is None:
                text += " \n LIMIT -1"
            text += " OFFSET " + str(select._offset)
        else:
            text += " OFFSET 0"
        return text

    def for_update_clause(self, select):
        # sqlite has no "FOR UPDATE" AFAICT
        return ''


class SQLiteDDLCompiler(compiler.DDLCompiler):

    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column) + " " + self.dialect.type_compiler.process(column.type)
        default = self.get_column_default_string(column)
        if default is not None:
            colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"
        return colspec

    def visit_create_index(self, create):
        index = create.element
        preparer = self.preparer
        text = "CREATE "
        if index.unique:
            text += "UNIQUE "
        text += "INDEX %s ON %s (%s)" \
                    % (preparer.format_index(index,
                       name=self._validate_identifier(index.name, True)),
                       preparer.format_table(index.table, use_schema=False),
                       ', '.join(preparer.quote(c.name, c.quote)
                                 for c in index.columns))
        return text

class SQLiteTypeCompiler(compiler.GenericTypeCompiler):
    def visit_binary(self, type_):
        return self.visit_BLOB(type_)

class SQLiteIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = set([
        'add', 'after', 'all', 'alter', 'analyze', 'and', 'as', 'asc',
        'attach', 'autoincrement', 'before', 'begin', 'between', 'by',
        'cascade', 'case', 'cast', 'check', 'collate', 'column', 'commit',
        'conflict', 'constraint', 'create', 'cross', 'current_date',
        'current_time', 'current_timestamp', 'database', 'default',
        'deferrable', 'deferred', 'delete', 'desc', 'detach', 'distinct',
        'drop', 'each', 'else', 'end', 'escape', 'except', 'exclusive',
        'explain', 'false', 'fail', 'for', 'foreign', 'from', 'full', 'glob',
        'group', 'having', 'if', 'ignore', 'immediate', 'in', 'index',
        'indexed', 'initially', 'inner', 'insert', 'instead', 'intersect', 'into', 'is',
        'isnull', 'join', 'key', 'left', 'like', 'limit', 'match', 'natural',
        'not', 'notnull', 'null', 'of', 'offset', 'on', 'or', 'order', 'outer',
        'plan', 'pragma', 'primary', 'query', 'raise', 'references',
        'reindex', 'rename', 'replace', 'restrict', 'right', 'rollback',
        'row', 'select', 'set', 'table', 'temp', 'temporary', 'then', 'to',
        'transaction', 'trigger', 'true', 'union', 'unique', 'update', 'using',
        'vacuum', 'values', 'view', 'virtual', 'when', 'where',
        ])

    def format_index(self, index, use_schema=True, name=None):
        """Prepare a quoted index and schema name."""

        if name is None:
            name = index.name
        result = self.quote(name, index.quote)
        if not self.omit_schema and use_schema and getattr(index.table, "schema", None):
            result = self.quote_schema(index.table.schema, index.table.quote_schema) + "." + result
        return result

class SQLiteDialect(default.DefaultDialect):
    name = 'sqlite'
    supports_alter = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    supports_default_values = True
    supports_empty_insert = False
    supports_cast = True

    default_paramstyle = 'qmark'
    statement_compiler = SQLiteCompiler
    ddl_compiler = SQLiteDDLCompiler
    type_compiler = SQLiteTypeCompiler
    preparer = SQLiteIdentifierPreparer
    ischema_names = ischema_names
    colspecs = colspecs
    isolation_level = None

    def __init__(self, isolation_level=None, **kwargs):
        default.DefaultDialect.__init__(self, **kwargs)
        if isolation_level and isolation_level not in ('SERIALIZABLE',
                'READ UNCOMMITTED'):
            raise exc.ArgumentError("Invalid value for isolation_level. "
                "Valid isolation levels for sqlite are 'SERIALIZABLE' and "
                "'READ UNCOMMITTED'.")
        self.isolation_level = isolation_level

    def visit_pool(self, pool):
        if self.isolation_level is not None:
            class SetIsolationLevel(object):
                def __init__(self, isolation_level):
                    if isolation_level == 'READ UNCOMMITTED':
                        self.isolation_level = 1
                    else:
                        self.isolation_level = 0

                def connect(self, conn, rec):
                    cursor = conn.cursor()
                    cursor.execute("PRAGMA read_uncommitted = %d" % self.isolation_level)
                    cursor.close()
            pool.add_listener(SetIsolationLevel(self.isolation_level))

    def table_names(self, connection, schema):
        if schema is not None:
            qschema = self.identifier_preparer.quote_identifier(schema)
            master = '%s.sqlite_master' % qschema
            s = ("SELECT name FROM %s "
                 "WHERE type='table' ORDER BY name") % (master,)
            rs = connection.execute(s)
        else:
            try:
                s = ("SELECT name FROM "
                     " (SELECT * FROM sqlite_master UNION ALL "
                     "  SELECT * FROM sqlite_temp_master) "
                     "WHERE type='table' ORDER BY name")
                rs = connection.execute(s)
            except exc.DBAPIError:
                raise
                s = ("SELECT name FROM sqlite_master "
                     "WHERE type='table' ORDER BY name")
                rs = connection.execute(s)

        return [row[0] for row in rs]

    def has_table(self, connection, table_name, schema=None):
        quote = self.identifier_preparer.quote_identifier
        if schema is not None:
            pragma = "PRAGMA %s." % quote(schema)
        else:
            pragma = "PRAGMA "
        qtable = quote(table_name)
        cursor = _pragma_cursor(connection.execute("%stable_info(%s)" % (pragma, qtable)))
        row = cursor.fetchone()

        # consume remaining rows, to work around
        # http://www.sqlite.org/cvstrac/tktview?tn=1884
        while cursor.fetchone() is not None:
            pass

        return (row is not None)

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        return self.table_names(connection, schema)

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        if schema is not None:
            qschema = self.identifier_preparer.quote_identifier(schema)
            master = '%s.sqlite_master' % qschema
            s = ("SELECT name FROM %s "
                 "WHERE type='view' ORDER BY name") % (master,)
            rs = connection.execute(s)
        else:
            try:
                s = ("SELECT name FROM "
                     " (SELECT * FROM sqlite_master UNION ALL "
                     "  SELECT * FROM sqlite_temp_master) "
                     "WHERE type='view' ORDER BY name")
                rs = connection.execute(s)
            except exc.DBAPIError:
                raise
                s = ("SELECT name FROM sqlite_master "
                     "WHERE type='view' ORDER BY name")
                rs = connection.execute(s)

        return [row[0] for row in rs]

    @reflection.cache
    def get_view_definition(self, connection, view_name, schema=None, **kw):
        quote = self.identifier_preparer.quote_identifier
        if schema is not None:
            qschema = self.identifier_preparer.quote_identifier(schema)
            master = '%s.sqlite_master' % qschema
            s = ("SELECT sql FROM %s WHERE name = '%s'"
                 "AND type='view'") % (master, view_name)
            rs = connection.execute(s)
        else:
            try:
                s = ("SELECT sql FROM "
                     " (SELECT * FROM sqlite_master UNION ALL "
                     "  SELECT * FROM sqlite_temp_master) "
                     "WHERE name = '%s' "
                     "AND type='view'") % view_name
                rs = connection.execute(s)
            except exc.DBAPIError:
                raise
                s = ("SELECT sql FROM sqlite_master WHERE name = '%s' "
                     "AND type='view'") % view_name
                rs = connection.execute(s)

        result = rs.fetchall()
        if result:
            return result[0].sql

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        quote = self.identifier_preparer.quote_identifier
        if schema is not None:
            pragma = "PRAGMA %s." % quote(schema)
        else:
            pragma = "PRAGMA "
        qtable = quote(table_name)
        c = _pragma_cursor(connection.execute("%stable_info(%s)" % (pragma, qtable)))
        found_table = False
        columns = []
        while True:
            row = c.fetchone()
            if row is None:
                break
            (name, type_, nullable, default, has_default, primary_key) = (row[1], row[2].upper(), not row[3], row[4], row[4] is not None, row[5])
            name = re.sub(r'^\"|\"$', '', name)
            if default:
                default = re.sub(r"^\'|\'$", '', default)
            match = re.match(r'(\w+)(\(.*?\))?', type_)
            if match:
                coltype = match.group(1)
                args = match.group(2)
            else:
                coltype = "VARCHAR"
                args = ''
            try:
                coltype = self.ischema_names[coltype]
            except KeyError:
                util.warn("Did not recognize type '%s' of column '%s'" %
                          (coltype, name))
                coltype = sqltypes.NullType
            if args is not None:
                args = re.findall(r'(\d+)', args)
                coltype = coltype(*[int(a) for a in args])

            columns.append({
                'name' : name,
                'type' : coltype,
                'nullable' : nullable,
                'default' : default,
                'primary_key': primary_key
            })
        return columns

    @reflection.cache
    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        cols = self.get_columns(connection, table_name, schema, **kw)
        pkeys = []
        for col in cols:
            if col['primary_key']:
                pkeys.append(col['name'])
        return pkeys

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        quote = self.identifier_preparer.quote_identifier
        if schema is not None:
            pragma = "PRAGMA %s." % quote(schema)
        else:
            pragma = "PRAGMA "
        qtable = quote(table_name)
        c = _pragma_cursor(connection.execute("%sforeign_key_list(%s)" % (pragma, qtable)))
        fkeys = []
        fks = {}
        while True:
            row = c.fetchone()
            if row is None:
                break
            (constraint_name, rtbl, lcol, rcol) = (row[0], row[2], row[3], row[4])
            rtbl = re.sub(r'^\"|\"$', '', rtbl)
            lcol = re.sub(r'^\"|\"$', '', lcol)
            rcol = re.sub(r'^\"|\"$', '', rcol)
            try:
                fk = fks[constraint_name]
            except KeyError:
                fk = {
                    'name' : constraint_name,
                    'constrained_columns' : [],
                    'referred_schema' : None,
                    'referred_table' : rtbl,
                    'referred_columns' : []
                }
                fkeys.append(fk)
                fks[constraint_name] = fk

            # look up the table based on the given table's engine, not 'self',
            # since it could be a ProxyEngine
            if lcol not in fk['constrained_columns']:
                fk['constrained_columns'].append(lcol)
            if rcol not in fk['referred_columns']:
                fk['referred_columns'].append(rcol)
        return fkeys

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        quote = self.identifier_preparer.quote_identifier
        if schema is not None:
            pragma = "PRAGMA %s." % quote(schema)
        else:
            pragma = "PRAGMA "
        include_auto_indexes = kw.pop('include_auto_indexes', False)
        qtable = quote(table_name)
        c = _pragma_cursor(connection.execute("%sindex_list(%s)" % (pragma, qtable)))
        indexes = []
        while True:
            row = c.fetchone()
            if row is None:
                break
            # ignore implicit primary key index.
            # http://www.mail-archive.com/sqlite-users@sqlite.org/msg30517.html
            elif not include_auto_indexes and row[1].startswith('sqlite_autoindex'):
                continue

            indexes.append(dict(name=row[1], column_names=[], unique=row[2]))
        # loop thru unique indexes to get the column names.
        for idx in indexes:
            c = connection.execute("%sindex_info(%s)" % (pragma, quote(idx['name'])))
            cols = idx['column_names']
            while True:
                row = c.fetchone()
                if row is None:
                    break
                cols.append(row[2])
        return indexes


def _pragma_cursor(cursor):
    """work around SQLite issue whereby cursor.description is blank when PRAGMA returns no rows."""
    
    if cursor.closed:
        cursor._fetchone_impl = lambda: None
    return cursor
