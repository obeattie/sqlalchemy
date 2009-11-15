"""Support for the PostgreSQL database via the psycopg2 driver.

Driver
------

The psycopg2 driver is supported, available at http://pypi.python.org/pypi/psycopg2/ .
The dialect has several behaviors  which are specifically tailored towards compatibility 
with this module.

Note that psycopg1 is **not** supported.

Connecting
----------

URLs are of the form `postgresql+psycopg2://user@password@host:port/dbname[?key=value&key=value...]`.

psycopg2-specific keyword arguments which are accepted by :func:`~sqlalchemy.create_engine()` are:

* *server_side_cursors* - Enable the usage of "server side cursors" for SQL statements which support
  this feature.  What this essentially means from a psycopg2 point of view is that the cursor is 
  created using a name, e.g. `connection.cursor('some name')`, which has the effect that result rows
  are not immediately pre-fetched and buffered after statement execution, but are instead left 
  on the server and only retrieved as needed.    SQLAlchemy's :class:`~sqlalchemy.engine.base.ResultProxy`
  uses special row-buffering behavior when this feature is enabled, such that groups of 100 rows 
  at a time are fetched over the wire to reduce conversational overhead.
* *use_native_unicode* - Enable the usage of Psycopg2 "native unicode" mode per connection.  True  
  by default.
* *isolation_level* - Sets the transaction isolation level for each transaction
  within the engine. Valid isolation levels are `READ_COMMITTED`,
  `READ_UNCOMMITTED`, `REPEATABLE_READ`, and `SERIALIZABLE`.

Transactions
------------

The psycopg2 dialect fully supports SAVEPOINT and two-phase commit operations.


"""

import decimal, random, re
from sqlalchemy import util
from sqlalchemy.engine import base, default
from sqlalchemy.sql import expression
from sqlalchemy.sql import operators as sql_operators
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects.postgresql.base import PGDialect, PGCompiler, \
                                            PGIdentifierPreparer, PGExecutionContext, \
                                            ENUM, ARRAY

class _PGNumeric(sqltypes.Numeric):
    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect, coltype):
        if self.asdecimal:
            if coltype in (700, 701):
                def process(value):
                    if value is not None:
                        return decimal.Decimal(str(value))
                    else:
                        return value
                return process
            elif coltype == 1700:
                # pg8000 returns Decimal natively for 1700
                return None
            else:
                raise exc.InvalidRequestError("Unknown PG numeric type: %d" % coltype)
        else:
            if coltype in (700, 701):
                # pg8000 returns float natively for 701
                return None
            elif coltype == 1700:
                def process(value):
                    if value is not None:
                        return float(value)
                    else:
                        return value
                return process
            else:
                raise exc.InvalidRequestError("Unknown PG numeric type: %d" % coltype)

class _PGEnum(ENUM):
    def __init__(self, *arg, **kw):
        super(_PGEnum, self).__init__(*arg, **kw)
        if self.convert_unicode:
            self.convert_unicode = "force"

class _PGArray(ARRAY):
    def __init__(self, *arg, **kw):
        super(_PGArray, self).__init__(*arg, **kw)
        # FIXME: this check won't work for setups that
        # have convert_unicode only on their create_engine().
        if isinstance(self.item_type, sqltypes.String) and \
                    self.item_type.convert_unicode:
            self.item_type.convert_unicode = "force"
    
# TODO: filter out 'FOR UPDATE' statements
SERVER_SIDE_CURSOR_RE = re.compile(
    r'\s*SELECT',
    re.I | re.UNICODE)

class PostgreSQL_psycopg2ExecutionContext(PGExecutionContext):
    def create_cursor(self):
        # TODO: coverage for server side cursors + select.for_update()
        is_server_side = \
            self.dialect.server_side_cursors and \
            not self.should_autocommit and \
            ((self.compiled and isinstance(self.compiled.statement, expression.Selectable) 
                and not getattr(self.compiled.statement, 'for_update', False)) \
            or \
            (
                (not self.compiled or isinstance(self.compiled.statement, expression._TextClause)) 
                and self.statement and SERVER_SIDE_CURSOR_RE.match(self.statement))
            )

        self.__is_server_side = is_server_side
        if is_server_side:
            # use server-side cursors:
            # http://lists.initd.org/pipermail/psycopg/2007-January/005251.html
            ident = "c_%s_%s" % (hex(id(self))[2:], hex(random.randint(0, 65535))[2:])
            return self._connection.connection.cursor(ident)
        else:
            return self._connection.connection.cursor()

    def get_result_proxy(self):
        if self.__is_server_side:
            return base.BufferedRowResultProxy(self)
        else:
            return base.ResultProxy(self)


class PostgreSQL_psycopg2Compiler(PGCompiler):
    def visit_mod(self, binary, **kw):
        return self.process(binary.left) + " %% " + self.process(binary.right)
    
    def post_process_text(self, text):
        return text.replace('%', '%%')


class PostgreSQL_psycopg2IdentifierPreparer(PGIdentifierPreparer):
    def _escape_identifier(self, value):
        value = value.replace(self.escape_quote, self.escape_to_quote)
        return value.replace('%', '%%')

class PostgreSQL_psycopg2(PGDialect):
    driver = 'psycopg2'
    supports_unicode_statements = False
    default_paramstyle = 'pyformat'
    supports_sane_multi_rowcount = False
    execution_ctx_cls = PostgreSQL_psycopg2ExecutionContext
    statement_compiler = PostgreSQL_psycopg2Compiler
    preparer = PostgreSQL_psycopg2IdentifierPreparer

    colspecs = util.update_copy(
        PGDialect.colspecs,
        {
            sqltypes.Numeric : _PGNumeric,
            ENUM : _PGEnum, # needs force_unicode
            sqltypes.Enum : _PGEnum, # needs force_unicode
            ARRAY : _PGArray, # needs force_unicode
        }
    )

    def __init__(self, server_side_cursors=False, use_native_unicode=True, **kwargs):
        PGDialect.__init__(self, **kwargs)
        self.server_side_cursors = server_side_cursors
        self.use_native_unicode = use_native_unicode
        self.supports_unicode_binds = use_native_unicode
        
    @classmethod
    def dbapi(cls):
        psycopg = __import__('psycopg2')
        return psycopg
    
    _unwrap_connection = None
    
    def visit_pool(self, pool):
        if self.dbapi and self.use_native_unicode:
            extensions = __import__('psycopg2.extensions').extensions
            def connect(conn, rec):
                if self._unwrap_connection:
                    conn = self._unwrap_connection(conn)
                    if conn is None:
                        return
                extensions.register_type(extensions.UNICODE, conn)
            pool.add_listener({'first_connect': connect, 'connect':connect})
        super(PostgreSQL_psycopg2, self).visit_pool(pool)
        
    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        if 'port' in opts:
            opts['port'] = int(opts['port'])
        opts.update(url.query)
        return ([], opts)

    def is_disconnect(self, e):
        if isinstance(e, self.dbapi.OperationalError):
            return 'closed the connection' in str(e) or 'connection not open' in str(e)
        elif isinstance(e, self.dbapi.InterfaceError):
            return 'connection already closed' in str(e) or 'cursor already closed' in str(e)
        elif isinstance(e, self.dbapi.ProgrammingError):
            # yes, it really says "losed", not "closed"
            return "losed the connection unexpectedly" in str(e)
        else:
            return False

dialect = PostgreSQL_psycopg2
    
