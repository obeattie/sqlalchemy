"""Support for the MySQL database via the oursql adapter.

Character Sets
--------------

oursql defaults to using ``utf8`` as the connection charset, but other 
encodings may be used instead. Like the MySQL-Python driver, unicode support 
can be completely disabled::

  # oursql sets the connection charset to utf8 automatically; all strings come 
  # back as utf8 str
  create_engine('mysql+oursql:///mydb?use_unicode=0')

To not automatically use ``utf8`` and instead use whatever the connection 
defaults to, there is a separate parameter::

  # use the default connection charset; all strings come back as unicode
  create_engine('mysql+oursql:///mydb?default_charset=1')
  
  # use latin1 as the connection charset; all strings come back as unicode
  create_engine('mysql+oursql:///mydb?charset=latin1')
"""

import decimal
import re

from sqlalchemy.dialects.mysql.base import (BIT, MySQLDialect, MySQLExecutionContext,
                                            MySQLCompiler, MySQLIdentifierPreparer, NUMERIC, _NumericType)
from sqlalchemy.engine import base as engine_base, default
from sqlalchemy.sql import operators as sql_operators
from sqlalchemy import exc, log, schema, sql, types as sqltypes, util


class _PlainQuery(unicode): 
    pass


class _oursqlNumeric(NUMERIC):
    def result_processor(self, dialect, coltype):
        if self.asdecimal:
            return None
        def process(value):
            if value is not None:
                return float(value)
            else:
                return value
        return process


class _oursqlBIT(BIT):
    def result_processor(self, dialect, coltype):
        """oursql already converts mysql bits, so."""

        return None

class MySQL_oursql(MySQLDialect):
    driver = 'oursql'
    supports_unicode_statements = True
    supports_unicode_binds = True
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True
    
    colspecs = util.update_copy(
        MySQLDialect.colspecs,
        {
            sqltypes.Time: sqltypes.Time,
            sqltypes.Numeric: _oursqlNumeric,
            BIT: _oursqlBIT,
        }
    )
    
    @classmethod
    def dbapi(cls):
        return __import__('oursql')

    def do_execute(self, cursor, statement, parameters, context=None):
        """Provide an implementation of *cursor.execute(statement, parameters)*."""
        
        if context and not context.compiled and isinstance(context.statement, _PlainQuery):
            cursor.execute(statement, plain_query=True)
        else:
            cursor.execute(statement, parameters)

    def do_begin(self, connection):
        connection.cursor().execute('BEGIN', plain_query=True)

    def _xa_query(self, connection, query, xid):
        connection.execute(_PlainQuery(query % connection.connection._escape_string(xid)))

    # Because mysql is bad, these methods have to be reimplemented to use _PlainQuery. Basically, some queries
    # refuse to return any data if they're run through the parameterized query API, or refuse to be parameterized
    # in the first place.
    def do_begin_twophase(self, connection, xid):
        self._xa_query(connection, 'XA BEGIN "%s"', xid)

    def do_prepare_twophase(self, connection, xid):
        self._xa_query(connection, 'XA END "%s"', xid)
        self._xa_query(connection, 'XA PREPARE "%s"', xid)

    def do_rollback_twophase(self, connection, xid, is_prepared=True,
                             recover=False):
        if not is_prepared:
            self._xa_query(connection, 'XA END "%s"', xid)
        self._xa_query(connection, 'XA ROLLBACK "%s"', xid)

    def do_commit_twophase(self, connection, xid, is_prepared=True,
                           recover=False):
        if not is_prepared:
            self.do_prepare_twophase(connection, xid)
        self._xa_query(connection, 'XA COMMIT "%s"', xid)

    def has_table(self, connection, table_name, schema=None):
        full_name = '.'.join(self.identifier_preparer._quote_free_identifiers(
            schema, table_name))

        st = "DESCRIBE %s" % full_name
        rs = None
        try:
            try:
                rs = connection.execute(_PlainQuery(st))
                have = rs.rowcount > 0
                rs.close()
                return have
            except exc.SQLError, e:
                if self._extract_error_code(e) == 1146:
                    return False
                raise
        finally:
            if rs:
                rs.close()

    def _show_create_table(self, connection, table, charset=None,
                           full_name=None):
        """Run SHOW CREATE TABLE for a ``Table``."""

        if full_name is None:
            full_name = self.identifier_preparer.format_table(table)
        st = "SHOW CREATE TABLE %s" % full_name

        rp = None
        try:
            try:
                rp = connection.execute(_PlainQuery(st))
            except exc.SQLError, e:
                if self._extract_error_code(e) == 1146:
                    raise exc.NoSuchTableError(full_name)
                else:
                    raise
            row = rp.fetchone()
            if not row:
                raise exc.NoSuchTableError(full_name)
            return row[1].strip()
        finally:
            if rp:
                rp.close()

    def is_disconnect(self, e):
        if isinstance(e, self.dbapi.ProgrammingError):  # if underlying connection is closed, this is the error you get
            return e.errno is None and e[1].endswith('closed')
        else:
            return e.errno in (2006, 2013, 2014, 2045, 2055)

    def create_connect_args(self, url):
        opts = url.translate_connect_args(database='db', username='user',
                                          password='passwd')
        opts.update(url.query)

        util.coerce_kw_type(opts, 'port', int)
        util.coerce_kw_type(opts, 'compress', bool)
        util.coerce_kw_type(opts, 'autoping', bool)

        util.coerce_kw_type(opts, 'default_charset', bool)
        if opts.pop('default_charset', False):
            opts['charset'] = None
        else:
            util.coerce_kw_type(opts, 'charset', str)
        util.coerce_kw_type(opts, 'use_unicode', bool)

        # FOUND_ROWS must be set in CLIENT_FLAGS to enable
        # supports_sane_rowcount.
        opts['found_rows'] = True
        # And sqlalchemy assumes that you get an exception when mysql reports a warning.
        opts['raise_on_warnings'] = True
        return [[], opts]
    
    def _get_server_version_info(self, connection):
        dbapi_con = connection.connection
        version = []
        r = re.compile('[.\-]')
        for n in r.split(dbapi_con.server_info):
            try:
                version.append(int(n))
            except ValueError:
                version.append(n)
        return tuple(version)

    def _extract_error_code(self, exception):
        try:
            return exception.orig.errno
        except AttributeError:
            return None

    def _detect_charset(self, connection):
        """Sniff out the character set in use for connection results."""
        return connection.connection.charset
    
    def _compat_fetchall(self, rp, charset=None):
        """oursql isn't super-broken like MySQLdb, yaaay."""
        return rp.fetchall()

    def _compat_fetchone(self, rp, charset=None):
        """oursql isn't super-broken like MySQLdb, yaaay."""
        return rp.fetchone()


dialect = MySQL_oursql
