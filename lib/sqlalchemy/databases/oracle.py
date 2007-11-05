# oracle.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


import re, warnings, random

from sqlalchemy import util, sql, schema, exceptions, logging
from sqlalchemy.engine import default, base
from sqlalchemy.sql import compiler, visitors
from sqlalchemy.sql import operators as sql_operators
from sqlalchemy import types as sqltypes

import datetime


class OracleNumeric(sqltypes.Numeric):
    def get_col_spec(self):
        if self.precision is None:
            return "NUMERIC"
        else:
            return "NUMERIC(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length}

class OracleInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"

class OracleSmallInteger(sqltypes.Smallinteger):
    def get_col_spec(self):
        return "SMALLINT"

class OracleDate(sqltypes.Date):
    def get_col_spec(self):
        return "DATE"
    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect):
        def process(value):
            if not isinstance(value, datetime.datetime):
                return value
            else:
                return value.date()
        return process
        
class OracleDateTime(sqltypes.DateTime):
    def get_col_spec(self):
        return "DATE"
        
    def result_processor(self, dialect):
        def process(value):
            if value is None or isinstance(value,datetime.datetime):
                return value
            else:
                # convert cx_oracle datetime object returned pre-python 2.4
                return datetime.datetime(value.year,value.month,
                    value.day,value.hour, value.minute, value.second)
        return process
        
# Note:
# Oracle DATE == DATETIME
# Oracle does not allow milliseconds in DATE
# Oracle does not support TIME columns

# only if cx_oracle contains TIMESTAMP
class OracleTimestamp(sqltypes.TIMESTAMP):
    def get_col_spec(self):
        return "TIMESTAMP"

    def get_dbapi_type(self, dialect):
        return dialect.TIMESTAMP

    def result_processor(self, dialect):
        def process(value):
            if value is None or isinstance(value,datetime.datetime):
                return value
            else:
                # convert cx_oracle datetime object returned pre-python 2.4
                return datetime.datetime(value.year,value.month,
                    value.day,value.hour, value.minute, value.second)
        return process

class OracleString(sqltypes.String):
    def get_col_spec(self):
        return "VARCHAR(%(length)s)" % {'length' : self.length}

class OracleText(sqltypes.TEXT):
    def get_dbapi_type(self, dbapi):
        return dbapi.CLOB

    def get_col_spec(self):
        return "CLOB"

    def result_processor(self, dialect):
        super_process = super(OracleText, self).result_processor(dialect)
        def process(value):
            if value is None:
                return None
            elif hasattr(value, 'read'):
                # cx_oracle doesnt seem to be consistent with CLOB returning LOB or str
                if super_process:
                    return super_process(value.read())
                else:
                    return value.read()
            else:
                if super_process:
                    return super_process(value)
                else:
                    return value
        return process

class OracleRaw(sqltypes.Binary):
    def get_col_spec(self):
        return "RAW(%(length)s)" % {'length' : self.length}

class OracleChar(sqltypes.CHAR):
    def get_col_spec(self):
        return "CHAR(%(length)s)" % {'length' : self.length}

class OracleBinary(sqltypes.Binary):
    def get_dbapi_type(self, dbapi):
        return dbapi.BLOB

    def get_col_spec(self):
        return "BLOB"

    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            else:
                return value.read()
        return process
        
class OracleBoolean(sqltypes.Boolean):
    def get_col_spec(self):
        return "SMALLINT"

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
        
colspecs = {
    sqltypes.Integer : OracleInteger,
    sqltypes.Smallinteger : OracleSmallInteger,
    sqltypes.Numeric : OracleNumeric,
    sqltypes.Float : OracleNumeric,
    sqltypes.DateTime : OracleDateTime,
    sqltypes.Date : OracleDate,
    sqltypes.String : OracleString,
    sqltypes.Binary : OracleBinary,
    sqltypes.Boolean : OracleBoolean,
    sqltypes.TEXT : OracleText,
    sqltypes.TIMESTAMP : OracleTimestamp,
    sqltypes.CHAR: OracleChar,
}

ischema_names = {
    'VARCHAR2' : OracleString,
    'DATE' : OracleDate,
    'DATETIME' : OracleDateTime,
    'NUMBER' : OracleNumeric,
    'BLOB' : OracleBinary,
    'CLOB' : OracleText,
    'TIMESTAMP' : OracleTimestamp,
    'RAW' : OracleRaw,
    'FLOAT' : OracleNumeric,
    'DOUBLE PRECISION' : OracleNumeric,
    'LONG' : OracleText,
}

def descriptor():
    return {'name':'oracle',
    'description':'Oracle',
    'arguments':[
        ('dsn', 'Data Source Name', None),
        ('user', 'Username', None),
        ('password', 'Password', None)
    ]}

class OracleExecutionContext(default.DefaultExecutionContext):
    def pre_exec(self):
        super(OracleExecutionContext, self).pre_exec()
        if self.dialect.auto_setinputsizes:
            self.set_input_sizes()
        if self.compiled_parameters is not None and len(self.compiled_parameters) == 1:
            for key in self.compiled.binds:
                bindparam = self.compiled.binds[key]
                name = self.compiled.bind_names[bindparam]
                value = self.compiled_parameters[0][name]
                if bindparam.isoutparam:
                    dbtype = bindparam.type.dialect_impl(self.dialect).get_dbapi_type(self.dialect.dbapi)
                    if not hasattr(self, 'out_parameters'):
                        self.out_parameters = {}
                    self.out_parameters[name] = self.cursor.var(dbtype)
                    self.parameters[0][name] = self.out_parameters[name]

    def get_result_proxy(self):
        if hasattr(self, 'out_parameters'):
            if self.compiled_parameters is not None and len(self.compiled_parameters) == 1:
                 for bind, name in self.compiled.bind_names.iteritems():
                     if name in self.out_parameters:
                         type = bind.type
                         self.out_parameters[name] = type.dialect_impl(self.dialect).result_processor(self.dialect)(self.out_parameters[name].getvalue())
            else:
                 for k in self.out_parameters:
                     self.out_parameters[k] = self.out_parameters[k].getvalue()

        if self.cursor.description is not None:
            for column in self.cursor.description:
                type_code = column[1]
                if type_code in self.dialect.ORACLE_BINARY_TYPES:
                    return base.BufferedColumnResultProxy(self)
        
        return base.ResultProxy(self)

class OracleDialect(default.DefaultDialect):
    supports_alter = True
    supports_unicode_statements = False
    max_identifier_length = 30
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = False
    preexecute_pk_sequences = True
    supports_pk_autoincrement = False

    def __init__(self, use_ansi=True, auto_setinputsizes=True, auto_convert_lobs=True, threaded=True, allow_twophase=True, **kwargs):
        default.DefaultDialect.__init__(self, default_paramstyle='named', **kwargs)
        self.use_ansi = use_ansi
        self.threaded = threaded
        self.allow_twophase = allow_twophase
        self.supports_timestamp = self.dbapi is None or hasattr(self.dbapi, 'TIMESTAMP' )
        self.auto_setinputsizes = auto_setinputsizes
        self.auto_convert_lobs = auto_convert_lobs
        
        if self.dbapi is not None:
            self.ORACLE_BINARY_TYPES = [getattr(self.dbapi, k) for k in ["BFILE", "CLOB", "NCLOB", "BLOB"] if hasattr(self.dbapi, k)]
        else:
            self.ORACLE_BINARY_TYPES = []

    def dbapi_type_map(self):
        if self.dbapi is None or not self.auto_convert_lobs:
            return {}
        else:
            # only use this for LOB objects.  using it for strings, dates
            # etc. leads to a little too much magic, reflection doesn't know if it should
            # expect encoded strings or unicodes, etc.
            return {
                self.dbapi.CLOB: OracleText(), 
                self.dbapi.BLOB: OracleBinary(), 
                self.dbapi.BINARY: OracleRaw(), 
            }

    def dbapi(cls):
        import cx_Oracle
        return cx_Oracle
    dbapi = classmethod(dbapi)
    
    def create_connect_args(self, url):
        dialect_opts = dict(url.query)
        for opt in ('use_ansi', 'auto_setinputsizes', 'auto_convert_lobs',
                    'threaded', 'allow_twophase'):
            if opt in dialect_opts:
                util.coerce_kw_type(dialect_opts, opt, bool)
                setattr(self, opt, dialect_opts[opt])

        if url.database:
            # if we have a database, then we have a remote host
            port = url.port
            if port:
                port = int(port)
            else:
                port = 1521
            dsn = self.dbapi.makedsn(url.host, port, url.database)
        else:
            # we have a local tnsname
            dsn = url.host

        opts = dict(
            user=url.username,
            password=url.password,
            dsn=dsn,
            threaded=self.threaded,
            twophase=self.allow_twophase,
            )
        if 'mode' in url.query:
            opts['mode'] = url.query['mode']
            if isinstance(opts['mode'], basestring):
                mode = opts['mode'].upper()
                if mode == 'SYSDBA':
                    opts['mode'] = self.dbapi.SYSDBA
                elif mode == 'SYSOPER':
                    opts['mode'] = self.dbapi.SYSOPER
                else:
                    util.coerce_kw_type(opts, 'mode', int)
        # Can't set 'handle' or 'pool' via URL query args, use connect_args

        return ([], opts)

    def type_descriptor(self, typeobj):
        return sqltypes.adapt_type(typeobj, colspecs)

    def oid_column_name(self, column):
        if not isinstance(column.table, (sql.TableClause, sql.Select)):
            return None
        else:
            return "rowid"

    def create_xid(self):
        """create a two-phase transaction ID.

        this id will be passed to do_begin_twophase(), do_rollback_twophase(),
        do_commit_twophase().  its format is unspecified."""

        id = random.randint(0,2**128)
        return (0x1234, "%032x" % 9, "%032x" % id)

    def do_release_savepoint(self, connection, name):
        # Oracle does not support RELEASE SAVEPOINT
        pass

    def do_begin_twophase(self, connection, xid):
        connection.connection.begin(*xid)
        
    def do_prepare_twophase(self, connection, xid):
        connection.connection.prepare()
        
    def do_rollback_twophase(self, connection, xid, is_prepared=True, recover=False):
        self.do_rollback(connection.connection)

    def do_commit_twophase(self, connection, xid, is_prepared=True, recover=False):
        self.do_commit(connection.connection)

    def do_recover_twophase(self, connection):
        pass
        
    def create_execution_context(self, *args, **kwargs):
        return OracleExecutionContext(self, *args, **kwargs)

    def has_table(self, connection, table_name, schema=None):
        cursor = connection.execute("""select table_name from all_tables where table_name=:name""", {'name':self._denormalize_name(table_name)})
        return bool( cursor.fetchone() is not None )

    def has_sequence(self, connection, sequence_name):
        cursor = connection.execute("""select sequence_name from all_sequences where sequence_name=:name""", {'name':self._denormalize_name(sequence_name)})
        return bool( cursor.fetchone() is not None )

    def _locate_owner_row(self, owner, name, rows, raiseerr=False):
        """return the row in the given list of rows which references the given table name and owner name."""
        if not rows:
            if raiseerr:
                raise exceptions.NoSuchTableError(name)
            else:
                return None
        else:
            if owner is not None:
                for row in rows:
                    if owner.upper() in row[0]:
                        return row
                else:
                    if raiseerr:
                        raise exceptions.AssertionError("Specified owner %s does not own table %s" % (owner, name))
                    else:
                        return None
            else:
                if len(rows)==1:
                    return rows[0]
                else:
                    if raiseerr:
                        raise exceptions.AssertionError("There are multiple tables with name '%s' visible to the schema, you must specifiy owner" % name)
                    else:
                        return None

    def _resolve_table_owner(self, connection, name, table, dblink=''):
        """Locate the given table in the ``ALL_TAB_COLUMNS`` view,
        including searching for equivalent synonyms and dblinks.
        """

        c = connection.execute ("select distinct OWNER from ALL_TAB_COLUMNS%(dblink)s where TABLE_NAME = :table_name" % {'dblink':dblink}, {'table_name':name})
        rows = c.fetchall()
        try:
            row = self._locate_owner_row(table.owner, name, rows, raiseerr=True)
            return name, row['OWNER'], ''
        except exceptions.SQLAlchemyError:
            # locate synonyms
            c = connection.execute ("""select OWNER, TABLE_OWNER, TABLE_NAME, DB_LINK
                                       from   ALL_SYNONYMS%(dblink)s
                                       where  SYNONYM_NAME = :synonym_name
                                       and (DB_LINK IS NOT NULL
                                               or ((TABLE_NAME, TABLE_OWNER) in
                                                    (select TABLE_NAME, OWNER from ALL_TAB_COLUMNS%(dblink)s)))""" % {'dblink':dblink},
                                    {'synonym_name':name})
            rows = c.fetchall()
            row = self._locate_owner_row(table.owner, name, rows)
            if row is None:
                row = self._locate_owner_row("PUBLIC", name, rows)

            if row is not None:
                owner, name, dblink = row['TABLE_OWNER'], row['TABLE_NAME'], row['DB_LINK']
                if dblink:
                    dblink = '@' + dblink
                    if not owner:
                        # re-resolve table owner using new dblink variable
                        t1, owner, t2 = self._resolve_table_owner(connection, name, table, dblink=dblink)
                else:
                    dblink = ''
                return name, owner, dblink
            raise

    def _normalize_name(self, name):
        if name is None:
            return None
        elif name.upper() == name and not self.identifier_preparer._requires_quotes(name.lower().decode(self.encoding)):
            return name.lower().decode(self.encoding)
        else:
            return name.decode(self.encoding)
    
    def _denormalize_name(self, name):
        if name is None:
            return None
        elif name.lower() == name and not self.identifier_preparer._requires_quotes(name.lower()):
            return name.upper().encode(self.encoding)
        else:
            return name.encode(self.encoding)
    
    def table_names(self, connection, schema):
        # note that table_names() isnt loading DBLINKed or synonym'ed tables
        s = "select table_name from all_tables where tablespace_name NOT IN ('SYSTEM', 'SYSAUX')"
        return [self._normalize_name(row[0]) for row in connection.execute(s)]

    def reflecttable(self, connection, table, include_columns):
        preparer = self.identifier_preparer

        # search for table, including across synonyms and dblinks.
        # locate the actual name of the table, the real owner, and any dblink clause needed.
        actual_name, owner, dblink = self._resolve_table_owner(connection, self._denormalize_name(table.name), table)

        c = connection.execute ("select COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT from ALL_TAB_COLUMNS%(dblink)s where TABLE_NAME = :table_name and OWNER = :owner" % {'dblink':dblink}, {'table_name':actual_name, 'owner':owner})

                
        while True:
            row = c.fetchone()
            if row is None:
                break
            found_table = True

            #print "ROW:" , row
            (colname, coltype, length, precision, scale, nullable, default) = (self._normalize_name(row[0]), row[1], row[2], row[3], row[4], row[5]=='Y', row[6])

            if include_columns and colname not in include_columns:
                continue

            # INTEGER if the scale is 0 and precision is null
            # NUMBER if the scale and precision are both null
            # NUMBER(9,2) if the precision is 9 and the scale is 2
            # NUMBER(3) if the precision is 3 and scale is 0
            #length is ignored except for CHAR and VARCHAR2
            if coltype=='NUMBER' :
                if precision is None and scale is None:
                    coltype = OracleNumeric
                elif precision is None and scale == 0  :
                    coltype = OracleInteger
                else :
                    coltype = OracleNumeric(precision, scale)
            elif coltype=='CHAR' or coltype=='VARCHAR2':
                coltype = ischema_names.get(coltype, OracleString)(length)
            else:
                coltype = re.sub(r'\(\d+\)', '', coltype)
                try:
                    coltype = ischema_names[coltype]
                except KeyError:
                    warnings.warn(RuntimeWarning("Did not recognize type '%s' of column '%s'" % (coltype, colname)))
                    coltype = sqltypes.NULLTYPE

            colargs = []
            if default is not None:
                colargs.append(schema.PassiveDefault(sql.text(default)))

            table.append_column(schema.Column(colname, coltype, nullable=nullable, *colargs))

        if not table.columns:
           raise exceptions.AssertionError("Couldn't find any column information for table %s" % actual_name)

        c = connection.execute("""SELECT
             ac.constraint_name,
             ac.constraint_type,
             loc.column_name AS local_column,
             rem.table_name AS remote_table,
             rem.column_name AS remote_column,
             rem.owner AS remote_owner
           FROM all_constraints%(dblink)s ac,
             all_cons_columns%(dblink)s loc,
             all_cons_columns%(dblink)s rem
           WHERE ac.table_name = :table_name
           AND ac.constraint_type IN ('R','P')
           AND ac.owner = :owner
           AND ac.owner = loc.owner
           AND ac.constraint_name = loc.constraint_name
           AND ac.r_owner = rem.owner(+)
           AND ac.r_constraint_name = rem.constraint_name(+)
           -- order multiple primary keys correctly
           ORDER BY ac.constraint_name, loc.position, rem.position"""
         % {'dblink':dblink}, {'table_name' : actual_name, 'owner' : owner})

        fks = {}
        while True:
            row = c.fetchone()
            if row is None:
                break
            #print "ROW:" , row
            (cons_name, cons_type, local_column, remote_table, remote_column, remote_owner) = row[0:2] + tuple([self._normalize_name(x) for x in row[2:]])
            if cons_type == 'P':
                table.primary_key.add(table.c[local_column])
            elif cons_type == 'R':
                try:
                    fk = fks[cons_name]
                except KeyError:
                   fk = ([], [])
                   fks[cons_name] = fk
                if remote_table is None:
                    # ticket 363
                    warnings.warn("Got 'None' querying 'table_name' from all_cons_columns%(dblink)s - does the user have proper rights to the table?" % {'dblink':dblink})
                    continue
                refspec = ".".join([remote_table, remote_column])
                schema.Table(remote_table, table.metadata, autoload=True, autoload_with=connection, owner=remote_owner)
                if local_column not in fk[0]:
                    fk[0].append(local_column)
                if refspec not in fk[1]:
                    fk[1].append(refspec)

        for name, value in fks.iteritems():
            table.append_constraint(schema.ForeignKeyConstraint(value[0], value[1], name=name))


OracleDialect.logger = logging.class_logger(OracleDialect)

class _OuterJoinColumn(sql.ClauseElement):
    __visit_name__ = 'outer_join_column'
    def __init__(self, column):
        self.column = column
        
class OracleCompiler(compiler.DefaultCompiler):
    """Oracle compiler modifies the lexical structure of Select
    statements to work under non-ANSI configured Oracle databases, if
    the use_ansi flag is False.
    """

    operators = compiler.DefaultCompiler.operators.copy()
    operators.update(
        {
            sql_operators.mod : lambda x, y:"mod(%s, %s)" % (x, y)
        }
    )

    def __init__(self, *args, **kwargs):
        super(OracleCompiler, self).__init__(*args, **kwargs)
        self.__wheres = {}
        
    def default_from(self):
        """Called when a ``SELECT`` statement has no froms, and no ``FROM`` clause is to be appended.

        The Oracle compiler tacks a "FROM DUAL" to the statement.
        """

        return " FROM DUAL"

    def apply_function_parens(self, func):
        return len(func.clauses) > 0

    def visit_join(self, join, **kwargs):
        if self.dialect.use_ansi:
            return compiler.DefaultCompiler.visit_join(self, join, **kwargs)

        (where, parentjoin) = self.__wheres.get(join, (None, None))

        class VisitOn(visitors.ClauseVisitor):
            def visit_binary(s, binary):
                if binary.operator == sql_operators.eq:
                    if binary.left.table is join.right:
                        binary.left = _OuterJoinColumn(binary.left)
                    elif binary.right.table is join.right:
                        binary.right = _OuterJoinColumn(binary.right)
        
        if join.isouter:
            if where is not None:
                self.__wheres[join.left] = self.__wheres[parentjoin] = (sql.and_(VisitOn().traverse(join.onclause, clone=True), where), parentjoin)
            else:
                self.__wheres[join.left] = self.__wheres[join] = (VisitOn().traverse(join.onclause, clone=True), join)
        else:
            if where is not None:
                self.__wheres[join.left] = self.__wheres[parentjoin] = (sql.and_(join.onclause, where), parentjoin)
            else:
                self.__wheres[join.left] = self.__wheres[join] = (join.onclause, join)
            
        return self.process(join.left, asfrom=True) + ", " + self.process(join.right, asfrom=True)
    
    def get_whereclause(self, f):
        if f in self.__wheres:
            return self.__wheres[f][0]
        else:
            return None
            
    def visit_outer_join_column(self, vc):
        return self.process(vc.column) + "(+)"
        
    def visit_sequence(self, seq):
        return self.dialect.identifier_preparer.format_sequence(seq) + ".nextval"
        
    def visit_alias(self, alias, asfrom=False, **kwargs):
        """Oracle doesn't like ``FROM table AS alias``.  Is the AS standard SQL??"""
        
        if asfrom:
            return self.process(alias.original, asfrom=asfrom, **kwargs) + " " + self.preparer.format_alias(alias, self._anonymize(alias.name))
        else:
            return self.process(alias.original, **kwargs)

    def _TODO_visit_compound_select(self, select):
        """Need to determine how to get ``LIMIT``/``OFFSET`` into a ``UNION`` for Oracle."""
        pass

    def visit_select(self, select, **kwargs):
        """Look for ``LIMIT`` and OFFSET in a select statement, and if
        so tries to wrap it in a subquery with ``row_number()`` criterion.
        """

        if not getattr(select, '_oracle_visit', None) and (select._limit is not None or select._offset is not None):
            # to use ROW_NUMBER(), an ORDER BY is required.
            orderby = self.process(select._order_by_clause)
            if not orderby:
                orderby = select.oid_column
                orderby = self.process(orderby)
                
            oldselect = select
            select = select.column(sql.literal_column("ROW_NUMBER() OVER (ORDER BY %s)" % orderby).label("ora_rn")).order_by(None)
            select._oracle_visit = True
                
            limitselect = sql.select([c for c in select.c if c.key!='ora_rn'])
            if select._offset is not None:
                limitselect.append_whereclause("ora_rn>%d" % select._offset)
                if select._limit is not None:
                    limitselect.append_whereclause("ora_rn<=%d" % (select._limit + select._offset))
            else:
                limitselect.append_whereclause("ora_rn<=%d" % select._limit)
            return self.process(limitselect, **kwargs)
        else:
            return compiler.DefaultCompiler.visit_select(self, select, **kwargs)

    def limit_clause(self, select):
        return ""

    def for_update_clause(self, select):
        if select.for_update=="nowait":
            return " FOR UPDATE NOWAIT"
        else:
            return super(OracleCompiler, self).for_update_clause(select)


class OracleSchemaGenerator(compiler.SchemaGenerator):
    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column)
        colspec += " " + column.type.dialect_impl(self.dialect).get_col_spec()
        default = self.get_column_default_string(column)
        if default is not None:
            colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"
        return colspec

    def visit_sequence(self, sequence):
        if not self.checkfirst  or not self.dialect.has_sequence(self.connection, sequence.name):
            self.append("CREATE SEQUENCE %s" % self.preparer.format_sequence(sequence))
            self.execute()

class OracleSchemaDropper(compiler.SchemaDropper):
    def visit_sequence(self, sequence):
        if not self.checkfirst or self.dialect.has_sequence(self.connection, sequence.name):
            self.append("DROP SEQUENCE %s" % self.preparer.format_sequence(sequence))
            self.execute()

class OracleDefaultRunner(base.DefaultRunner):
    def visit_sequence(self, seq):
        return self.execute_string("SELECT " + self.dialect.identifier_preparer.format_sequence(seq) + ".nextval FROM DUAL", {})

class OracleIdentifierPreparer(compiler.IdentifierPreparer):
    def format_savepoint(self, savepoint):
        name = re.sub(r'^_+', '', savepoint.ident)
        return super(OracleIdentifierPreparer, self).format_savepoint(savepoint, name)

    
dialect = OracleDialect
dialect.statement_compiler = OracleCompiler
dialect.schemagenerator = OracleSchemaGenerator
dialect.schemadropper = OracleSchemaDropper
dialect.preparer = OracleIdentifierPreparer
dialect.defaultrunner = OracleDefaultRunner
