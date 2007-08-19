# mssql.py

"""MSSQL backend, thru either pymssq, adodbapi or pyodbc interfaces.

* ``IDENTITY`` columns are supported by using SA ``schema.Sequence()``
  objects. In other words::

    Table('test', mss_engine,
           Column('id',   Integer, Sequence('blah',100,10), primary_key=True),
           Column('name', String(20))
         ).create()

  would yield::

   CREATE TABLE test (
     id INTEGER NOT NULL IDENTITY(100,10) PRIMARY KEY,
     name VARCHAR(20)
     )

  Note that the start & increment values for sequences are optional
  and will default to 1,1.

* Support for ``SET IDENTITY_INSERT ON`` mode (automagic on / off for 
  ``INSERT`` s)

* Support for auto-fetching of ``@@IDENTITY/@@SCOPE_IDENTITY()`` on ``INSERT``

* ``select._limit`` implemented as ``SELECT TOP n``


Known issues / TODO:

* No support for more than one ``IDENTITY`` column per table

* pymssql has problems with binary and unicode data that this module
  does **not** work around
  
"""

import datetime, random, warnings, re, sys, operator

from sqlalchemy import util, sql, schema, exceptions
from sqlalchemy.sql import compiler, expression
from sqlalchemy.engine import default, base
from sqlalchemy import types as sqltypes
    
class MSNumeric(sqltypes.Numeric):
    def result_processor(self, dialect):
        return None

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                # Not sure that this exception is needed
                return value
            else:
                return str(value) 
        return process
        
    def get_col_spec(self):
        if self.precision is None:
            return "NUMERIC"
        else:
            return "NUMERIC(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length}

class MSFloat(sqltypes.Float):
    def get_col_spec(self):
        return "FLOAT(%(precision)s)" % {'precision': self.precision}

    def bind_processor(self, dialect):
        def process(value):
            """By converting to string, we can use Decimal types round-trip."""
            if not value is None:
                return str(value)
            return None
        return process
        
class MSInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"

class MSBigInteger(MSInteger):
    def get_col_spec(self):
        return "BIGINT"

class MSTinyInteger(MSInteger):
    def get_col_spec(self):
        return "TINYINT"

class MSSmallInteger(MSInteger):
    def get_col_spec(self):
        return "SMALLINT"

class MSDateTime(sqltypes.DateTime):
    def __init__(self, *a, **kw):
        super(MSDateTime, self).__init__(False)

    def get_col_spec(self):
        return "DATETIME"

class MSDate(sqltypes.Date):
    def __init__(self, *a, **kw):
        super(MSDate, self).__init__(False)

    def get_col_spec(self):
        return "SMALLDATETIME"

class MSTime(sqltypes.Time):
    __zero_date = datetime.date(1900, 1, 1)

    def __init__(self, *a, **kw):
        super(MSTime, self).__init__(False)
    
    def get_col_spec(self):
        return "DATETIME"

    def bind_processor(self, dialect):
        def process(value):
            if isinstance(value, datetime.datetime):
                value = datetime.datetime.combine(self.__zero_date, value.time())
            elif isinstance(value, datetime.time):
                value = datetime.datetime.combine(self.__zero_date, value)
            return value
        return process
    
    def result_processor(self, dialect):
        def process(value):
            if isinstance(value, datetime.datetime):
                return value.time()
            elif isinstance(value, datetime.date):
                return datetime.time(0, 0, 0)
            return value
        return process
        
class MSDateTime_adodbapi(MSDateTime):
    def result_processor(self, dialect):
        def process(value):
            # adodbapi will return datetimes with empty time values as datetime.date() objects.
            # Promote them back to full datetime.datetime()
            if value and not hasattr(value, 'second'):
                return datetime.datetime(value.year, value.month, value.day)
            return value
        return process
        
class MSDateTime_pyodbc(MSDateTime):
    def bind_processor(self, dialect):
        def process(value):
            if value and not hasattr(value, 'second'):
                return datetime.datetime(value.year, value.month, value.day)
            else:
                return value
        return process
        
class MSDate_pyodbc(MSDate):
    def bind_processor(self, dialect):
        def process(value):
            if value and not hasattr(value, 'second'):
                return datetime.datetime(value.year, value.month, value.day)
            else:
                return value
        return process
    
    def result_processor(self, dialect):
        def process(value):
            # pyodbc returns SMALLDATETIME values as datetime.datetime(). truncate it back to datetime.date()
            if value and hasattr(value, 'second'):
                return value.date()
            else:
                return value
        return process
        
class MSDate_pymssql(MSDate):
    def result_processor(self, dialect):
        def process(value):
            # pymssql will return SMALLDATETIME values as datetime.datetime(), truncate it back to datetime.date()
            if value and hasattr(value, 'second'):
                return value.date()
            else:
                return value
        return process
        
class MSText(sqltypes.TEXT):
    def get_col_spec(self):
        if self.dialect.text_as_varchar:
            return "VARCHAR(max)"            
        else:
            return "TEXT"

class MSString(sqltypes.String):
    def get_col_spec(self):
        return "VARCHAR(%(length)s)" % {'length' : self.length}

class MSNVarchar(sqltypes.Unicode):
    def get_col_spec(self):
        if self.length:
            return "NVARCHAR(%(length)s)" % {'length' : self.length}
        elif self.dialect.text_as_varchar:
            return "NVARCHAR(max)"
        else:
            return "NTEXT"

class AdoMSNVarchar(MSNVarchar):
    """overrides bindparam/result processing to not convert any unicode strings"""
    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect):
        return None

class MSChar(sqltypes.CHAR):
    def get_col_spec(self):
        return "CHAR(%(length)s)" % {'length' : self.length}

class MSNChar(sqltypes.NCHAR):
    def get_col_spec(self):
        return "NCHAR(%(length)s)" % {'length' : self.length}

class MSBinary(sqltypes.Binary):
    def get_col_spec(self):
        return "IMAGE"

class MSBoolean(sqltypes.Boolean):
    def get_col_spec(self):
        return "BIT"

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
        
class MSTimeStamp(sqltypes.TIMESTAMP):
    def get_col_spec(self):
        return "TIMESTAMP"
        
class MSMoney(sqltypes.TypeEngine):
    def get_col_spec(self):
        return "MONEY"
        
class MSSmallMoney(MSMoney):
    def get_col_spec(self):
        return "SMALLMONEY"
        
class MSUniqueIdentifier(sqltypes.TypeEngine):
    def get_col_spec(self):
        return "UNIQUEIDENTIFIER"
        
class MSVariant(sqltypes.TypeEngine):
    def get_col_spec(self):
        return "SQL_VARIANT"
        
def descriptor():
    return {'name':'mssql',
    'description':'MSSQL',
    'arguments':[
        ('user',"Database Username",None),
        ('password',"Database Password",None),
        ('db',"Database Name",None),
        ('host',"Hostname", None),
    ]}

class MSSQLExecutionContext(default.DefaultExecutionContext):
    def __init__(self, *args, **kwargs):
        self.IINSERT = self.HASIDENT = False
        super(MSSQLExecutionContext, self).__init__(*args, **kwargs)

    def _has_implicit_sequence(self, column):
        if column.primary_key and column.autoincrement:
            if isinstance(column.type, sqltypes.Integer) and not column.foreign_key:
                if column.default is None or (isinstance(column.default, schema.Sequence) and \
                                              column.default.optional):
                    return True
        return False

    def pre_exec(self):
        """MS-SQL has a special mode for inserting non-NULL values
        into IDENTITY columns.
        
        Activate it if the feature is turned on and needed.
        """
        if self.compiled.isinsert:
            tbl = self.compiled.statement.table
            if not hasattr(tbl, 'has_sequence'):
                tbl.has_sequence = None
                for column in tbl.c:
                    if getattr(column, 'sequence', False) or self._has_implicit_sequence(column):
                        tbl.has_sequence = column
                        break

            self.HASIDENT = bool(tbl.has_sequence)
            if self.dialect.auto_identity_insert and self.HASIDENT:
                if isinstance(self.compiled_parameters, list):
                    self.IINSERT = tbl.has_sequence.key in self.compiled_parameters[0]
                else:
                    self.IINSERT = tbl.has_sequence.key in self.compiled_parameters
            else:
                self.IINSERT = False

            if self.IINSERT:
                self.cursor.execute("SET IDENTITY_INSERT %s ON" % self.dialect.identifier_preparer.format_table(self.compiled.statement.table))

        super(MSSQLExecutionContext, self).pre_exec()

    def post_exec(self):
        """Turn off the INDENTITY_INSERT mode if it's been activated,
        and fetch recently inserted IDENTIFY values (works only for
        one column).
        """
        
        if self.compiled.isinsert:
            if self.IINSERT:
                self.cursor.execute("SET IDENTITY_INSERT %s OFF" % self.dialect.identifier_preparer.format_table(self.compiled.statement.table))
                self.IINSERT = False
            elif self.HASIDENT:
                if not len(self._last_inserted_ids) or self._last_inserted_ids[0] is None:
                    if self.dialect.use_scope_identity:
                        self.cursor.execute("SELECT scope_identity() AS lastrowid")
                    else:
                        self.cursor.execute("SELECT @@identity AS lastrowid")
                    row = self.cursor.fetchone()
                    self._last_inserted_ids = [int(row[0])] + self._last_inserted_ids[1:]
                    # print "LAST ROW ID", self._last_inserted_ids
            self.HASIDENT = False
        super(MSSQLExecutionContext, self).post_exec()
    
    _ms_is_select = re.compile(r'\s*(?:SELECT|sp_columns)',
                               re.I | re.UNICODE)
    
    def is_select(self):
        return self._ms_is_select.match(self.statement) is not None


class MSSQLExecutionContext_pyodbc (MSSQLExecutionContext):    
    def pre_exec(self):
        """execute "set nocount on" on all connections, as a partial 
        workaround for multiple result set issues."""
                
        if not getattr(self.connection, 'pyodbc_done_nocount', False):
            self.connection.execute('SET nocount ON')
            self.connection.pyodbc_done_nocount = True
            
        super(MSSQLExecutionContext_pyodbc, self).pre_exec()

        # where appropriate, issue "select scope_identity()" in the same statement
        if self.compiled.isinsert and self.HASIDENT and (not self.IINSERT) and self.dialect.use_scope_identity:
            self.statement += "; select scope_identity()"

    def post_exec(self):
        if self.compiled.isinsert and self.HASIDENT and (not self.IINSERT) and self.dialect.use_scope_identity:
            # do nothing - id was fetched in dialect.do_execute()
            self.HASIDENT = False
        else:
            super(MSSQLExecutionContext_pyodbc, self).post_exec()


class MSSQLDialect(default.DefaultDialect):
    colspecs = {
        sqltypes.Unicode : MSNVarchar,
        sqltypes.Integer : MSInteger,
        sqltypes.Smallinteger: MSSmallInteger,
        sqltypes.Numeric : MSNumeric,
        sqltypes.Float : MSFloat,
        sqltypes.DateTime : MSDateTime,
        sqltypes.Date : MSDate,
        sqltypes.Time : MSTime,
        sqltypes.String : MSString,
        sqltypes.Binary : MSBinary,
        sqltypes.Boolean : MSBoolean,
        sqltypes.TEXT : MSText,
        sqltypes.CHAR: MSChar,
        sqltypes.NCHAR: MSNChar,
        sqltypes.TIMESTAMP: MSTimeStamp,
    }

    ischema_names = {
        'int' : MSInteger,
        'bigint': MSBigInteger,
        'smallint' : MSSmallInteger,
        'tinyint' : MSTinyInteger,
        'varchar' : MSString,
        'nvarchar' : MSNVarchar,
        'char' : MSChar,
        'nchar' : MSNChar,
        'text' : MSText,
        'ntext' : MSText,
        'decimal' : MSNumeric,
        'numeric' : MSNumeric,
        'float' : MSFloat,
        'datetime' : MSDateTime,
        'smalldatetime' : MSDate,
        'binary' : MSBinary,
        'varbinary' : MSBinary,
        'bit': MSBoolean,
        'real' : MSFloat,
        'image' : MSBinary,
        'timestamp': MSTimeStamp,
        'money': MSMoney,
        'smallmoney': MSSmallMoney,
        'uniqueidentifier': MSUniqueIdentifier,
        'sql_variant': MSVariant,
    }

    def __new__(cls, dbapi=None, *args, **kwargs):
        if cls != MSSQLDialect:
            return super(MSSQLDialect, cls).__new__(cls, *args, **kwargs)
        if dbapi:
            dialect = dialect_mapping.get(dbapi.__name__)
            return dialect(*args, **kwargs)
        else:
            return object.__new__(cls, *args, **kwargs)
                
    def __init__(self, auto_identity_insert=True, **params):
        super(MSSQLDialect, self).__init__(**params)
        self.auto_identity_insert = auto_identity_insert
        self.text_as_varchar = False
        self.use_scope_identity = False
        self.set_default_schema_name("dbo")

    def dbapi(cls, module_name=None):
        if module_name:
            try:
                dialect_cls = dialect_mapping[module_name]
                return dialect_cls.import_dbapi()
            except KeyError:
                raise exceptions.InvalidRequestError("Unsupported MSSQL module '%s' requested (must be adodbpi, pymssql or pyodbc)" % module_name)
        else:
            for dialect_cls in [MSSQLDialect_pyodbc, MSSQLDialect_pymssql, MSSQLDialect_adodbapi]:
                try:
                    return dialect_cls.import_dbapi()
                except ImportError, e:
                    pass
            else:
                raise ImportError('No DBAPI module detected for MSSQL - please install pyodbc, pymssql, or adodbapi')
    dbapi = classmethod(dbapi)
    
    def create_connect_args(self, url):
        opts = url.translate_connect_args(['host', 'database', 'user', 'password', 'port'])
        opts.update(url.query)
        if 'auto_identity_insert' in opts:
            self.auto_identity_insert = bool(int(opts.pop('auto_identity_insert')))
        if 'query_timeout' in opts:
            self.query_timeout = int(opts.pop('query_timeout'))
        if 'text_as_varchar' in opts:
            self.text_as_varchar = bool(int(opts.pop('text_as_varchar')))
        if 'use_scope_identity' in opts:
            self.use_scope_identity = bool(int(opts.pop('use_scope_identity')))
        return self.make_connect_string(opts)

    def create_execution_context(self, *args, **kwargs):
        return MSSQLExecutionContext(self, *args, **kwargs)

    def type_descriptor(self, typeobj):
        newobj = sqltypes.adapt_type(typeobj, self.colspecs)
        # Some types need to know about the dialect
        if isinstance(newobj, (MSText, MSNVarchar)):
            newobj.dialect = self
        return newobj

    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    # this is only implemented in the dbapi-specific subclasses
    def supports_sane_rowcount(self):
        raise NotImplementedError()

    def get_default_schema_name(self, connection):
        return self.schema_name

    def set_default_schema_name(self, schema_name):
        self.schema_name = schema_name

    def last_inserted_ids(self):
        return self.context.last_inserted_ids
            
    def do_execute(self, cursor, statement, params, **kwargs):
        if params == {}:
            params = ()
        super(MSSQLDialect, self).do_execute(cursor, statement, params, **kwargs)

    def _execute(self, c, statement, parameters):
        try:
            if parameters == {}:
                parameters = ()
            c.execute(statement, parameters)
            self.context.rowcount = c.rowcount
            c.DBPROP_COMMITPRESERVE = "Y"
        except Exception, e:
            raise exceptions.SQLError(statement, parameters, e)

    def table_names(self, connection, schema):
        from sqlalchemy.databases import information_schema as ischema
        return ischema.table_names(connection, schema)

    def raw_connection(self, connection):
        """Pull the raw pymmsql connection out--sensative to "pool.ConnectionFairy" and pymssql.pymssqlCnx Classes"""
        try:
            # TODO: probably want to move this to individual dialect subclasses to 
            # save on the exception throw + simplify
            return connection.connection.__dict__['_pymssqlCnx__cnx']
        except:
            return connection.connection.adoConn

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
            (name, type, nullable, charlen, numericprec, numericscale, default) = (
                row[columns.c.column_name], 
                row[columns.c.data_type], 
                row[columns.c.is_nullable] == 'YES', 
                row[columns.c.character_maximum_length],
                row[columns.c.numeric_precision],
                row[columns.c.numeric_scale],
                row[columns.c.column_default]
            )
            if include_columns and name not in include_columns:
                continue

            args = []
            for a in (charlen, numericprec, numericscale):
                if a is not None:
                    args.append(a)
            coltype = self.ischema_names.get(type, None)
            if coltype == MSString and charlen == -1:
                coltype = MSText()                
            else:
                if coltype is None:
                    warnings.warn(RuntimeWarning("Did not recognize type '%s' of column '%s'" % (type, name)))
                    coltype = sqltypes.NULLTYPE
                    
                elif coltype == MSNVarchar and charlen == -1:
                    charlen = None
                coltype = coltype(*args)
            colargs= []
            if default is not None:
                colargs.append(schema.PassiveDefault(sql.text(default)))
                
            table.append_column(schema.Column(name, coltype, nullable=nullable, *colargs))
        
        if not found_table:
            raise exceptions.NoSuchTableError(table.name)

        # We also run an sp_columns to check for identity columns:
        cursor = connection.execute("sp_columns " + self.preparer().format_table(table))
        ic = None
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            col_name, type_name = row[3], row[5]
            if type_name.endswith("identity"):
                ic = table.c[col_name]
                # setup a psuedo-sequence to represent the identity attribute - we interpret this at table.create() time as the identity attribute
                ic.sequence = schema.Sequence(ic.name + '_identity')
                # MSSQL: only one identity per table allowed
                cursor.close()
                break
        if not ic is None:
            try:
                cursor = connection.execute("select ident_seed(?), ident_incr(?)", table.fullname, table.fullname)
                row = cursor.fetchone()
                cursor.close()
                if not row is None:
                    ic.sequence.start=int(row[0])
                    ic.sequence.increment=int(row[1])
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
                                                                         C.c.table_name == table.name))
        c = connection.execute(s)
        for row in c:
            if 'PRIMARY' in row[TC.c.constraint_type.name]:
                table.primary_key.add(table.c[row[0]])

        # Foreign key constraints
        s = sql.select([C.c.column_name,
                        R.c.table_schema, R.c.table_name, R.c.column_name,
                        RR.c.constraint_name, RR.c.match_option, RR.c.update_rule, RR.c.delete_rule],
                       sql.and_(C.c.table_name == table.name,
                                C.c.table_schema == current_schema,
                                C.c.constraint_name == RR.c.constraint_name,
                                R.c.constraint_name == RR.c.unique_constraint_name,
                                C.c.ordinal_position == R.c.ordinal_position
                                ),
                       order_by = [RR.c.constraint_name, R.c.ordinal_position])
        rows = connection.execute(s).fetchall()

        # group rows by constraint ID, to handle multi-column FKs
        fknm, scols, rcols = (None, [], [])
        for r in rows:
            scol, rschema, rtbl, rcol, rfknm, fkmatch, fkuprule, fkdelrule = r
            if rfknm != fknm:
                if fknm:
                    table.append_constraint(schema.ForeignKeyConstraint(scols, ['%s.%s' % (t,c) for (s,t,c) in rcols], fknm))
                fknm, scols, rcols = (rfknm, [], [])
            if (not scol in scols): scols.append(scol)
            if (not (rschema, rtbl, rcol) in rcols): rcols.append((rschema, rtbl, rcol))

        if fknm and scols:
            table.append_constraint(schema.ForeignKeyConstraint(scols, ['%s.%s' % (t,c) for (s,t,c) in rcols], fknm))

class MSSQLDialect_pymssql(MSSQLDialect):
    def import_dbapi(cls):
        import pymssql as module
        # pymmsql doesn't have a Binary method.  we use string
        # TODO: monkeypatching here is less than ideal
        module.Binary = lambda st: str(st)
        return module
    import_dbapi = classmethod(import_dbapi)
    
    colspecs = MSSQLDialect.colspecs.copy()
    colspecs[sqltypes.Date] = MSDate_pymssql

    ischema_names = MSSQLDialect.ischema_names.copy()
    ischema_names['smalldatetime'] = MSDate_pymssql

    def __init__(self, **params):
        super(MSSQLDialect_pymssql, self).__init__(**params)
        self.use_scope_identity = True

    def supports_sane_rowcount(self):
        return False

    def max_identifier_length(self):
        return 30

    def do_rollback(self, connection):
        # pymssql throws an error on repeated rollbacks. Ignore it.
        # TODO: this is normal behavior for most DBs.  are we sure we want to ignore it ?
        try:
            connection.rollback()
        except:
            pass

    def create_connect_args(self, url):
        r = super(MSSQLDialect_pymssql, self).create_connect_args(url)
        if hasattr(self, 'query_timeout'):
            self.dbapi._mssql.set_query_timeout(self.query_timeout)
        return r

    def make_connect_string(self, keys):
        if keys.get('port'):
            # pymssql expects port as host:port, not a separate arg
            keys['host'] = ''.join([keys.get('host', ''), ':', str(keys['port'])])
            del keys['port']
        return [[], keys]

    def is_disconnect(self, e):
        return isinstance(e, self.dbapi.DatabaseError) and "Error 10054" in str(e)


##    This code is leftover from the initial implementation, for reference
##    def do_begin(self, connection):
##        """implementations might want to put logic here for turning autocommit on/off, etc."""
##        pass  

##    def do_rollback(self, connection):
##        """implementations might want to put logic here for turning autocommit on/off, etc."""
##        try:
##            # connection.rollback() for pymmsql failed sometimes--the begin tran doesn't show up
##            # this is a workaround that seems to be handle it.
##            r = self.raw_connection(connection)
##            r.query("if @@trancount > 0 rollback tran")
##            r.fetch_array()
##            r.query("begin tran")
##            r.fetch_array()
##        except:
##            pass

##    def do_commit(self, connection):
##        """implementations might want to put logic here for turning autocommit on/off, etc.
##            do_commit is set for pymmsql connections--ADO seems to handle transactions without any issue 
##        """
##        # ADO Uses Implicit Transactions.
##        # This is very pymssql specific.  We use this instead of its commit, because it hangs on failed rollbacks.
##        # By using the "if" we don't assume an open transaction--much better.
##        r = self.raw_connection(connection)
##        r.query("if @@trancount > 0 commit tran")
##        r.fetch_array()
##        r.query("begin tran")
##        r.fetch_array()

class MSSQLDialect_pyodbc(MSSQLDialect):
    
    def __init__(self, **params):
        super(MSSQLDialect_pyodbc, self).__init__(**params)
        # whether use_scope_identity will work depends on the version of pyodbc
        try:
            import pyodbc
            self.use_scope_identity = hasattr(pyodbc.Cursor, 'nextset')
        except:
            pass
        
    def import_dbapi(cls):
        import pyodbc as module
        return module
    import_dbapi = classmethod(import_dbapi)
    
    colspecs = MSSQLDialect.colspecs.copy()
    colspecs[sqltypes.Unicode] = AdoMSNVarchar
    colspecs[sqltypes.Date] = MSDate_pyodbc
    colspecs[sqltypes.DateTime] = MSDateTime_pyodbc

    ischema_names = MSSQLDialect.ischema_names.copy()
    ischema_names['nvarchar'] = AdoMSNVarchar
    ischema_names['smalldatetime'] = MSDate_pyodbc
    ischema_names['datetime'] = MSDateTime_pyodbc

    def supports_sane_rowcount(self):
        return False

    def supports_unicode_statements(self):
        """indicate whether the DBAPI can receive SQL statements as Python unicode strings"""
        # PyODBC unicode is broken on UCS-4 builds
        return sys.maxunicode == 65535

    def make_connect_string(self, keys):
        if 'dsn' in keys:
            connectors = ['dsn=%s' % keys['dsn']]
        else:
            connectors = ["Driver={SQL Server}"]
            if 'port' in keys:
                connectors.append('Server=%s,%d' % (keys.get('host'), keys.get('port')))
            else:
                connectors.append('Server=%s' % keys.get('host'))
            connectors.append("Database=%s" % keys.get("database"))
        user = keys.get("user")
        if user:
            connectors.append("UID=%s" % user)
            connectors.append("PWD=%s" % keys.get("password", ""))
        else:
            connectors.append ("TrustedConnection=Yes")
        return [[";".join (connectors)], {}]

    def is_disconnect(self, e):
        return isinstance(e, self.dbapi.Error) and '[08S01]' in str(e)

    def create_execution_context(self, *args, **kwargs):
        return MSSQLExecutionContext_pyodbc(self, *args, **kwargs)

    def do_execute(self, cursor, statement, parameters, context=None, **kwargs):
        super(MSSQLDialect_pyodbc, self).do_execute(cursor, statement, parameters, context=context, **kwargs)
        if context and context.HASIDENT and (not context.IINSERT) and context.dialect.use_scope_identity:
            import pyodbc
            # fetch the last inserted id from the manipulated statement (pre_exec).
            try:
                row = cursor.fetchone()
            except pyodbc.Error, e:
                # if nocount OFF fetchone throws an exception and we have to jump over
                # the rowcount to the resultset
                cursor.nextset()
                row = cursor.fetchone()
            context._last_inserted_ids = [int(row[0])]

class MSSQLDialect_adodbapi(MSSQLDialect):
    def import_dbapi(cls):
        import adodbapi as module
        return module
    import_dbapi = classmethod(import_dbapi)

    colspecs = MSSQLDialect.colspecs.copy()
    colspecs[sqltypes.Unicode] = AdoMSNVarchar
    colspecs[sqltypes.DateTime] = MSDateTime_adodbapi

    ischema_names = MSSQLDialect.ischema_names.copy()
    ischema_names['nvarchar'] = AdoMSNVarchar
    ischema_names['datetime'] = MSDateTime_adodbapi

    def supports_sane_rowcount(self):
        return True

    def supports_unicode_statements(self):
        """indicate whether the DBAPI can receive SQL statements as Python unicode strings"""
        return True

    def make_connect_string(self, keys):
        connectors = ["Provider=SQLOLEDB"]
        if 'port' in keys:
            connectors.append ("Data Source=%s, %s" % (keys.get("host"), keys.get("port")))
        else:
            connectors.append ("Data Source=%s" % keys.get("host"))
        connectors.append ("Initial Catalog=%s" % keys.get("database"))
        user = keys.get("user")
        if user:
            connectors.append("User Id=%s" % user)
            connectors.append("Password=%s" % keys.get("password", ""))
        else:
            connectors.append("Integrated Security=SSPI")
        return [[";".join (connectors)], {}]

    def is_disconnect(self, e):
        return isinstance(e, self.dbapi.adodbapi.DatabaseError) and "'connection failure'" in str(e)

dialect_mapping = {
    'pymssql':  MSSQLDialect_pymssql,
    'pyodbc':   MSSQLDialect_pyodbc,
    'adodbapi': MSSQLDialect_adodbapi
    }


class MSSQLCompiler(compiler.DefaultCompiler):
    def __init__(self, dialect, statement, parameters, **kwargs):
        super(MSSQLCompiler, self).__init__(dialect, statement, parameters, **kwargs)
        self.tablealiases = {}

    def get_select_precolumns(self, select):
        """ MS-SQL puts TOP, it's version of LIMIT here """
        s = select._distinct and "DISTINCT " or ""
        if select._limit:
            s += "TOP %s " % (select._limit,)
        if select._offset:
            raise exceptions.InvalidRequestError('MSSQL does not support LIMIT with an offset')
        return s

    def limit_clause(self, select):    
        # Limit in mssql is after the select keyword
        return ""
            
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

    def visit_column(self, column):
        if column.table is not None:
            # translate for schema-qualified table aliases
            t = self._schema_aliased_table(column.table)
            if t is not None:
                return self.process(t.corresponding_column(column))
        return super(MSSQLCompiler, self).visit_column(column)

    def visit_binary(self, binary):
        """Move bind parameters to the right-hand side of an operator, where possible."""
        if isinstance(binary.left, expression._BindParamClause) and binary.operator == operator.eq:
            return self.process(expression._BinaryExpression(binary.right, binary.left, binary.operator))
        else:
            return super(MSSQLCompiler, self).visit_binary(binary)

    def label_select_column(self, select, column):
        if isinstance(column, expression._Function):
            return column.label(column.name + "_" + hex(random.randint(0, 65535))[2:])        
        else:
            return super(MSSQLCompiler, self).label_select_column(select, column)

    function_rewrites =  {'current_date': 'getdate',
                          'length':     'len',
                          }
    def visit_function(self, func):
        func.name = self.function_rewrites.get(func.name, func.name)
        return super(MSSQLCompiler, self).visit_function(func)            

    def for_update_clause(self, select):
        # "FOR UPDATE" is only allowed on "DECLARE CURSOR" which SQLAlchemy doesn't use
        return ''

    def order_by_clause(self, select):
        order_by = self.process(select._order_by_clause)

        # MSSQL only allows ORDER BY in subqueries if there is a LIMIT
        if order_by and (not self.is_subquery(select) or select._limit):
            return " ORDER BY " + order_by
        else:
            return ""


class MSSQLSchemaGenerator(compiler.SchemaGenerator):
    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column) + " " + column.type.dialect_impl(self.dialect).get_col_spec()
        
        # install a IDENTITY Sequence if we have an implicit IDENTITY column
        if (not getattr(column.table, 'has_sequence', False)) and column.primary_key and \
                column.autoincrement and isinstance(column.type, sqltypes.Integer) and not column.foreign_key:
            if column.default is None or (isinstance(column.default, schema.Sequence) and column.default.optional):
                column.sequence = schema.Sequence(column.name + '_seq')

        if not column.nullable:
            colspec += " NOT NULL"

        if hasattr(column, 'sequence'):
            column.table.has_sequence = column
            colspec += " IDENTITY(%s,%s)" % (column.sequence.start or 1, column.sequence.increment or 1)
        else:
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default
        
        return colspec

class MSSQLSchemaDropper(compiler.SchemaDropper):
    def visit_index(self, index):
        self.append("\nDROP INDEX %s.%s" % (
            self.preparer.quote_identifier(index.table.name),
            self.preparer.quote_identifier(index.name)
            ))
        self.execute()


class MSSQLDefaultRunner(base.DefaultRunner):
    # TODO: does ms-sql have standalone sequences ?
    pass

class MSSQLIdentifierPreparer(compiler.IdentifierPreparer):
    def __init__(self, dialect):
        super(MSSQLIdentifierPreparer, self).__init__(dialect, initial_quote='[', final_quote=']')

    def _escape_identifier(self, value):
        #TODO: determin MSSQL's escapeing rules
        return value

    def _fold_identifier_case(self, value):
        #TODO: determin MSSQL's case folding rules
        return value

dialect = MSSQLDialect
dialect.statement_compiler = MSSQLCompiler
dialect.schemagenerator = MSSQLSchemaGenerator
dialect.schemadropper = MSSQLSchemaDropper
dialect.preparer = MSSQLIdentifierPreparer
dialect.defaultrunner = MSSQLDefaultRunner



