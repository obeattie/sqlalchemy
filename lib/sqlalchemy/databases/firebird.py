# firebird.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


import sys, StringIO, string, types

from sqlalchemy import util
import sqlalchemy.engine.default as default
import sqlalchemy.sql as sql
import sqlalchemy.schema as schema
import sqlalchemy.ansisql as ansisql
import sqlalchemy.types as sqltypes
import sqlalchemy.exceptions as exceptions

try:
    import kinterbasdb
except:
    kinterbasdb = None

dbmodule = kinterbasdb

_initialized_kb = False


class FBNumeric(sqltypes.Numeric):
    def get_col_spec(self):
        if self.precision is None:
            return "NUMERIC"
        else:
            return "NUMERIC(%(precision)s, %(length)s)" % { 'precision': self.precision,
                                                            'length' : self.length }


class FBInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"


class FBSmallInteger(sqltypes.Smallinteger):
    def get_col_spec(self):
        return "SMALLINT"


class FBDateTime(sqltypes.DateTime):
    def get_col_spec(self):
        return "TIMESTAMP"


class FBDate(sqltypes.DateTime):
    def get_col_spec(self):
        return "DATE"


class FBText(sqltypes.TEXT):
    def get_col_spec(self):
        return "BLOB SUB_TYPE 2"


class FBString(sqltypes.String):
    def get_col_spec(self):
        return "VARCHAR(%(length)s)" % {'length' : self.length}


class FBChar(sqltypes.CHAR):
    def get_col_spec(self):
        return "CHAR(%(length)s)" % {'length' : self.length}


class FBBinary(sqltypes.Binary):
    def get_col_spec(self):
        return "BLOB SUB_TYPE 1"


class FBBoolean(sqltypes.Boolean):
    def get_col_spec(self):
        return "SMALLINT"


colspecs = {
    sqltypes.Integer : FBInteger,
    sqltypes.Smallinteger : FBSmallInteger,
    sqltypes.Numeric : FBNumeric,
    sqltypes.Float : FBNumeric,
    sqltypes.DateTime : FBDateTime,
    sqltypes.Date : FBDate,
    sqltypes.String : FBString,
    sqltypes.Binary : FBBinary,
    sqltypes.Boolean : FBBoolean,
    sqltypes.TEXT : FBText,
    sqltypes.CHAR: FBChar,
}


def descriptor():
    return {'name':'firebird',
    'description':'Firebird',
    'arguments':[
        ('host', 'Host Server Name', None),
        ('database', 'Database Name', None),
        ('user', 'Username', None),
        ('password', 'Password', None)
    ]}


class FBExecutionContext(default.DefaultExecutionContext):
    def supports_sane_rowcount(self):
        return True


class FBDialect(ansisql.ANSIDialect):
    def __init__(self, module = None, **params):
        global _initialized_kb
        self.module = module or dbmodule
        self.opts = {}

        if not _initialized_kb:
            _initialized_kb = True
            type_conv = params.get('type_conv', 200) or 200
            if isinstance(type_conv, types.StringTypes):
                type_conv = int(type_conv)

            concurrency_level = params.get('concurrency_level', 1) or 1
            if isinstance(concurrency_level, types.StringTypes):
                concurrency_level = int(concurrency_level)

            if kinterbasdb is not None:
                kinterbasdb.init(type_conv=type_conv, concurrency_level=concurrency_level)
        ansisql.ANSIDialect.__init__(self, **params)

    def create_connect_args(self, url):
        opts = url.translate_connect_args(['host', 'database', 'user', 'password', 'port'])
        if opts.get('port'):
            opts['host'] = "%s/%s" % (opts['host'], opts['port'])
            del opts['port']
        opts.update(url.query)
        # pop arguments that we took at the module level
        opts.pop('type_conv', None)
        opts.pop('concurrency_level', None)
        self.opts = opts

        return ([], self.opts)

    def create_execution_context(self):
        return FBExecutionContext(self)

    def type_descriptor(self, typeobj):
        return sqltypes.adapt_type(typeobj, colspecs)

    def supports_sane_rowcount(self):
        return True

    def compiler(self, statement, bindparams, **kwargs):
        return FBCompiler(self, statement, bindparams, **kwargs)

    def schemagenerator(self, *args, **kwargs):
        return FBSchemaGenerator(*args, **kwargs)

    def schemadropper(self, *args, **kwargs):
        return FBSchemaDropper(*args, **kwargs)

    def defaultrunner(self, engine, proxy):
        return FBDefaultRunner(engine, proxy)

    def preparer(self):
        return FBIdentifierPreparer(self)

    def has_table(self, connection, table_name):
        tblqry = """
        SELECT count(*)
        FROM RDB$RELATIONS R
        WHERE R.RDB$RELATION_NAME=?"""

        c = connection.execute(tblqry, [table_name.upper()])
        row = c.fetchone()
        if row[0] > 0:
            return True
        else:
            return False

    def reflecttable(self, connection, table):
        #TODO: map these better
        column_func = {
            14 : lambda r: sqltypes.String(r['FLEN']), # TEXT
            7  : lambda r: sqltypes.Integer(), # SHORT
            8  : lambda r: sqltypes.Integer(), # LONG
            9  : lambda r: sqltypes.Float(), # QUAD
            10 : lambda r: sqltypes.Float(), # FLOAT
            27 : lambda r: sqltypes.Float(), # DOUBLE
            35 : lambda r: sqltypes.DateTime(), # TIMESTAMP
            37 : lambda r: sqltypes.String(r['FLEN']), # VARYING
            261: lambda r: sqltypes.TEXT(), # BLOB
            40 : lambda r: sqltypes.Char(r['FLEN']), # CSTRING
            12 : lambda r: sqltypes.Date(), # DATE
            13 : lambda r: sqltypes.Time(), # TIME
            16 : lambda r: sqltypes.Numeric(precision=r['FPREC'], length=r['FSCALE'] * -1)  #INT64
            }
        tblqry = """
        SELECT DISTINCT R.RDB$FIELD_NAME AS FNAME,
                  R.RDB$NULL_FLAG AS NULL_FLAG,
                  R.RDB$FIELD_POSITION,
                  F.RDB$FIELD_TYPE AS FTYPE,
                  F.RDB$FIELD_SUB_TYPE AS STYPE,
                  F.RDB$FIELD_LENGTH AS FLEN,
                  F.RDB$FIELD_PRECISION AS FPREC,
                  F.RDB$FIELD_SCALE AS FSCALE
        FROM RDB$RELATION_FIELDS R
             JOIN RDB$FIELDS F ON R.RDB$FIELD_SOURCE=F.RDB$FIELD_NAME
        WHERE F.RDB$SYSTEM_FLAG=0 and R.RDB$RELATION_NAME=?
        ORDER BY R.RDB$FIELD_POSITION"""
        keyqry = """
        SELECT SE.RDB$FIELD_NAME SENAME
        FROM RDB$RELATION_CONSTRAINTS RC
             JOIN RDB$INDEX_SEGMENTS SE
               ON RC.RDB$INDEX_NAME=SE.RDB$INDEX_NAME
        WHERE RC.RDB$CONSTRAINT_TYPE=? AND RC.RDB$RELATION_NAME=?"""
        fkqry = """
        SELECT RC.RDB$CONSTRAINT_NAME CNAME,
               CSE.RDB$FIELD_NAME FNAME,
               IX2.RDB$RELATION_NAME RNAME,
               SE.RDB$FIELD_NAME SENAME
        FROM RDB$RELATION_CONSTRAINTS RC
             JOIN RDB$INDICES IX1
               ON IX1.RDB$INDEX_NAME=RC.RDB$INDEX_NAME
             JOIN RDB$INDICES IX2
               ON IX2.RDB$INDEX_NAME=IX1.RDB$FOREIGN_KEY
             JOIN RDB$INDEX_SEGMENTS CSE
               ON CSE.RDB$INDEX_NAME=IX1.RDB$INDEX_NAME
             JOIN RDB$INDEX_SEGMENTS SE
               ON SE.RDB$INDEX_NAME=IX2.RDB$INDEX_NAME AND SE.RDB$FIELD_POSITION=CSE.RDB$FIELD_POSITION
        WHERE RC.RDB$CONSTRAINT_TYPE=? AND RC.RDB$RELATION_NAME=?
        ORDER BY SE.RDB$INDEX_NAME, SE.RDB$FIELD_POSITION"""

        # get primary key fields
        c = connection.execute(keyqry, ["PRIMARY KEY", table.name.upper()])
        pkfields =[r['SENAME'] for r in c.fetchall()]

        # get all of the fields for this table

        def lower_if_possible(name):
            # Remove trailing spaces: FB uses a CHAR() type,
            # that is padded with spaces
            name = name.rstrip()
            # If its composed only by upper case chars, use
            # the lowered version, otherwise keep the original
            # (even if stripped...)
            lname = name.lower()
            if lname.upper() == name and not ' ' in name:
                return lname
            return name

        c = connection.execute(tblqry, [table.name.upper()])
        row = c.fetchone()
        if not row:
            raise exceptions.NoSuchTableError(table.name)

        while row:
            name = row['FNAME']
            args = [lower_if_possible(name)]

            kw = {}
            # get the data types and lengths
            args.append(column_func[row['FTYPE']](row))

            # is it a primary key?
            kw['primary_key'] = name in pkfields

            table.append_column(schema.Column(*args, **kw))
            row = c.fetchone()

        # get the foreign keys
        c = connection.execute(fkqry, ["FOREIGN KEY", table.name.upper()])
        fks = {}
        while True:
            row = c.fetchone()
            if not row: break

            cname = lower_if_possible(row['CNAME'])
            try:
                fk = fks[cname]
            except KeyError:
                fks[cname] = fk = ([], [])
            rname = lower_if_possible(row['RNAME'])
            schema.Table(rname, table.metadata, autoload=True, autoload_with=connection)
            fname = lower_if_possible(row['FNAME'])
            refspec = rname + '.' + lower_if_possible(row['SENAME'])
            fk[0].append(fname)
            fk[1].append(refspec)

        for name,value in fks.iteritems():
            table.append_constraint(schema.ForeignKeyConstraint(value[0], value[1], name=name))

    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    def do_execute(self, cursor, statement, parameters, **kwargs):
        cursor.execute(statement, parameters or [])

    def do_rollback(self, connection):
        connection.rollback(True)

    def do_commit(self, connection):
        connection.commit(True)

    def connection(self):
        """Returns a managed DBAPI connection from this SQLEngine's connection pool."""
        c = self._pool.connect()
        c.supportsTransactions = 0
        return c

    def dbapi(self):
        return self.module


class FBCompiler(ansisql.ANSICompiler):
    """Firebird specific idiosincrasies"""

    def visit_alias(self, alias):
        # Override to not use the AS keyword which FB 1.5 does not like
        self.froms[alias] = self.get_from_text(alias.original) + " " + self.preparer.format_alias(alias)
        self.strings[alias] = self.get_str(alias.original)

    def visit_function(self, func):
        if len(func.clauses):
            super(FBCompiler, self).visit_function(func)
        else:
            self.strings[func] = func.name

    def visit_insert(self, insert):
        """Inserts are required to have the primary keys be explicitly present.
         mapper will by default not put them in the insert statement to comply
         with autoincrement fields that require they not be present. So,
         put them all in for all primary key columns."""
        for c in insert.table.primary_key:
            if not self.parameters.has_key(c.key):
                self.parameters[c.key] = None
        return ansisql.ANSICompiler.visit_insert(self, insert)

    def visit_select_precolumns(self, select):
        """Called when building a SELECT statement, position is just before column list
        Firebird puts the limit and offset right after the select..."""
        result = ""
        if select.limit:
            result += " FIRST %d "  % select.limit
        if select.offset:
            result +=" SKIP %d "  %  select.offset
        if select.distinct:
            result += " DISTINCT "
        return result

    def limit_clause(self, select):
        """Already taken care of in the visit_select_precolumns method."""
        return ""


class FBSchemaGenerator(ansisql.ANSISchemaGenerator):
    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column)
        colspec += " " + column.type.engine_impl(self.engine).get_col_spec()

        default = self.get_column_default_string(column)
        if default is not None:
            colspec += " DEFAULT " + default

        if not column.nullable or column.primary_key:
            colspec += " NOT NULL"

        return colspec

    def visit_sequence(self, sequence):
        self.append("CREATE GENERATOR %s" % sequence.name)
        self.execute()


class FBSchemaDropper(ansisql.ANSISchemaDropper):
    def visit_sequence(self, sequence):
        self.append("DROP GENERATOR %s" % sequence.name)
        self.execute()


class FBDefaultRunner(ansisql.ANSIDefaultRunner):
    def exec_default_sql(self, default):
        c = sql.select([default.arg], from_obj=["rdb$database"], engine=self.engine).compile()
        return self.proxy(str(c), c.get_params()).fetchone()[0]

    def visit_sequence(self, seq):
        return self.proxy("SELECT gen_id(" + seq.name + ", 1) FROM rdb$database").fetchone()[0]


RESERVED_WORDS = util.Set(
    ["action", "active", "add", "admin", "after", "all", "alter", "and", "any",
     "as", "asc", "ascending", "at", "auto", "autoddl", "avg", "based", "basename",
     "base_name", "before", "begin", "between", "bigint", "blob", "blobedit", "buffer",
     "by", "cache", "cascade", "case", "cast", "char", "character", "character_length",
     "char_length", "check", "check_point_len", "check_point_length", "close", "collate",
     "collation", "column", "commit", "committed", "compiletime", "computed", "conditional",
     "connect", "constraint", "containing", "continue", "count", "create", "cstring",
     "current", "current_connection", "current_date", "current_role", "current_time",
     "current_timestamp", "current_transaction", "current_user", "cursor", "database",
     "date", "day", "db_key", "debug", "dec", "decimal", "declare", "default", "delete",
     "desc", "descending", "describe", "descriptor", "disconnect", "display", "distinct",
     "do", "domain", "double", "drop", "echo", "edit", "else", "end", "entry_point",
     "escape", "event", "exception", "execute", "exists", "exit", "extern", "external",
     "extract", "fetch", "file", "filter", "float", "for", "foreign", "found", "free_it",
     "from", "full", "function", "gdscode", "generator", "gen_id", "global", "goto",
     "grant", "group", "group_commit_", "group_commit_wait", "having", "help", "hour",
     "if", "immediate", "in", "inactive", "index", "indicator", "init", "inner", "input",
     "input_type", "insert", "int", "integer", "into", "is", "isolation", "isql", "join",
     "key", "lc_messages", "lc_type", "left", "length", "lev", "level", "like", "logfile",
     "log_buffer_size", "log_buf_size", "long", "manual", "max", "maximum", "maximum_segment",
     "max_segment", "merge", "message", "min", "minimum", "minute", "module_name", "month",
     "names", "national", "natural", "nchar", "no", "noauto", "not", "null", "numeric",
     "num_log_buffers", "num_log_bufs", "octet_length", "of", "on", "only", "open", "option",
     "or", "order", "outer", "output", "output_type", "overflow", "page", "pagelength",
     "pages", "page_size", "parameter", "password", "plan", "position", "post_event",
     "precision", "prepare", "primary", "privileges", "procedure", "protected", "public",
     "quit", "raw_partitions", "rdb$db_key", "read", "real", "record_version", "recreate",
     "references", "release", "release", "reserv", "reserving", "restrict", "retain",
     "return", "returning_values", "returns", "revoke", "right", "role", "rollback",
     "row_count", "runtime", "savepoint", "schema", "second", "segment", "select",
     "set", "shadow", "shared", "shell", "show", "singular", "size", "smallint",
     "snapshot", "some", "sort", "sqlcode", "sqlerror", "sqlwarning", "stability",
     "starting", "starts", "statement", "static", "statistics", "sub_type", "sum",
     "suspend", "table", "terminator", "then", "time", "timestamp", "to", "transaction",
     "translate", "translation", "trigger", "trim", "type", "uncommitted", "union",
     "unique", "update", "upper", "user", "using", "value", "values", "varchar",
     "variable", "varying", "version", "view", "wait", "wait_time", "weekday", "when",
     "whenever", "where", "while", "with", "work", "write", "year", "yearday" ])


class FBIdentifierPreparer(ansisql.ANSIIdentifierPreparer):
    def __init__(self, dialect):
        super(FBIdentifierPreparer,self).__init__(dialect, omit_schema=True)

    def _reserved_words(self):
        return RESERVED_WORDS


dialect = FBDialect
