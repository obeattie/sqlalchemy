# sqlite.py
# Copyright (C) 2005 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


import sys, StringIO, string, types, re

import sqlalchemy.sql as sql
import sqlalchemy.engine as engine
import sqlalchemy.schema as schema
import sqlalchemy.ansisql as ansisql
import sqlalchemy.types as sqltypes
from sqlalchemy.ansisql import *
import datetime,time

try:
    from pysqlite2 import dbapi2 as sqlite
except:
    sqlite = None

class SLNumeric(sqltypes.Numeric):
    def get_col_spec(self):
        return "NUMERIC(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length}
class SLInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"
class SLDateTime(sqltypes.DateTime):
    def get_col_spec(self):
        return "TIMESTAMP"
    def convert_result_value(self, value):
        print "RESULT", value
        if value is None:
            print "RETNONE"
            return None
        print "HI"
        parts = value.split('.')
        try:
            (value, microsecond) = value.split('.')
            microsecond = int(microsecond)
        except ValueError:
            (value, microsecond) = (value, 0)
        tup = time.strptime(value, "%Y-%m-%d %H:%M:%S")
        return datetime.datetime(microsecond=microsecond, *tup[0:6])

class SLText(sqltypes.TEXT):
    def get_col_spec(self):
        return "TEXT"
class SLString(sqltypes.String):
    def get_col_spec(self):
        return "VARCHAR(%(length)s)" % {'length' : self.length}
class SLChar(sqltypes.CHAR):
    def get_col_spec(self):
        return "CHAR(%(length)s)" % {'length' : self.length}
class SLBinary(sqltypes.Binary):
    def get_col_spec(self):
        return "BLOB"
class SLBoolean(sqltypes.Boolean):
    def get_col_spec(self):
        return "BOOLEAN"
        
colspecs = {
    sqltypes.Integer : SLInteger,
    sqltypes.Numeric : SLNumeric,
    sqltypes.Float : SLNumeric,
    sqltypes.DateTime : SLDateTime,
    sqltypes.String : SLString,
    sqltypes.Binary : SLBinary,
    sqltypes.Boolean : SLBoolean,
    sqltypes.TEXT : SLText,
    sqltypes.CHAR: SLChar,
}

pragma_names = {
    'INTEGER' : SLInteger,
    'VARCHAR' : SLString,
    'CHAR' : SLChar,
    'TEXT' : SLText,
    'NUMERIC' : SLNumeric,
    'FLOAT' : SLNumeric,
    'TIMESTAMP' : SLDateTime,
    'BLOB' : SLBinary,
}

def engine(opts, **params):
    return SQLiteSQLEngine(opts, **params)

def descriptor():
    return {'name':'sqlite',
    'description':'SQLite',
    'arguments':[
        ('filename', "Database Filename",None)
    ]}
    
class SQLiteSQLEngine(ansisql.ANSISQLEngine):
    def __init__(self, opts, **params):
        self.filename = opts.pop('filename', ':memory:')
        self.opts = opts or {}
        params['poolclass'] = sqlalchemy.pool.SingletonThreadPool
        ansisql.ANSISQLEngine.__init__(self, **params)

    def post_exec(self, proxy, compiled, parameters, **kwargs):
        if getattr(compiled, "isinsert", False):
            self.context.last_inserted_ids = [proxy().lastrowid]

    def type_descriptor(self, typeobj):
        return sqltypes.adapt_type(typeobj, colspecs)
        
    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    def rowid_column_name(self):
        return "oid"

    def connect_args(self):
        return ([self.filename], self.opts)

    def compiler(self, statement, bindparams, **kwargs):
        return SQLiteCompiler(self, statement, bindparams, **kwargs)

    def dbapi(self):
        return sqlite

    def schemagenerator(self, proxy, **params):
        return SQLiteSchemaGenerator(proxy, **params)

    def reflecttable(self, table):
        c = self.execute("PRAGMA table_info(" + table.name + ")", {})
        while True:
            row = c.fetchone()
            if row is None:
                break
            #print "row! " + repr(row)
            (name, type, nullable, primary_key) = (row[1], row[2].upper(), not row[3], row[5])
            
            match = re.match(r'(\w+)(\(.*?\))?', type)
            coltype = match.group(1)
            args = match.group(2)
            
            #print "coltype: " + repr(coltype) + " args: " + repr(args)
            coltype = pragma_names.get(coltype, SLString)
            if args is not None:
                args = re.findall(r'(\d+)', args)
                #print "args! " +repr(args)
                coltype = coltype(*args)
            table.append_item(schema.Column(name, coltype, primary_key = primary_key, nullable = nullable))
        c = self.execute("PRAGMA foreign_key_list(" + table.name + ")", {})
        while True:
            row = c.fetchone()
            if row is None:
                break
            (tablename, localcol, remotecol) = (row[2], row[3], row[4])
            #print "row! " + repr(row)
            remotetable = Table(tablename, self, autoload = True)
            table.c[localcol].foreign_key = schema.ForeignKey(remotetable.c[remotecol])
        # check for UNIQUE indexes
        c = self.execute("PRAGMA index_list(" + table.name + ")", {})
        unique_indexes = []
        while True:
            row = c.fetchone()
            if row is None:
                break
            if (row[2] == 1):
                unique_indexes.append(row[1])
        # loop thru unique indexes for one that includes the primary key
        for idx in unique_indexes:
            c = self.execute("PRAGMA index_info(" + idx + ")", {})
            cols = []
            while True:
                row = c.fetchone()
                if row is None:
                    break
                cols.append(row[2])
                col = table.columns[row[2]]
            # unique index that includes the pk is considered a multiple primary key
            for col in cols:
                column = table.columns[col]
                table.columns[col]._set_primary_key()
                    
class SQLiteCompiler(ansisql.ANSICompiler):
    def __init__(self, *args, **params):
        params.setdefault('paramstyle', 'named')
        ansisql.ANSICompiler.__init__(self, *args, **params)
    def limit_clause(self, select):
        text = ""
        if select.limit is not None:
            text +=  " \n LIMIT " + str(select.limit)
        if select.offset is not None:
            if select.limit is None:
                text += " \n LIMIT -1"
            text += " OFFSET " + str(select.offset)
        return text
    def binary_operator_string(self, binary):
        if isinstance(binary.type, sqltypes.String) and binary.operator == '+':
            return '||'
        else:
            return ansisql.ANSICompiler.binary_operator_string(self, binary)
        
class SQLiteSchemaGenerator(ansisql.ANSISchemaGenerator):
    def get_column_specification(self, column, override_pk=False, **kwargs):
        colspec = column.name + " " + column.type.get_col_spec()
        if not column.nullable:
            colspec += " NOT NULL"
        if column.primary_key and not override_pk:
            colspec += " PRIMARY KEY"
        if column.foreign_key:
            colspec += " REFERENCES %s(%s)" % (column.foreign_key.column.table.name, column.foreign_key.column.name) 
        return colspec
    def visit_table(self, table):
        """sqlite is going to create multi-primary keys with just a UNIQUE index."""
        self.append("\nCREATE TABLE " + table.fullname + "(")

        separator = "\n"

        have_pk = False
        use_pks = len(table.primary_key) == 1
        for column in table.columns:
            self.append(separator)
            separator = ", \n"
            self.append("\t" + self.get_column_specification(column, override_pk=not use_pks))
                
        if len(table.primary_key) > 1:
            self.append(", \n")
            # put all PRIMARY KEYS in a UNIQUE index
            self.append("\tUNIQUE (%s)" % string.join([c.name for c in table.primary_key],', '))

        self.append("\n)\n\n")
        self.execute()

        
