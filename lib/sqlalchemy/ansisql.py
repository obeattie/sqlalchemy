# ansisql.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""defines ANSI SQL operations."""

import sqlalchemy.schema as schema

from sqlalchemy.schema import *
import sqlalchemy.sql as sql
import sqlalchemy.engine
from sqlalchemy.sql import *
from sqlalchemy.util import *
import string, re

def engine(**params):
    return ANSISQLEngine(**params)

class ANSISQLEngine(sqlalchemy.engine.SQLEngine):

    def schemagenerator(self, **params):
        return ANSISchemaGenerator(self, **params)
    
    def schemadropper(self, **params):
        return ANSISchemaDropper(self, **params)

    def compiler(self, statement, parameters, **kwargs):
        return ANSICompiler(self, statement, parameters, **kwargs)
    
    def connect_args(self):
        return ([],{})

    def dbapi(self):
        return None

class ANSICompiler(sql.Compiled):
    """default implementation of Compiled, which compiles ClauseElements into ANSI-compliant SQL strings."""
    def __init__(self, engine, statement, parameters=None, typemap=None, **kwargs):
        """constructs a new ANSICompiler object.
        
        engine - SQLEngine to compile against
        
        statement - ClauseElement to be compiled
        
        parameters - optional dictionary indicating a set of bind parameters
        specified with this Compiled object.  These parameters are the "default"
        key/value pairs when the Compiled is executed, and also may affect the 
        actual compilation, as in the case of an INSERT where the actual columns
        inserted will correspond to the keys present in the parameters."""
        sql.Compiled.__init__(self, engine, statement, parameters)
        self.binds = {}
        self.froms = {}
        self.wheres = {}
        self.strings = {}
        self.select_stack = []
        self.typemap = typemap or {}
        self.isinsert = False
        
    def after_compile(self):
        if self.engine.positional:
            self.positiontup = []
            match = r'%\(([\w_]+)\)s'
            params = re.finditer(match, self.strings[self.statement])
            for p in params:
                self.positiontup.append(p.group(1))
            if self.engine.paramstyle=='qmark':
                self.strings[self.statement] = re.sub(match, '?', self.strings[self.statement])
            elif self.engine.paramstyle=='format':
                self.strings[self.statement] = re.sub(match, '%s', self.strings[self.statement])
            elif self.engine.paramstyle=='numeric':
                i = [0]
                def getnum(x):
                    i[0] += 1
                    return str(i[0])
                self.strings[self.statement] = re.sub(match, getnum, self.strings[self.statement])

    def get_from_text(self, obj):
        return self.froms[obj]

    def get_str(self, obj):
        return self.strings[obj]

    def get_whereclause(self, obj):
        return self.wheres.get(obj, None)

    def get_params(self, **params):
        """returns a structure of bind parameters for this compiled object.
        This includes bind parameters that might be compiled in via the "values"
        argument of an Insert or Update statement object, and also the given **params.
        The keys inside of **params can be any key that matches the BindParameterClause
        objects compiled within this object.  The output is dependent on the paramstyle
        of the DBAPI being used; if a named style, the return result will be a dictionary
        with keynames matching the compiled statement.  If a positional style, the output
        will be a list corresponding to the bind positions in the compiled statement.
        
        for an executemany style of call, this method should be called for each element
        in the list of parameter groups that will ultimately be executed.
        """
        if self.parameters is not None:
            bindparams = self.parameters.copy()
        else:
            bindparams = {}
        bindparams.update(params)

        if self.engine.positional:
            d = OrderedDict()
            for k in self.positiontup:
                b = self.binds[k]
                d[k] = b.typeprocess(b.value, self.engine)
        else:
            d = {}
            for b in self.binds.values():
                d[b.key] = b.typeprocess(b.value, self.engine)
            
        for key, value in bindparams.iteritems():
            try:
                b = self.binds[key]
            except KeyError:
                continue
            d[b.key] = b.typeprocess(value, self.engine)

        return d
        if self.engine.positional:
            return d.values()
        else:
            return d

    def get_named_params(self, parameters):
        """given the results of the get_params method, returns the parameters
        in dictionary format.  For a named paramstyle, this just returns the
        same dictionary.  For a positional paramstyle, the given parameters are
        assumed to be in list format and are converted back to a dictionary.
        """
#        return parameters
        if self.engine.positional:
            p = {}
            for i in range(0, len(self.positiontup)):
                p[self.positiontup[i]] = parameters[i]
            return p
        else:
            return parameters
    
    def visit_label(self, label):
        if len(self.select_stack):
            self.typemap.setdefault(label.name.lower(), label.obj.type)
        self.strings[label] = self.strings[label.obj] + " AS "  + label.name
        
    def visit_column(self, column):
        if len(self.select_stack):
            # if we are within a visit to a Select, set up the "typemap"
            # for this column which is used to translate result set values
            self.typemap.setdefault(column.key.lower(), column.type)
        if column.table is not None and column.table.name is None:
            self.strings[column] = column.name
        else:
            self.strings[column] = "%s.%s" % (column.table.name, column.name)


    def visit_fromclause(self, fromclause):
        self.froms[fromclause] = fromclause.from_name

    def visit_index(self, index):
        self.strings[index] = index.name
        
    def visit_textclause(self, textclause):
        if textclause.parens and len(textclause.text):
            self.strings[textclause] = "(" + textclause.text + ")"
        else:
            self.strings[textclause] = textclause.text
        self.froms[textclause] = textclause.text
        if textclause.typemap is not None:
            self.typemap.update(textclause.typemap)
        
    def visit_null(self, null):
        self.strings[null] = 'NULL'
       
    def visit_compound(self, compound):
        if compound.operator is None:
            sep = " "
        else:
            sep = " " + compound.operator + " "
        
        s = string.join([self.get_str(c) for c in compound.clauses], sep)
        if compound.parens:
            self.strings[compound] = "(" + s + ")"
        else:
            self.strings[compound] = s
        
    def visit_clauselist(self, list):
        if list.parens:
            self.strings[list] = "(" + string.join([self.get_str(c) for c in list.clauses], ', ') + ")"
        else:
            self.strings[list] = string.join([self.get_str(c) for c in list.clauses], ', ')

    def visit_function(self, func):
        self.strings[func] = func.name + "(" + string.join([self.get_str(c) for c in func.clauses], ', ') + ")"
        
    def visit_compound_select(self, cs):
        text = string.join([self.get_str(c) for c in cs.selects], " " + cs.keyword + " ")
        for tup in cs.clauses:
            text += " " + tup[0] + " " + self.get_str(tup[1])
        self.strings[cs] = text
        self.froms[cs] = "(" + text + ")"

    def visit_binary(self, binary):
        result = self.get_str(binary.left)
        if binary.operator is not None:
            result += " " + self.binary_operator_string(binary)
        result += " " + self.get_str(binary.right)
        if binary.parens:
            result = "(" + result + ")"
        self.strings[binary] = result

    def binary_operator_string(self, binary):
        return binary.operator

    def visit_bindparam(self, bindparam):
        if bindparam.shortname != bindparam.key:
            self.binds.setdefault(bindparam.shortname, bindparam)
        count = 1
        key = bindparam.key

        # redefine the generated name of the bind param in the case
        # that we have multiple conflicting bind parameters.
        while self.binds.setdefault(key, bindparam) is not bindparam:
            key = "%s_%d" % (bindparam.key, count)
            count += 1
        bindparam.key = key
        self.strings[bindparam] = self.bindparam_string(key)

    def bindparam_string(self, name):
        return self.engine.bindtemplate % name
        
    def visit_alias(self, alias):
        self.froms[alias] = self.get_from_text(alias.original) + " AS " + alias.name
        self.strings[alias] = self.get_str(alias.original)

    def visit_select(self, select):
        
        # the actual list of columns to print in the SELECT column list.
        # its an ordered dictionary to insure that the actual labeled column name
        # is unique.
        inner_columns = OrderedDict()

        self.select_stack.append(select)
        for c in select._raw_columns:
            if c.is_selectable():
                for co in c.columns:
                    if select.use_labels:
                        l = co.label(co._label)
                        l.accept_visitor(self)
                        inner_columns[co._label] = l
                    # TODO: figure this out, a ColumnClause with a select as a parent
                    # is different from any other kind of parent
                    elif select.issubquery and isinstance(co, sql.ColumnClause) and co.table is not None and not isinstance(co.table, sql.Select):
                        # SQLite doesnt like selecting from a subquery where the column
                        # names look like table.colname, so add a label synonomous with
                        # the column name
                        l = co.label(co.text)
                        l.accept_visitor(self)
                        inner_columns[self.get_str(l.obj)] = l
                    else:
                        co.accept_visitor(self)
                        inner_columns[self.get_str(co)] = co
            else:
                c.accept_visitor(self)
                inner_columns[self.get_str(c)] = c
        self.select_stack.pop(-1)
        
        collist = string.join([self.get_str(v) for v in inner_columns.values()], ', ')

        text = "SELECT "
        text += self.visit_select_precolumns(select)
        text += collist
        
        whereclause = select.whereclause
        
        froms = []
        for f in select.froms:

            if self.parameters is not None:
                # look at our own parameters, see if they
                # are all present in the form of BindParamClauses.  if
                # not, then append to the above whereclause column conditions
                # matching those keys
                for c in f.columns:
                    if sql.is_column(c) and self.parameters.has_key(c.key) and not self.binds.has_key(c.key):
                        value = self.parameters[c.key]
                    else:
                        continue
                    clause = c==value
                    clause.accept_visitor(self)
                    whereclause = sql.and_(clause, whereclause)
                    self.visit_compound(whereclause)

            # special thingy used by oracle to redefine a join
            w = self.get_whereclause(f)
            if w is not None:
                # TODO: move this more into the oracle module
                whereclause = sql.and_(w, whereclause)
                self.visit_compound(whereclause)
                
            t = self.get_from_text(f)
            if t is not None:
                froms.append(t)
        
        if len(froms):
            text += " \nFROM "
            text += string.join(froms, ', ')

        if whereclause is not None:
            t = self.get_str(whereclause)
            if t:
                text += " \nWHERE " + t

        for tup in select.clauses:
            ss = self.get_str(tup[1])
            if ss:
                text += " " + tup[0] + " " + ss

        if select.having is not None:
            t = self.get_str(select.having)
            if t:
                text += " \nHAVING " + t
                
        text += self.visit_select_postclauses(select)
 
        if getattr(select, 'issubquery', False):
            self.strings[select] = "(" + text + ")"
        else:
            self.strings[select] = text
        self.froms[select] = "(" + text + ")"

    def visit_select_precolumns(self, select):
        """ called when building a SELECT statment, position is just before column list """
        return select.distinct and "DISTINCT " or ""

    def visit_select_postclauses(self, select):
        """ called when building a SELECT statement, position is after all other SELECT clauses. Most DB syntaxes put LIMIT/OFFSET here """
        return (select.limit or select.offset) and self.limit_clause(select) or ""

    def limit_clause(self, select):
        if select.limit is not None:
            return  " \n LIMIT " + str(select.limit)
        if select.offset is not None:
            if select.limit is None:
                return " \n LIMIT -1"
            return " OFFSET " + str(select.offset)

    def visit_table(self, table):
        self.froms[table] = table.fullname
        self.strings[table] = ""

    def visit_join(self, join):
        # TODO: ppl are going to want RIGHT, FULL OUTER and NATURAL joins.
        righttext = self.get_from_text(join.right)
        if join.right._group_parenthesized():
            righttext = "(" + righttext + ")"
        if join.isouter:
            self.froms[join] = (self.get_from_text(join.left) + " LEFT OUTER JOIN " + righttext + 
            " ON " + self.get_str(join.onclause))
        else:
            self.froms[join] = (self.get_from_text(join.left) + " JOIN " + righttext +
            " ON " + self.get_str(join.onclause))
        self.strings[join] = self.froms[join]

    def visit_insert_column_default(self, column, default):
        """called when visiting an Insert statement, for each column in the table that
        contains a ColumnDefault object."""
        self.parameters.setdefault(column.key, None)
        
    def visit_insert_sequence(self, column, sequence):
        """called when visiting an Insert statement, for each column in the table that
        contains a Sequence object."""
        pass
    
    def visit_insert_column(self, column):
        """called when visiting an Insert statement, for each column in the table
        that is a NULL insert into the table"""
        pass
        
    def visit_insert(self, insert_stmt):
        # set up a call for the defaults and sequences inside the table
        class DefaultVisitor(schema.SchemaVisitor):
            def visit_column(s, c):
                self.visit_insert_column(c)
            def visit_column_default(s, cd):
                self.visit_insert_column_default(c, cd)
            def visit_sequence(s, seq):
                self.visit_insert_sequence(c, seq)
        vis = DefaultVisitor()
        for c in insert_stmt.table.c:
            if (isinstance(c, schema.SchemaItem) and (self.parameters is None or self.parameters.get(c.key, None) is None)):
                c.accept_schema_visitor(vis)
        
        self.isinsert = True
        colparams = self._get_colparams(insert_stmt)
        for c in colparams:
            b = c[1]
            self.binds[b.key] = b
            self.binds[b.shortname] = b
            
        text = ("INSERT INTO " + insert_stmt.table.fullname + " (" + string.join([c[0].name for c in colparams], ', ') + ")" +
         " VALUES (" + string.join([self.bindparam_string(c[1].key) for c in colparams], ', ') + ")")
         
        self.strings[insert_stmt] = text

    def visit_update(self, update_stmt):
        colparams = self._get_colparams(update_stmt)
        def create_param(p):
            if isinstance(p, sql.BindParamClause):
                self.binds[p.key] = p
                self.binds[p.shortname] = p
                return self.bindparam_string(p.key)
            else:
                p.accept_visitor(self)
                if isinstance(p, sql.ClauseElement) and not isinstance(p, sql.ColumnClause):
                    return "(" + self.get_str(p) + ")"
                else:
                    return self.get_str(p)
                
        text = "UPDATE " + update_stmt.table.fullname + " SET " + string.join(["%s=%s" % (c[0].name, create_param(c[1])) for c in colparams], ', ')
        
        if update_stmt.whereclause:
            text += " WHERE " + self.get_str(update_stmt.whereclause)
         
        self.strings[update_stmt] = text


    def _get_colparams(self, stmt):
        """determines the VALUES or SET clause for an INSERT or UPDATE
        clause based on the arguments specified to this ANSICompiler object
        (i.e., the execute() or compile() method clause object):

        insert(mytable).execute(col1='foo', col2='bar')
        mytable.update().execute(col2='foo', col3='bar')

        in the above examples, the insert() and update() methods have no "values" sent to them
        at all, so compiling them with no arguments would yield an insert for all table columns,
        or an update with no SET clauses.  but the parameters sent indicate a set of per-compilation
        arguments that result in a differently compiled INSERT or UPDATE object compared to the
        original.  The "values" parameter to the insert/update is figured as well if present,
        but the incoming "parameters" sent here take precedence.
        """
        # case one: no parameters in the statement, no parameters in the 
        # compiled params - just return binds for all the table columns
        if self.parameters is None and stmt.parameters is None:
            return [(c, bindparam(c.name, type=c.type)) for c in stmt.table.columns]

        # if we have statement parameters - set defaults in the 
        # compiled params
        if self.parameters is None:
            parameters = {}
        else:
            parameters = self.parameters.copy()

        if stmt.parameters is not None:
            for k, v in stmt.parameters.iteritems():
                parameters.setdefault(k, v)

        # now go thru compiled params, get the Column object for each key
        d = {}
        for key, value in parameters.iteritems():
            if isinstance(key, sql.ColumnClause):
                d[key] = value
            else:
                try:
                    d[stmt.table.columns[str(key)]] = value
                except KeyError:
                    pass

        # create a list of column assignment clauses as tuples
        values = []
        for c in stmt.table.columns:
            if d.has_key(c):
                value = d[c]
                if sql._is_literal(value):
                    value = bindparam(c.name, value, type=c.type)
                values.append((c, value))
        return values

    def visit_delete(self, delete_stmt):
        text = "DELETE FROM " + delete_stmt.table.fullname
        
        if delete_stmt.whereclause:
            text += " WHERE " + self.get_str(delete_stmt.whereclause)
         
        self.strings[delete_stmt] = text
        
    def __str__(self):
        return self.get_str(self.statement)


class ANSISchemaGenerator(sqlalchemy.engine.SchemaIterator):
    def get_column_specification(self, column, override_pk=False, first_pk=False):
        raise NotImplementedError()
        
    def visit_table(self, table):
        self.append("\nCREATE TABLE " + table.fullname + "(")
        
        separator = "\n"
        
        # if only one primary key, specify it along with the column
        pks = table.primary_key
        first_pk = False
        for column in table.columns:
            self.append(separator)
            separator = ", \n"
            self.append("\t" + self.get_column_specification(column, override_pk=len(pks)>1, first_pk=column.primary_key and not first_pk))
            if column.primary_key:
                first_pk = True
        # if multiple primary keys, specify it at the bottom
        if len(pks) > 1:
            self.append(", \n")
            self.append("\tPRIMARY KEY (%s)" % string.join([c.name for c in pks],', '))
                    
        self.append("\n)%s\n\n" % self.post_create_table(table))
        self.execute()        
        if hasattr(table, 'indexes'):
            for index in table.indexes:
                self.visit_index(index)
        
    def post_create_table(self, table):
        return ''

    def get_column_default_string(self, column):
        if isinstance(column.default, schema.PassiveDefault):
            if isinstance(column.default.arg, str):
                return repr(column.default.arg)
            else:
                return str(column.default.arg.compile(self.engine))
        else:
            return None

    def visit_column(self, column):
        pass

    def visit_index(self, index):
        self.append('CREATE ')
        if index.unique:
            self.append('UNIQUE ')
        self.append('INDEX %s ON %s (%s)' \
                    % (index.name, index.table.name,
                       string.join([c.name for c in index.columns], ', ')))
        self.execute()
        
    
class ANSISchemaDropper(sqlalchemy.engine.SchemaIterator):
    def visit_index(self, index):
        self.append("\nDROP INDEX " + index.name)
        self.execute()
        
    def visit_table(self, table):
        # NOTE: indexes on the table will be automatically dropped, so
        # no need to drop them individually
        self.append("\nDROP TABLE " + table.fullname)
        self.execute()


class ANSIDefaultRunner(sqlalchemy.engine.DefaultRunner):
    pass
