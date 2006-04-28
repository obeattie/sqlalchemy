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

ANSI_FUNCS = HashSet([
'CURRENT_TIME',
'CURRENT_TIMESTAMP',
'CURRENT_DATE',
'LOCALTIME',
'LOCALTIMESTAMP',
'CURRENT_USER',
'SESSION_USER',
'USER'
])


def engine(**params):
    return ANSISQLEngine(**params)

class ANSISQLEngine(sqlalchemy.engine.SQLEngine):

    def schemagenerator(self, **params):
        return ANSISchemaGenerator(self, **params)
    
    def schemadropper(self, **params):
        return ANSISchemaDropper(self, **params)

    def compiler(self, statement, parameters, **kwargs):
        return ANSICompiler(statement, parameters, engine=self, **kwargs)
    
    def connect_args(self):
        return ([],{})

    def dbapi(self):
        return None

class ANSICompiler(sql.Compiled):
    """default implementation of Compiled, which compiles ClauseElements into ANSI-compliant SQL strings."""
    def __init__(self, statement, parameters=None, typemap=None, engine=None, positional=None, paramstyle=None, **kwargs):
        """constructs a new ANSICompiler object.
        
        engine - SQLEngine to compile against
        
        statement - ClauseElement to be compiled
        
        parameters - optional dictionary indicating a set of bind parameters
        specified with this Compiled object.  These parameters are the "default"
        key/value pairs when the Compiled is executed, and also may affect the 
        actual compilation, as in the case of an INSERT where the actual columns
        inserted will correspond to the keys present in the parameters."""
        sql.Compiled.__init__(self, statement, parameters, engine=engine)
        self.binds = {}
        self.froms = {}
        self.wheres = {}
        self.strings = {}
        self.select_stack = []
        self.typemap = typemap or {}
        self.isinsert = False
        self.isupdate = False
        self.bindtemplate = ":%s"
        if engine is not None:
            self.paramstyle = engine.paramstyle
            self.positional = engine.positional
        else:
            self.positional = False
            self.paramstyle = 'named'
        
    def after_compile(self):
        # this re will search for params like :param
        # it has a negative lookbehind for an extra ':' so that it doesnt match
        # postgres '::text' tokens
        match = r'(?<!:):([\w_]+)'
        if self.paramstyle=='pyformat':
            self.strings[self.statement] = re.sub(match, lambda m:'%(' + m.group(1) +')s', self.strings[self.statement])
        elif self.positional:
            self.positiontup = []
            params = re.finditer(match, self.strings[self.statement])
            for p in params:
                self.positiontup.append(p.group(1))
            if self.paramstyle=='qmark':
                self.strings[self.statement] = re.sub(match, '?', self.strings[self.statement])
            elif self.paramstyle=='format':
                self.strings[self.statement] = re.sub(match, '%s', self.strings[self.statement])
            elif self.paramstyle=='numeric':
                i = [0]
                def getnum(x):
                    i[0] += 1
                    return str(i[0])
                self.strings[self.statement] = re.sub(match, getnum, self.strings[self.statement])

    def get_from_text(self, obj):
        return self.froms.get(obj, None)

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
        will be a list, with an iterator that will return parameter 
        values in an order corresponding to the bind positions in the compiled statement.
        
        for an executemany style of call, this method should be called for each element
        in the list of parameter groups that will ultimately be executed.
        """
        if self.parameters is not None:
            bindparams = self.parameters.copy()
        else:
            bindparams = {}
        bindparams.update(params)

        d = sql.ClauseParameters(self.engine)
        if self.positional:
            for k in self.positiontup:
                b = self.binds[k]
                d.set_parameter(k, b.value, b)
        else:
            for b in self.binds.values():
                d.set_parameter(b.key, b.value, b)
            
        for key, value in bindparams.iteritems():
            try:
                b = self.binds[key]
            except KeyError:
                continue
            d.set_parameter(b.key, value, b)

        return d

    def get_named_params(self, parameters):
        """given the results of the get_params method, returns the parameters
        in dictionary format.  For a named paramstyle, this just returns the
        same dictionary.  For a positional paramstyle, the given parameters are
        assumed to be in list format and are converted back to a dictionary.
        """
        if self.positional:
            p = {}
            for i in range(0, len(self.positiontup)):
                p[self.positiontup[i]] = parameters[i]
            return p
        else:
            return parameters
    
    def default_from(self):
        """called when a SELECT statement has no froms, and no FROM clause is to be appended.  
        gives Oracle a chance to tack on a "FROM DUAL" to the string output. """
        return ""

    def visit_label(self, label):
        if len(self.select_stack):
            self.typemap.setdefault(label.name.lower(), label.obj.type)
        self.strings[label] = self.strings[label.obj] + " AS "  + label.name
        
    def visit_column(self, column):
        if len(self.select_stack):
            # if we are within a visit to a Select, set up the "typemap"
            # for this column which is used to translate result set values
            self.typemap.setdefault(column.key.lower(), column.type)
        if column.table is None or column.table.name is None:
            self.strings[column] = column.name
        else:
            self.strings[column] = "%s.%s" % (column.table.name, column.name)


    def visit_fromclause(self, fromclause):
        self.froms[fromclause] = fromclause.from_name

    def visit_index(self, index):
        self.strings[index] = index.name
    
    def visit_typeclause(self, typeclause):
        self.strings[typeclause] = typeclause.type.engine_impl(self.engine).get_col_spec()
            
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

    def apply_function_parens(self, func):
        return func.name.upper() not in ANSI_FUNCS or len(func.clauses) > 0
        
    def visit_function(self, func):
        if len(self.select_stack):
            self.typemap.setdefault(func.name, func.type)
        if not self.apply_function_parens(func):
            self.strings[func] = ".".join(func.packagenames + [func.name])
        else:
            self.strings[func] = ".".join(func.packagenames + [func.name]) + "(" + string.join([self.get_str(c) for c in func.clauses], ', ') + ")"
        
    def visit_compound_select(self, cs):
        text = string.join([self.get_str(c) for c in cs.selects], " " + cs.keyword + " ")
        group_by = self.get_str(cs.group_by_clause)
        if group_by:
            text += " GROUP BY " + group_by
        order_by = self.get_str(cs.order_by_clause)
        if order_by:
            text += " ORDER BY " + order_by
        if cs.parens:
            self.strings[cs] = "(" + text + ")"
        else:
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
            # insure the name doesn't expand the length of the string
            # in case we're at the edge of max identifier length
            tag = "_%d" % count
            key = bindparam.key[0 : len(bindparam.key) - len(tag)] + tag
            count += 1
        bindparam.key = key
        self.strings[bindparam] = self.bindparam_string(key)

    def bindparam_string(self, name):
        return self.bindtemplate % name
        
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
            # TODO: make this polymorphic?
            if isinstance(c, sql.Select) and c._scalar:
                c.accept_visitor(self)
                inner_columns[self.get_str(c)] = c
            elif c.is_selectable():
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
        else:
            text += self.default_from()
            
        if whereclause is not None:
            t = self.get_str(whereclause)
            if t:
                text += " \nWHERE " + t

        group_by = self.get_str(select.group_by_clause)
        if group_by:
            text += " GROUP BY " + group_by

        order_by = self.get_str(select.order_by_clause)
        if order_by:
            text += " ORDER BY " + order_by

        if select.having is not None:
            t = self.get_str(select.having)
            if t:
                text += " \nHAVING " + t
                
        text += self.visit_select_postclauses(select)
 
        if select.for_update:
            text += " FOR UPDATE"
            
        if getattr(select, 'parens', False):
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

    def visit_insert_column_default(self, column, default, parameters):
        """called when visiting an Insert statement, for each column in the table that
        contains a ColumnDefault object.  adds a blank 'placeholder' parameter so the 
        Insert gets compiled with this column's name in its column and VALUES clauses."""
        parameters.setdefault(column.key, None)

    def visit_update_column_default(self, column, default, parameters):
        """called when visiting an Update statement, for each column in the table that
        contains a ColumnDefault object as an onupdate. adds a blank 'placeholder' parameter so the 
        Update gets compiled with this column's name as one of its SET clauses."""
        parameters.setdefault(column.key, None)
        
    def visit_insert_sequence(self, column, sequence, parameters):
        """called when visiting an Insert statement, for each column in the table that
        contains a Sequence object.  Overridden by compilers that support sequences to place
        a blank 'placeholder' parameter, so the Insert gets compiled with this column's
        name in its column and VALUES clauses."""
        pass
    
    def visit_insert_column(self, column, parameters):
        """called when visiting an Insert statement, for each column in the table
        that is a NULL insert into the table.  Overridden by compilers who disallow
        NULL columns being set in an Insert where there is a default value on the column
        (i.e. postgres), to remove the column from the parameter list."""
        pass
        
    def visit_insert(self, insert_stmt):
        # scan the table's columns for defaults that have to be pre-set for an INSERT
        # add these columns to the parameter list via visit_insert_XXX methods
        default_params = {}
        class DefaultVisitor(schema.SchemaVisitor):
            def visit_column(s, c):
                self.visit_insert_column(c, default_params)
            def visit_column_default(s, cd):
                self.visit_insert_column_default(c, cd, default_params)
            def visit_sequence(s, seq):
                self.visit_insert_sequence(c, seq, default_params)
        vis = DefaultVisitor()
        for c in insert_stmt.table.c:
            if (isinstance(c, schema.SchemaItem) and (self.parameters is None or self.parameters.get(c.key, None) is None)):
                c.accept_schema_visitor(vis)
        
        self.isinsert = True
        colparams = self._get_colparams(insert_stmt, default_params)

        def create_param(p):
            if isinstance(p, sql.BindParamClause):
                self.binds[p.key] = p
                if p.shortname is not None:
                    self.binds[p.shortname] = p
                return self.bindparam_string(p.key)
            else:
                p.accept_visitor(self)
                if isinstance(p, sql.ClauseElement) and not isinstance(p, sql.ColumnElement):
                    return "(" + self.get_str(p) + ")"
                else:
                    return self.get_str(p)

        text = ("INSERT INTO " + insert_stmt.table.fullname + " (" + string.join([c[0].name for c in colparams], ', ') + ")" +
         " VALUES (" + string.join([create_param(c[1]) for c in colparams], ', ') + ")")

        self.strings[insert_stmt] = text

    def visit_update(self, update_stmt):
        # scan the table's columns for onupdates that have to be pre-set for an UPDATE
        # add these columns to the parameter list via visit_update_XXX methods
        default_params = {}
        class OnUpdateVisitor(schema.SchemaVisitor):
            def visit_column_onupdate(s, cd):
                self.visit_update_column_default(c, cd, default_params)
        vis = OnUpdateVisitor()
        for c in update_stmt.table.c:
            if (isinstance(c, schema.SchemaItem) and (self.parameters is None or self.parameters.get(c.key, None) is None)):
                c.accept_schema_visitor(vis)

        self.isupdate = True
        colparams = self._get_colparams(update_stmt, default_params)
        def create_param(p):
            if isinstance(p, sql.BindParamClause):
                self.binds[p.key] = p
                self.binds[p.shortname] = p
                return self.bindparam_string(p.key)
            else:
                p.accept_visitor(self)
                if isinstance(p, sql.ClauseElement) and not isinstance(p, sql.ColumnElement):
                    return "(" + self.get_str(p) + ")"
                else:
                    return self.get_str(p)
                
        text = "UPDATE " + update_stmt.table.fullname + " SET " + string.join(["%s=%s" % (c[0].name, create_param(c[1])) for c in colparams], ', ')
        
        if update_stmt.whereclause:
            text += " WHERE " + self.get_str(update_stmt.whereclause)
         
        self.strings[update_stmt] = text


    def _get_colparams(self, stmt, default_params):
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

        for k, v in default_params.iteritems():
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
                    % (index.name, index.table.fullname,
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
