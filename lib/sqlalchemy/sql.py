# sql.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""defines the base components of SQL expression trees."""

import sqlalchemy.schema as schema
import sqlalchemy.util as util
import sqlalchemy.types as sqltypes
import string, re, random
types = __import__('types')

__all__ = ['text', 'column', 'func', 'select', 'update', 'insert', 'delete', 'join', 'and_', 'or_', 'not_', 'union', 'union_all', 'desc', 'asc', 'outerjoin', 'alias', 'subquery', 'literal', 'bindparam', 'exists']

def desc(column):
    """returns a descending ORDER BY clause element, e.g.:
    
    order_by = [desc(table1.mycol)]
    """    
    return CompoundClause(None, column, "DESC")

def asc(column):
    """returns an ascending ORDER BY clause element, e.g.:
    
    order_by = [asc(table1.mycol)]
    """
    return CompoundClause(None, column, "ASC")

def outerjoin(left, right, onclause=None, **kwargs):
    """returns an OUTER JOIN clause element, given the left and right hand expressions,
    as well as the ON condition's expression.  To chain joins together, use the resulting
    Join object's "join()" or "outerjoin()" methods."""
    return Join(left, right, onclause, isouter = True, **kwargs)

def join(left, right, onclause=None, **kwargs):
    """returns a JOIN clause element (regular inner join), given the left and right 
    hand expressions, as well as the ON condition's expression.  To chain joins 
    together, use the resulting Join object's "join()" or "outerjoin()" methods."""
    return Join(left, right, onclause, **kwargs)

def select(columns=None, whereclause = None, from_obj = [], **kwargs):
    """returns a SELECT clause element.
    
    this can also be called via the table's select() method.
    
    'columns' is a list of columns and/or selectable items to select columns from
    'whereclause' is a text or ClauseElement expression which will form the WHERE clause
    'from_obj' is an list of additional "FROM" objects, such as Join objects, which will 
    extend or override the default "from" objects created from the column list and the 
    whereclause.
    **kwargs - additional parameters for the Select object.
    """
    return Select(columns, whereclause = whereclause, from_obj = from_obj, **kwargs)

def insert(table, values = None, **kwargs):
    """returns an INSERT clause element.  
    
    This can also be called from a table directly via the table's insert() method.
    
    'table' is the table to be inserted into.
    
    'values' is a dictionary which specifies the column specifications of the INSERT, 
    and is optional.  If left as None, the column specifications are determined from the 
    bind parameters used during the compile phase of the INSERT statement.  If the 
    bind parameters also are None during the compile phase, then the column
    specifications will be generated from the full list of table columns.

    If both 'values' and compile-time bind parameters are present, the compile-time 
    bind parameters override the information specified within 'values' on a per-key basis.

    The keys within 'values' can be either Column objects or their string identifiers.  
    Each key may reference one of: a literal data value (i.e. string, number, etc.), a Column object,
    or a SELECT statement.  If a SELECT statement is specified which references this INSERT 
    statement's table, the statement will be correlated against the INSERT statement.  
    """
    return Insert(table, values, **kwargs)

def update(table, whereclause = None, values = None, **kwargs):
    """returns an UPDATE clause element.   
    
    This can also be called from a table directly via the table's update() method.
    
    'table' is the table to be updated.
    'whereclause' is a ClauseElement describing the WHERE condition of the UPDATE statement.
    'values' is a dictionary which specifies the SET conditions of the UPDATE, and is
    optional. If left as None, the SET conditions are determined from the bind parameters
    used during the compile phase of the UPDATE statement.  If the bind parameters also are
    None during the compile phase, then the SET conditions will be generated from the full
    list of table columns.

    If both 'values' and compile-time bind parameters are present, the compile-time bind
    parameters override the information specified within 'values' on a per-key basis.

    The keys within 'values' can be either Column objects or their string identifiers. Each
    key may reference one of: a literal data value (i.e. string, number, etc.), a Column
    object, or a SELECT statement.  If a SELECT statement is specified which references this
    UPDATE statement's table, the statement will be correlated against the UPDATE statement.
    """
    return Update(table, whereclause, values, **kwargs)

def delete(table, whereclause = None, **kwargs):
    """returns a DELETE clause element.  
    
    This can also be called from a table directly via the table's delete() method.
    
    'table' is the table to be updated.
    'whereclause' is a ClauseElement describing the WHERE condition of the UPDATE statement.
    """
    return Delete(table, whereclause, **kwargs)

def and_(*clauses):
    """joins a list of clauses together by the AND operator.  the & operator can be used as well."""
    return _compound_clause('AND', *clauses)

def or_(*clauses):
    """joins a list of clauses together by the OR operator.  the | operator can be used as well."""
    return _compound_clause('OR', *clauses)

def not_(clause):
    """returns a negation of the given clause, i.e. NOT(clause).  the ~ operator can be used as well."""
    clause.parens=True
    return BooleanExpression(TextClause("NOT"), clause, None)

def between_(ctest, cleft, cright):
    """ returns BETWEEN predicate clause (clausetest BETWEEN clauseleft AND clauseright) """
    return BooleanExpression(ctest, and_(cleft, cright), 'BETWEEN')
        
def exists(*args, **params):
    s = select(*args, **params)
    return BooleanExpression(TextClause("EXISTS"), s, None)

def union(*selects, **params):
    return _compound_select('UNION', *selects, **params)

def union_all(*selects, **params):
    return _compound_select('UNION ALL', *selects, **params)

def alias(*args, **params):
    return Alias(*args, **params)

def subquery(alias, *args, **params):
    return Alias(Select(*args, **params), alias)

def literal(value, type=None):
    """returns a literal clause, bound to a bind parameter.  
    
    literal clauses are created automatically when used as the right-hand 
    side of a boolean or math operation against a column object.  use this 
    function when a literal is needed on the left-hand side (and optionally on the right as well).
    
    the optional type parameter is a sqlalchemy.types.TypeEngine object which indicates bind-parameter
    and result-set translation for this literal.
    """
    return BindParamClause('literal', value, type=type)

def label(name, obj):
    """returns a Label object for the given selectable, used in the column list for a select statement."""
    return Label(name, obj)
    
def column(table, text):
    """returns a textual column clause, relative to a table.  this differs from using straight text
    or text() in that the column is treated like a regular column, i.e. gets added to a Selectable's list
    of columns."""
    return ColumnClause(text, table)
    
def bindparam(key, value = None, type=None):
    """creates a bind parameter clause with the given key.  
    
    An optional default value can be specified by the value parameter, and the optional type parameter
    is a sqlalchemy.types.TypeEngine object which indicates bind-parameter and result-set translation for
    this bind parameter."""
    if isinstance(key, schema.Column):
        return BindParamClause(key.name, value, type=key.type)
    else:
        return BindParamClause(key, value, type=type)

def text(text, engine=None, *args, **kwargs):
    """creates literal text to be inserted into a query.  
    
    When constructing a query from a select(), update(), insert() or delete(), using 
    plain strings for argument values will usually result in text objects being created
    automatically.  Use this function when creating textual clauses outside of other
    ClauseElement objects, or optionally wherever plain text is to be used.
    
    Arguments include:

    text - the text of the SQL statement to be created.  use :<param> to specify
    bind parameters; they will be compiled to their engine-specific format.

    engine - the engine to be used for this text query.  Alternatively, call the
    text() method off the engine directly.

    bindparams - a list of bindparam() instances which can be used to define the
    types and/or initial values for the bind parameters within the textual statement;
    the keynames of the bindparams must match those within the text of the statement.
    The types will be used for pre-processing on bind values.

    typemap - a dictionary mapping the names of columns represented in the SELECT
    clause of the textual statement to type objects, which will be used to perform
    post-processing on columns within the result set (for textual statements that 
    produce result sets)."""
    return TextClause(text, engine=engine, *args, **kwargs)

def null():
    """returns a Null object, which compiles to NULL in a sql statement."""
    return Null()

class FunctionGateway(object):
    """returns a callable based on an attribute name, which then returns a Function 
    object with that name."""
    def __getattr__(self, name):
        return lambda *c, **kwargs: Function(name, *c, **kwargs)
func = FunctionGateway()

def _compound_clause(keyword, *clauses):
    return CompoundClause(keyword, *clauses)

def _compound_select(keyword, *selects, **kwargs):
    return CompoundSelect(keyword, *selects, **kwargs)

def _is_literal(element):
    return not isinstance(element, ClauseElement) and not isinstance(element, schema.SchemaItem)

def is_column(col):
    return isinstance(col, schema.Column) or isinstance(col, ColumnElement)

class ClauseVisitor(schema.SchemaVisitor):
    """builds upon SchemaVisitor to define the visiting of SQL statement elements in 
    addition to Schema elements."""
    def visit_columnclause(self, column):pass
    def visit_fromclause(self, fromclause):pass
    def visit_bindparam(self, bindparam):pass
    def visit_textclause(self, textclause):pass
    def visit_compound(self, compound):pass
    def visit_compound_select(self, compound):pass
    def visit_binary(self, binary):pass
    def visit_alias(self, alias):pass
    def visit_select(self, select):pass
    def visit_join(self, join):pass
    def visit_null(self, null):pass
    def visit_clauselist(self, list):pass
    def visit_function(self, func):pass
    def visit_label(self, label):pass
        
class Compiled(ClauseVisitor):
    """represents a compiled SQL expression.  the __str__ method of the Compiled object
    should produce the actual text of the statement.  Compiled objects are specific to the
    database library that created them, and also may or may not be specific to the columns
    referenced within a particular set of bind parameters.  In no case should the Compiled
    object be dependent on the actual values of those bind parameters, even though it may
    reference those values as defaults."""

    def __init__(self, engine, statement, parameters):
        """constructs a new Compiled object.
        
        engine - SQLEngine to compile against
        
        statement - ClauseElement to be compiled
        
        parameters - optional dictionary indicating a set of bind parameters
        specified with this Compiled object.  These parameters are the "default"
        values corresponding to the ClauseElement's BindParamClauses when the Compiled 
        is executed.   In the case of an INSERT or UPDATE statement, these parameters 
        will also result in the creation of new BindParamClause objects for each key
        and will also affect the generated column list in an INSERT statement and the SET 
        clauses of an UPDATE statement.  The keys of the parameter dictionary can
        either be the string names of columns or actual sqlalchemy.schema.Column objects."""
        self.engine = engine
        self.parameters = parameters
        self.statement = statement

    def __str__(self):
        """returns the string text of the generated SQL statement."""
        raise NotImplementedError()
    def get_params(self, **params):
        """returns the bind params for this compiled object.
        
        Will start with the default parameters specified when this Compiled object
        was first constructed, and will override those values with those sent via
        **params, which are key/value pairs.  Each key should match one of the 
        BindParamClause objects compiled into this object; either the "key" or 
        "shortname" property of the BindParamClause.
        """
        raise NotImplementedError()

    def execute(self, *multiparams, **params):
        """executes this compiled object using the underlying SQLEngine"""
        if len(multiparams):
            params = multiparams
            
        return self.engine.execute_compiled(self, params)

    def scalar(self, *multiparams, **params):
        """executes this compiled object via the execute() method, then 
        returns the first column of the first row.  Useful for executing functions,
        sequences, rowcounts, etc."""
        # we are still going off the assumption that fetching only the first row
        # in a result set is not performance-wise any different than specifying limit=1
        # else we'd have to construct a copy of the select() object with the limit
        # installed (else if we change the existing select, not threadsafe)
        row = self.execute(*multiparams, **params).fetchone()
        if row is not None:
            return row[0]
        else:
            return None
        
class ClauseElement(object):
    """base class for elements of a programmatically constructed SQL expression."""
    def hash_key(self):
        """returns a string that uniquely identifies the concept this ClauseElement
        represents.

        two ClauseElements can have the same value for hash_key() iff they both correspond to
        the exact same generated SQL.  This allows the hash_key() values of a collection of
        ClauseElements to be constructed into a larger identifying string for the purpose of
        caching a SQL expression.

        Note that since ClauseElements may be mutable, the hash_key() value is subject to
        change if the underlying structure of the ClauseElement changes.""" 
        raise NotImplementedError(repr(self))
    def _get_from_objects(self):
        """returns objects represented in this ClauseElement that should be added to the
        FROM list of a query."""
        raise NotImplementedError(repr(self))
    def _process_from_dict(self, data, asfrom):
        """given a dictionary attached to a Select object, places the appropriate
        FROM objects in the dictionary corresponding to this ClauseElement,
        and possibly removes or modifies others."""
        for f in self._get_from_objects():
            data.setdefault(f.id, f)
        if asfrom:
            data[self.id] = self
    def compare(self, other):
        """compares this ClauseElement to the given ClauseElement.
        
        Subclasses should override the default behavior, which is a straight
        identity comparison."""
        return self is other
        
    def accept_visitor(self, visitor):
        """accepts a ClauseVisitor and calls the appropriate visit_xxx method."""
        raise NotImplementedError(repr(self))

    def copy_container(self):
        """should return a copy of this ClauseElement, iff this ClauseElement contains other
        ClauseElements.  Otherwise, it should be left alone to return self.  This is used to
        create copies of expression trees that still reference the same "leaf nodes".  The
        new structure can then be restructured without affecting the original."""
        return self

    def is_selectable(self):
        """returns True if this ClauseElement is Selectable, i.e. it contains a list of Column
        objects and can be used as the target of a select statement."""
        return False

    def _find_engine(self):
        try:
            if self._engine is not None:
                return self._engine
        except AttributeError:
            pass
        for f in self._get_from_objects():
            engine = f.engine
            if engine is not None: 
                return engine
        else:
            return None
            
    engine = property(lambda s: s._find_engine())
    
    def compile(self, engine = None, parameters = None, typemap=None):
        """compiles this SQL expression using its underlying SQLEngine to produce
        a Compiled object.  If no engine can be found, an ansisql engine is used.
        bindparams is a dictionary representing the default bind parameters to be used with 
        the statement.  """
        if engine is None:
            engine = self.engine

        if engine is None:
            raise "no SQLEngine could be located within this ClauseElement."

        return engine.compile(self, parameters=parameters, typemap=typemap)

    def __str__(self):
        e = self.engine
        if e is None:
            import sqlalchemy.ansisql as ansisql
            e = ansisql.engine()
        return str(self.compile(e))
        
    def execute(self, *multiparams, **params):
        """compiles and executes this SQL expression using its underlying SQLEngine. the
        given **params are used as bind parameters when compiling and executing the
        expression. the DBAPI cursor object is returned."""
        e = self.engine
        if len(multiparams):
            bindparams = multiparams[0]
        else:
            bindparams = params
        c = self.compile(e, parameters=bindparams)
        return c.execute(*multiparams, **params)

    def scalar(self, *multiparams, **params):
        """executes this SQL expression via the execute() method, then 
        returns the first column of the first row.  Useful for executing functions,
        sequences, rowcounts, etc."""
        # we are still going off the assumption that fetching only the first row
        # in a result set is not performance-wise any different than specifying limit=1
        # else we'd have to construct a copy of the select() object with the limit
        # installed (else if we change the existing select, not threadsafe)
        row = self.execute(*multiparams, **params).fetchone()
        if row is not None:
            return row[0]
        else:
            return None

    def __and__(self, other):
        return and_(self, other)
    def __or__(self, other):
        return or_(self, other)
    def __invert__(self):
        return not_(self)

class CompareMixin(object):
    def __lt__(self, other):
        return self._compare('<', other)
    def __le__(self, other):
        return self._compare('<=', other)
    def __eq__(self, other):
        return self._compare('=', other)
    def __ne__(self, other):
        return self._compare('!=', other)
    def __gt__(self, other):
        return self._compare('>', other)
    def __ge__(self, other):
        return self._compare('>=', other)
    def like(self, other):
        return self._compare('LIKE', other)
    def in_(self, *other):
        if len(other) == 0:
            return self.__eq__(None)
        elif len(other) == 1 and not isinstance(other[0], Selectable):
            return self.__eq__(other[0])
        elif _is_literal(other[0]):
            return self._compare('IN', ClauseList(parens=True, *[self._bind_param(o) for o in other]))
        else:
            # assume *other is a list of selects.
            # so put them in a UNION.  if theres only one, you just get one SELECT 
            # statement out of it.
            return self._compare('IN', union(*other))
    def startswith(self, other):
        return self._compare('LIKE', str(other) + "%")
    def endswith(self, other):
        return self._compare('LIKE', "%" + str(other))
    def label(self, name):
        return Label(name, self)
    def op(self, operator):
        return lambda other: self._compare(operator, other)
    # and here come the math operators:
    def __add__(self, other):
        return self._operate('+', other)
    def __sub__(self, other):
        return self._operate('-', other)
    def __mul__(self, other):
        return self._operate('*', other)
    def __div__(self, other):
        return self._operate('/', other)
    def __truediv__(self, other):
        return self._operate('/', other)
    def _bind_param(self, obj):
        return BindParamClause('literal', obj, shortname=None, type=self.type)
    def _compare(self, operator, obj):
        if _is_literal(obj):
            if obj is None:
                if operator != '=':
                    raise "Only '=' operator can be used with NULL"
                return BooleanExpression(self._compare_self(), null(), 'IS')
            else:
                obj = self._bind_param(obj)

        return BooleanExpression(self._compare_self(), obj, operator, type=self._compare_type(obj))
    def _operate(self, operator, obj):
        if _is_literal(obj):
            obj = self._bind_param(obj)
        return BinaryExpression(self._compare_self(), obj, operator, type=self._compare_type(obj))
    def _compare_self(self):
        """allows ColumnImpl to return its Column object for usage in ClauseElements, all others to
        just return self"""
        return self
    def _compare_type(self, obj):
        """allows subclasses to override the type used in constructing BinaryClause objects.  Default return
        value is the type of the given object."""
        return obj.type
        
class Selectable(ClauseElement):
    """represents a column list-holding object."""

    def accept_visitor(self, visitor):
        raise NotImplementedError(repr(self))

    def is_selectable(self):
        return True

    def select(self, whereclauses = None, **params):
        return select([self], whereclauses, **params)

    def _group_parenthesized(self):
        """indicates if this Selectable requires parenthesis when grouped into a compound
        statement"""
        return True


class ColumnElement(Selectable, CompareMixin):
    """represents a column element within the list of a Selectable's columns.  Provides 
    default implementations for the things a "column" needs, including a "primary_key" flag,
    a "foreign_key" accessor, an "original" accessor which represents the ultimate column
    underlying a string of labeled/select-wrapped columns, and "columns" which returns a list
    of the single column, providing the same list-based interface as a FromClause."""
    primary_key = property(lambda self:getattr(self, '_primary_key', False))
    foreign_key = property(lambda self:getattr(self, '_foreign_key', False))
    original = property(lambda self:getattr(self, '_original', self))
    columns = property(lambda self:[self])
    def _make_proxy(self, selectable, name=None):
        """creates a new ColumnElement representing this ColumnElement as it appears in the select list
        of an enclosing selectable.  The default implementation returns a ColumnClause if a name is given,
        else just returns self.  This has various mechanics with schema.Column and sql.Label so that 
        Column objects as well as non-column objects like Function and BinaryClause can both appear in the 
        select list of an enclosing selectable."""
        if name is not None:
            co = ColumnClause(name, selectable)
            selectable.columns[name]= co
            return co
        else:
            return self

class FromClause(Selectable):
    """represents an element that can be used within the FROM clause of a SELECT statement."""
    def __init__(self, from_name = None, from_key = None):
        self.from_name = from_name
        self.id = from_key or from_name
    def _get_from_objects(self):
        # this could also be [self], at the moment it doesnt matter to the Select object
        return []
    def default_order_by(self):
        if not self.engine.default_ordering:
            return None
        elif self.oid_column is not None:
            return [self.oid_column]    
        else:
            return self.primary_key
    def hash_key(self):
        return "FromClause(%s, %s)" % (repr(self.id), repr(self.from_name))
    def accept_visitor(self, visitor): 
        visitor.visit_fromclause(self)
    def count(self, whereclause=None, **params):
        return select([func.count(1).label('count')], whereclause, from_obj=[self], **params)
    def join(self, right, *args, **kwargs):
        return Join(self, right, *args, **kwargs)
    def outerjoin(self, right, *args, **kwargs):
        return Join(self, right, isouter = True, *args, **kwargs)
    def alias(self, name=None):
        return Alias(self, name)
    def _get_col_by_original(self, column):
        """given a column which is a schema.Column object attached to a schema.Table object
        (i.e. an "original" column), return the Column object from this 
        Selectable which corresponds to that original Column, or None if this Selectable
        does not contain the column."""
        return self.original_columns.get(column.original, None)
    def _get_exported_attribute(self, name):
        try:
            return getattr(self, name)
        except AttributeError:
            self._export_columns()
            return getattr(self, name)
    columns = property(lambda s:s._get_exported_attribute('_columns'))
    c = property(lambda s:s._get_exported_attribute('_columns'))
    primary_key = property(lambda s:s._get_exported_attribute('_primary_key'))
    foreign_keys = property(lambda s:s._get_exported_attribute('_foreign_keys'))
    original_columns = property(lambda s:s._get_exported_attribute('_orig_cols'))
    
    def _export_columns(self):
        if hasattr(self, '_columns'):
            # TODO: put a mutex here ?  this is a key place for threading probs
            return
        self._columns = util.OrderedProperties()
        self._primary_key = []
        self._foreign_keys = []
        self._orig_cols = {}
        export = self._exportable_columns()
        for column in export:
            if column.is_selectable():
                for co in column.columns:
                    cp = self._proxy_column(co)
                    self._orig_cols[co.original] = cp
    def _exportable_columns(self):
        return []
    def _proxy_column(self, column):
        return column._make_proxy(self)
    
class BindParamClause(ClauseElement, CompareMixin):
    """represents a bind parameter.  public constructor is the bindparam() function."""
    def __init__(self, key, value, shortname=None, type=None):
        self.key = key
        self.value = value
        self.shortname = shortname
        self.type = type or sqltypes.NULLTYPE
    def _get_convert_type(self, engine):
        try:
            return self._converted_type
        except AttributeError:
            self._converted_type = engine.type_descriptor(self.type)
            return self._converted_type
    def accept_visitor(self, visitor):
        visitor.visit_bindparam(self)
    def _get_from_objects(self):
        return []
    def hash_key(self):
        return "BindParam(%s, %s, %s)" % (repr(self.key), repr(self.value), repr(self.shortname))
    def typeprocess(self, value, engine):
        return self._get_convert_type(engine).convert_bind_param(value, engine)
    def compare(self, other):
        """compares this BindParamClause to the given clause.
        
        Since compare() is meant to compare statement syntax, this method
        returns True if the two BindParamClauses have just the same type."""
        return isinstance(other, BindParamClause) and other.type.__class__ == self.type.__class__
            
class TextClause(ClauseElement):
    """represents literal a SQL text fragment.  public constructor is the 
    text() function.  
    
    TextClauses, since they can be anything, have no comparison operators or
    typing information.
      
    A single literal value within a compiled SQL statement is more useful 
    being specified as a bind parameter via the bindparam() method,
    since it provides more information about what it is, including an optional
    type, as well as providing comparison operations."""
    def __init__(self, text = "", engine=None, bindparams=None, typemap=None, escape=True):
        self.parens = False
        self._engine = engine
        self.id = id(self)
        self.bindparams = {}
        self.typemap = typemap
        if typemap is not None:
            for key in typemap.keys():
                typemap[key] = engine.type_descriptor(typemap[key])
        def repl(m):
            self.bindparams[m.group(1)] = bindparam(m.group(1))
            return self.engine.bindtemplate % m.group(1)
        
        if escape: 
            self.text = re.compile(r':([\w_]+)', re.S).sub(repl, text)
        else:
            self.text = text
        if bindparams is not None:
            for b in bindparams:
                self.bindparams[b.key] = b
            
    def accept_visitor(self, visitor): 
        for item in self.bindparams.values():
            item.accept_visitor(visitor)
        visitor.visit_textclause(self)
    def hash_key(self):
        return "TextClause(%s)" % repr(self.text)
    def _get_from_objects(self):
        return []

class Null(ClauseElement):
    """represents the NULL keyword in a SQL statement. public contstructor is the
    null() function."""
    def accept_visitor(self, visitor):
        visitor.visit_null(self)
    def _get_from_objects(self):
        return []
    def hash_key(self):
        return "Null"

class ClauseList(ClauseElement):
    """describes a list of clauses.  by default, is comma-separated, 
    such as a column listing."""
    def __init__(self, *clauses, **kwargs):
        self.clauses = []
        for c in clauses:
            if c is None: continue
            self.append(c)
        self.parens = kwargs.get('parens', False)
    def hash_key(self):
        return string.join([c.hash_key() for c in self.clauses], ",")
    def copy_container(self):
        clauses = [clause.copy_container() for clause in self.clauses]
        return ClauseList(parens=self.parens, *clauses)
    def append(self, clause):
        if _is_literal(clause):
            clause = TextClause(str(clause))
        self.clauses.append(clause)
    def accept_visitor(self, visitor):
        for c in self.clauses:
            c.accept_visitor(visitor)
        visitor.visit_clauselist(self)
    def _get_from_objects(self):
        f = []
        for c in self.clauses:
            f += c._get_from_objects()
        return f
    def compare(self, other):
        """compares this ClauseList to the given ClauseList, including
        a comparison of all the clause items."""
        if isinstance(other, ClauseList) and len(self.clauses) == len(other.clauses):
            for i in range(0, len(self.clauses)):
                if not self.clauses[i].compare(other.clauses[i]):
                    return False
            else:
                return True
        else:
            return False

class CompoundClause(ClauseList):
    """represents a list of clauses joined by an operator, such as AND or OR.  
    extends ClauseList to add the operator as well as a from_objects accessor to 
    help determine FROM objects in a SELECT statement."""
    def __init__(self, operator, *clauses, **kwargs):
        ClauseList.__init__(self, *clauses, **kwargs)
        self.operator = operator
    def copy_container(self):
        clauses = [clause.copy_container() for clause in self.clauses]
        return CompoundClause(self.operator, *clauses)
    def append(self, clause):
        if isinstance(clause, CompoundClause):
            clause.parens = True
        ClauseList.append(self, clause)
    def accept_visitor(self, visitor):
        for c in self.clauses:
            c.accept_visitor(visitor)
        visitor.visit_compound(self)
    def _get_from_objects(self):
        f = []
        for c in self.clauses:
            f += c._get_from_objects()
        return f
    def hash_key(self):
        return string.join([c.hash_key() for c in self.clauses], self.operator or " ")
    def compare(self, other):
        """compares this CompoundClause to the given item.  
        
        In addition to the regular comparison, has the special case that it 
        returns True if this CompoundClause has only one item, and that 
        item matches the given item."""
        if not isinstance(other, CompoundClause):
            if len(self.clauses) == 1:
                return self.clauses[0].compare(other)
        if ClauseList.compare(self, other):
            return self.operator == other.operator
        else:
            return False
                
class Function(ClauseList, ColumnElement):
    """describes a SQL function. extends ClauseList to provide comparison operators."""
    def __init__(self, name, *clauses, **kwargs):
        self.name = name
        self.type = kwargs.get('type', sqltypes.NULLTYPE)
        ClauseList.__init__(self, parens=True, *clauses)
    key = property(lambda self:self.name)
    def append(self, clause):
        if _is_literal(clause):
            if clause is None:
                clause = null()
            else:
                clause = BindParamClause(self.name, clause, shortname=self.name, type=None)
        self.clauses.append(clause)
    def copy_container(self):
        clauses = [clause.copy_container() for clause in self.clauses]
        return Function(self.name, type=self.type, *clauses)
    def accept_visitor(self, visitor):
        for c in self.clauses:
            c.accept_visitor(visitor)
        visitor.visit_function(self)
    def _bind_param(self, obj):
        return BindParamClause(self.name, obj, shortname=self.name, type=self.type)
    def select(self):
        return select([self])
    def hash_key(self):
        return self.name + "(" + string.join([c.hash_key() for c in self.clauses], ", ") + ")"
            
class BinaryClause(ClauseElement):
    """represents two clauses with an operator in between"""
    def __init__(self, left, right, operator, type=None):
        self.left = left
        self.right = right
        self.operator = operator
        self.type = type
        self.parens = False
    def copy_container(self):
        return BinaryClause(self.left.copy_container(), self.right.copy_container(), self.operator)
    def _get_from_objects(self):
        return self.left._get_from_objects() + self.right._get_from_objects()
    def hash_key(self):
        return self.left.hash_key() + (self.operator or " ") + self.right.hash_key()
    def accept_visitor(self, visitor):
        self.left.accept_visitor(visitor)
        self.right.accept_visitor(visitor)
        visitor.visit_binary(self)
    def swap(self):
        c = self.left
        self.left = self.right
        self.right = c
    def compare(self, other):
        """compares this BinaryClause against the given BinaryClause."""
        return (
            isinstance(other, BinaryClause) and self.operator == other.operator and 
            self.left.compare(other.left) and self.right.compare(other.right)
        )

class BooleanExpression(BinaryClause):
    """represents a boolean expression, which is only useable in WHERE criterion."""
    pass
class BinaryExpression(BinaryClause, ColumnElement):
    """represents a binary expression, which can be in a WHERE criterion or in the column list 
    of a SELECT.  By adding "ColumnElement" to its inherited list, it becomes a Selectable
    unit which can be placed in the column list of a SELECT."""
    pass
    
        
class Join(FromClause):
    def __init__(self, left, right, onclause=None, isouter = False):
        self.left = left
        self.right = right
        self.id = self.left.id + "_" + self.right.id

        # TODO: if no onclause, do NATURAL JOIN
        if onclause is None:
            self.onclause = self._match_primaries(left, right)
        else:
            self.onclause = onclause
        self.isouter = isouter
        self.oid_column = self.left.oid_column
    def _exportable_columns(self):
        return [c for c in self.left.columns] + [c for c in self.right.columns]
    def _proxy_column(self, column):
        self._columns[column.table.name + "_" + column.key] = column
        if column.primary_key:
            self._primary_key.append(column)
        if column.foreign_key:
            self._foreign_keys.append(column.foreign_key)
        return column
    def _match_primaries(self, primary, secondary):
        crit = []
        for fk in secondary.foreign_keys:
            if fk.references(primary):
                crit.append(primary._get_col_by_original(fk.column) == fk.parent)
                self.foreignkey = fk.parent
        if primary is not secondary:
            for fk in primary.foreign_keys:
                if fk.references(secondary):
                    crit.append(secondary._get_col_by_original(fk.column) == fk.parent)
                    self.foreignkey = fk.parent
        if len(crit) == 0:
            raise "Cant find any foreign key relationships between '%s' (%s) and '%s' (%s)" % (primary.name, repr(primary), secondary.name, repr(secondary))
        elif len(crit) == 1:
            return (crit[0])
        else:
            return and_(*crit)
            
    def _group_parenthesized(self):
        """indicates if this Selectable requires parenthesis when grouped into a compound
        statement"""
        return True

    def hash_key(self):
        return "Join(%s, %s, %s, %s)" % (repr(self.left.hash_key()), repr(self.right.hash_key()), repr(self.onclause.hash_key()), repr(self.isouter))

    def select(self, whereclauses = None, **params):
        return select([self.left, self.right], whereclauses, from_obj=[self], **params)

    def accept_visitor(self, visitor):
        self.left.accept_visitor(visitor)
        self.right.accept_visitor(visitor)
        self.onclause.accept_visitor(visitor)
        visitor.visit_join(self)

    engine = property(lambda s:s.left.engine or s.right.engine)

    class JoinMarker(FromClause):
        def __init__(self, id, join):
            FromClause.__init__(self, from_key=id)
            self.join = join
        def _exportable_columns(self):
            return []
                
    def _process_from_dict(self, data, asfrom):
        for f in self.onclause._get_from_objects():
            data[f.id] = f
        for f in self.left._get_from_objects() + self.right._get_from_objects():
            # mark the object as a "blank" "from" that wont be printed
            data[f.id] = Join.JoinMarker(f.id, self)
        # a JOIN always impacts the final FROM list of a select statement
        data[self.id] = self
        
    def _get_from_objects(self):
        return [self] + self.onclause._get_from_objects() + self.left._get_from_objects() + self.right._get_from_objects()
        
class Alias(FromClause):
    def __init__(self, selectable, alias = None):
        while isinstance(selectable, Alias):
            selectable = selectable.selectable
        self.selectable = selectable
        if alias is None:
            n = getattr(selectable, 'name')
            if n is None:
                n = 'anon'
            alias = n + "_" + hex(random.randint(0, 65535))[2:]
        self.name = alias
        self.id = self.name
        self.count = 0
        if self.selectable.oid_column is not None:
            self.oid_column = self.selectable.oid_column._make_proxy(self)
        else:
            self.oid_column = None

    def _exportable_columns(self):
        return self.selectable.columns

    def hash_key(self):
        return "Alias(%s, %s)" % (self.selectable.hash_key(), repr(self.name))

    def accept_visitor(self, visitor):
        self.selectable.accept_visitor(visitor)
        visitor.visit_alias(self)

    def _get_from_objects(self):
        return [self]

    def _group_parenthesized(self):
        return False
        
    engine = property(lambda s: s.selectable.engine)

    
class Label(ColumnElement):
    def __init__(self, name, obj):
        self.name = name
        while isinstance(obj, Label):
            obj = obj.obj
        self.obj = obj
        obj.parens=True
    key = property(lambda s: s.name)
    _label = property(lambda s: s.name)
    original = property(lambda s:s.obj.original)
    def accept_visitor(self, visitor):
        self.obj.accept_visitor(visitor)
        visitor.visit_label(self)
    def _get_from_objects(self):
        return self.obj._get_from_objects()
    def _make_proxy(self, selectable, name = None):
        return self.obj._make_proxy(selectable, name=self.name)
        
    def hash_key(self):
        return "Label(%s, %s)" % (self.name, self.obj.hash_key())
     
class ColumnClause(ColumnElement):
    """represents a textual column clause in a SQL statement. allows the creation
    of an additional ad-hoc column that is compiled against a particular table."""

    def __init__(self, text, selectable=None):
        self.text = text
        self.table = selectable
        self.type = sqltypes.NullTypeEngine()

    name = property(lambda self:self.text)
    key = property(lambda self:self.text)
    _label = property(lambda self:self.text)
    
    def accept_visitor(self, visitor): 
        visitor.visit_columnclause(self)

    def hash_key(self):
        if self.table is not None:
            return "ColumnClause(%s, %s)" % (self.text, util.hash_key(self.table))
        else:
            return "ColumnClause(%s)" % self.text

    def _get_from_objects(self):
        return []

    def _bind_param(self, obj):
        if self.table.name is None:
            return BindParamClause(self.text, obj, shortname=self.text, type=self.type)
        else:
            return BindParamClause(self.table.name + "_" + self.text, obj, shortname = self.text, type=self.type)
    def _make_proxy(self, selectable, name = None):
        c = ColumnClause(name or self.text, selectable)
        selectable.columns[c.key] = c
        return c

class ColumnImpl(ColumnElement):
    """gets attached to a schema.Column object."""
    
    def __init__(self, column):
        self.column = column
        self.name = column.name
        
        if column.table.name:
            self._label = column.table.name + "_" + self.column.name
        else:
            self._label = self.column.name

    engine = property(lambda s: s.column.engine)
    default_label = property(lambda s:s._label)
    original = property(lambda self:self.column)
    columns = property(lambda self:[self.column])
    
    def label(self, name):
        return Label(name, self.column)
        
    def copy_container(self):
        return self.column

    def compare(self, other):
        """compares this ColumnImpl's column to the other given Column"""
        return self.column is other
        
    def _group_parenthesized(self):
        return False
        
    def _get_from_objects(self):
        return [self.column.table]
    
    def _bind_param(self, obj):
        if self.column.table.name is None:
            return BindParamClause(self.name, obj, shortname = self.name, type = self.column.type)
        else:
            return BindParamClause(self.column.table.name + "_" + self.name, obj, shortname = self.name, type = self.column.type)
    def _compare_self(self):
        """allows ColumnImpl to return its Column object for usage in ClauseElements, all others to
        just return self"""
        return self.column
    def _compare_type(self, obj):
        return self.column.type
        
    def compile(self, engine = None, parameters = None, typemap=None):
        if engine is None:
            engine = self.engine
        if engine is None:
            raise "no SQLEngine could be located within this ClauseElement."
        return engine.compile(self.column, parameters=parameters, typemap=typemap)

class TableImpl(FromClause):
    """attached to a schema.Table to provide it with a Selectable interface
    as well as other functions
    """

    def __init__(self, table):
        self.table = table
        self.id = self.table.name

    def _oid_col(self):
        # OID remains a little hackish so far
        if not hasattr(self, '_oid_column'):
            if self.table.engine.oid_column_name() is not None:
                self._oid_column = schema.Column(self.table.engine.oid_column_name(), sqltypes.Integer, hidden=True)
                self._oid_column._set_parent(self.table)
            else:
                self._oid_column = None
        return self._oid_column

    def _orig_columns(self):
        try:
            return self._orig_cols
        except AttributeError:
            self._orig_cols= {}
            for c in self.columns:
                self._orig_cols[c.original] = c
            return self._orig_cols
            
    oid_column = property(_oid_col)
    engine = property(lambda s: s.table.engine)
    columns = property(lambda self: self.table.columns)
    primary_key = property(lambda self:self.table.primary_key)
    foreign_keys = property(lambda self:self.table.foreign_keys)
    original_columns = property(_orig_columns)

    def _exportable_columns(self):
        raise NotImplementedError()
        
    def _group_parenthesized(self):
        return False

    def _process_from_dict(self, data, asfrom):
        for f in self._get_from_objects():
            data.setdefault(f.id, f)
        if asfrom:
            data[self.id] = self.table
    def count(self, whereclause=None, **params):
        return select([func.count(1).label('count')], whereclause, from_obj=[self.table], **params)
    def join(self, right, *args, **kwargs):
        return Join(self.table, right, *args, **kwargs)
    def outerjoin(self, right, *args, **kwargs):
        return Join(self.table, right, isouter = True, *args, **kwargs)
    def alias(self, name=None):
        return Alias(self.table, name)
    def select(self, whereclause = None, **params):
        return select([self.table], whereclause, **params)
    def insert(self, values = None):
        return insert(self.table, values=values)
    def update(self, whereclause = None, values = None):
        return update(self.table, whereclause, values)
    def delete(self, whereclause = None):
        return delete(self.table, whereclause)
    def create(self, **params):
        self.table.engine.create(self.table)
    def drop(self, **params):
        self.table.engine.drop(self.table)
    def _get_from_objects(self):
        return [self.table]

class SelectBaseMixin(object):
    """base class for Select and CompoundSelects"""
    def order_by(self, *clauses):
        self._append_clause('order_by_clause', "ORDER BY", *clauses)
    def group_by(self, *clauses):
        self._append_clause('group_by_clause', "GROUP BY", *clauses)
    def _append_clause(self, attribute, prefix, *clauses):
        if len(clauses) == 1 and clauses[0] is None:
            try:
                delattr(self, attribute)
            except AttributeError:
                pass
            return
        if not hasattr(self, attribute):
            l = ClauseList(*clauses)
            setattr(self, attribute, l)
            self.append_clause(prefix, l)
        else:
            getattr(self, attribute).clauses  += clauses
    def append_clause(self, keyword, clause):
        if type(clause) == str:
            clause = TextClause(clause)
        self.clauses.append((keyword, clause))
    def select(self, whereclauses = None, **params):
        return select([self], whereclauses, **params)
    def _get_from_objects(self):
        if self.is_where:
            return []
        else:
            return [self]
            
class CompoundSelect(SelectBaseMixin, FromClause):
    def __init__(self, keyword, *selects, **kwargs):
        self.id = "Compound(%d)" % id(self)
        self.keyword = keyword
        self.selects = selects
        self.use_labels = kwargs.pop('use_labels', False)
        self.oid_column = selects[0].oid_column
        for s in self.selects:
            s.group_by(None)
            s.order_by(None)
        self.clauses = []
        group_by = kwargs.get('group_by', None)
        if group_by:
            self.group_by(*group_by)
        order_by = kwargs.get('order_by', None)
        if order_by:
            self.order_by(*order_by)
    def hash_key(self):
        return "CompoundSelect(%s)" % string.join(
            [util.hash_key(s) for s in self.selects] + 
            ["%s=%s" % (k, repr(getattr(self, k))) for k in ['use_labels', 'keyword']],
            ",")
    def _exportable_columns(self):
        return self.selects[0].columns
    def _proxy_column(self, column):
        self._columns[column.key] = column
        if column.primary_key:
            self._primary_key.append(column)
        if column.foreign_key:
            self._foreign_keys.append(column)
    def accept_visitor(self, visitor):
        for tup in self.clauses:
            tup[1].accept_visitor(visitor)
        for s in self.selects:
            s.accept_visitor(visitor)
        visitor.visit_compound_select(self)
    def _find_engine(self):
        for s in self.selects:
            e = s._find_engine()
            if e:
                return e
        else:
            return None
       
class Select(SelectBaseMixin, FromClause):
    """represents a SELECT statement, with appendable clauses, as well as 
    the ability to execute itself and return a result set."""
    def __init__(self, columns=None, whereclause = None, from_obj = [], order_by = None, group_by=None, having=None, use_labels = False, distinct=False, engine = None, limit=None, offset=None):
        self._froms = util.OrderedDict()
        self.use_labels = use_labels
        self.id = "Select(%d)" % id(self)
        self.name = None
        self.whereclause = None
        self.having = None
        self._engine = engine
        self.oid_column = None
        self.limit = limit
        self.offset = offset
        
        # indicates if this select statement is a subquery inside another query
        self.issubquery = False
        # indicates if this select statement is a subquery as a criterion
        # inside of a WHERE clause
        self.is_where = False
        self.clauses = []

        self.distinct = distinct
        self._text = None
        self._raw_columns = []
        self._correlated = None
        self._correlator = Select.CorrelatedVisitor(self, False)
        self._wherecorrelator = Select.CorrelatedVisitor(self, True)
        
        if columns is not None:
            for c in columns:
                self.append_column(c)
            
        if whereclause is not None:
            self.append_whereclause(whereclause)
        if having is not None:
            self.append_having(having)
            
        for f in from_obj:
            self.append_from(f)

        if group_by:
            self.group_by(*group_by)
        if order_by:
            self.order_by(*order_by)
            
    class CorrelatedVisitor(ClauseVisitor):
        """visits a clause, locates any Select clauses, and tells them that they should
        correlate their FROM list to that of their parent."""
        def __init__(self, select, is_where):
            self.select = select
            self.is_where = is_where
        def visit_compound_select(self, cs):
            self.visit_select(cs)
        def visit_select(self, select):
            if select is self.select:
                return
            select.is_where = self.is_where
            select.issubquery = True
            if getattr(select, '_correlated', None) is None:
                select._correlated = self.select._froms

    def append_column(self, column):
        if _is_literal(column):
            column = ColumnClause(str(column), self)

        self._raw_columns.append(column)

        for f in column._get_from_objects():
            f.accept_visitor(self._correlator)
        column._process_from_dict(self._froms, False)
        
    def _exportable_columns(self):
        return self._raw_columns
    def _proxy_column(self, column):
        if self.use_labels:
            return column._make_proxy(self, name=column._label)
        else:
            return column._make_proxy(self, name=column.key)
            
    def append_whereclause(self, whereclause):
        self._append_condition('whereclause', whereclause)
    def append_having(self, having):
        self._append_condition('having', having)
    def _append_condition(self, attribute, condition):
        if type(condition) == str:
            condition = TextClause(condition)
        condition.accept_visitor(self._wherecorrelator)
        condition._process_from_dict(self._froms, False)
        if getattr(self, attribute) is not None:
            setattr(self, attribute, and_(getattr(self, attribute), condition))
        else:
            setattr(self, attribute, condition)

    _hash_recursion = util.RecursionStack()
    
    def hash_key(self):
        # selects call alot of stuff so we do some "recursion checking"
        # to eliminate loops
        if Select._hash_recursion.push(self):
            return "recursive_select()"
        try:
            return "Select(%s)" % string.join(
                [
                    "columns=" + string.join([util.hash_key(c) for c in self._raw_columns],','),
                    "where=" + util.hash_key(self.whereclause),
                    "from=" + string.join([util.hash_key(f) for f in self.froms],','),
                    "having=" + util.hash_key(self.having),
                    "clauses=" + string.join([util.hash_key(c) for c in self.clauses], ',')
                ] + ["%s=%s" % (k, repr(getattr(self, k))) for k in ['use_labels', 'distinct', 'limit', 'offset']], ","
            ) 
        finally:
            Select._hash_recursion.pop(self)
        
    def clear_from(self, id):
        self.append_from(FromClause(from_name = None, from_key = id))
        
    def append_from(self, fromclause):
        if type(fromclause) == str:
            fromclause = FromClause(from_name = fromclause)

        fromclause.accept_visitor(self._correlator)
        fromclause._process_from_dict(self._froms, True)

    def _get_froms(self):
        return [f for f in self._froms.values() if self._correlated is None or not self._correlated.has_key(f.id)]
    froms = property(lambda s: s._get_froms())

    def accept_visitor(self, visitor):
        # TODO: add contextual visit_ methods
        # visit_select_whereclause, visit_select_froms, visit_select_orderby, etc.
        # which will allow the compiler to set contextual flags before traversing 
        # into each thing.  
        for f in self._get_froms():
            f.accept_visitor(visitor)
        if self.whereclause is not None:
            self.whereclause.accept_visitor(visitor)
        if self.having is not None:
            self.having.accept_visitor(visitor)
        for tup in self.clauses:
            tup[1].accept_visitor(visitor)
        visitor.visit_select(self)
    
    def union(self, other, **kwargs):
        return union(self, other, **kwargs)
    def union_all(self, other, **kwargs):
        return union_all(self, other, **kwargs)

#    def scalar(self, *multiparams, **params):
        # need to set limit=1, but only in this thread.
        # we probably need to make a copy of the select().  this
        # is expensive.  I think cursor.fetchone(), then discard remaining results 
        # should be fine with most DBs
        # for now use base scalar() method
        
    def _find_engine(self):
        """tries to return a SQLEngine, either explicitly set in this object, or searched
        within the from clauses for one"""
        
        if self._engine is not None:
            return self._engine
        for f in self._froms.values():
            e = f.engine
            if e is not None: 
                self._engine = e
                return e
        return None

class UpdateBase(ClauseElement):
    """forms the base for INSERT, UPDATE, and DELETE statements."""
    
    def hash_key(self):
        return str(id(self))
        
    def _process_colparams(self, parameters):
        """receives the "values" of an INSERT or UPDATE statement and constructs
        appropriate ind parameters."""
        if parameters is None:
            return None

        if isinstance(parameters, list) or isinstance(parameters, tuple):
            pp = {}
            i = 0
            for c in self.table.c:
                pp[c.key] = parameters[i]
                i +=1
            parameters = pp
            
        for key in parameters.keys():
            value = parameters[key]
            if isinstance(value, Select):
                value.clear_from(self.table.id)
            elif _is_literal(value):
                if _is_literal(key):
                    col = self.table.c[key]
                else:
                    col = key
                try:
                    parameters[key] = bindparam(col, value)
                except KeyError:
                    del parameters[key]
        return parameters
        

class Insert(UpdateBase):
    def __init__(self, table, values=None, **params):
        self.table = table
        self.select = None
        self.parameters = self._process_colparams(values)
        self._engine = self.table.engine
        
    def accept_visitor(self, visitor):
        if self.select is not None:
            self.select.accept_visitor(visitor)

        visitor.visit_insert(self)

class Update(UpdateBase):
    def __init__(self, table, whereclause, values=None, **params):
        self.table = table
        self.whereclause = whereclause
        self.parameters = self._process_colparams(values)
        self._engine = self.table.engine

    def accept_visitor(self, visitor):
        if self.whereclause is not None:
            self.whereclause.accept_visitor(visitor)
        visitor.visit_update(self)

class Delete(UpdateBase):
    def __init__(self, table, whereclause, **params):
        self.table = table
        self.whereclause = whereclause
        self._engine = self.table.engine

    def accept_visitor(self, visitor):
        if self.whereclause is not None:
            self.whereclause.accept_visitor(visitor)
        visitor.visit_delete(self)

        
