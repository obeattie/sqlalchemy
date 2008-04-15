# mapper/util.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import new
from sqlalchemy import sql, util, exceptions
from sqlalchemy.sql.util import row_adapter as create_row_adapter
from sqlalchemy.sql import visitors, expression, util as sql_util, operators
from sqlalchemy.orm.interfaces import MapperExtension, EXT_CONTINUE, PropComparator, MapperProperty
from sqlalchemy.orm import attributes

all_cascades = util.Set(["delete", "delete-orphan", "all", "merge",
                         "expunge", "save-update", "refresh-expire", "none"])

class CascadeOptions(object):
    """Keeps track of the options sent to relation().cascade"""

    def __init__(self, arg=""):
        values = util.Set([c.strip() for c in arg.split(',')])
        self.delete_orphan = "delete-orphan" in values
        self.delete = "delete" in values or "all" in values
        self.save_update = "save-update" in values or "all" in values
        self.merge = "merge" in values or "all" in values
        self.expunge = "expunge" in values or "all" in values
        self.refresh_expire = "refresh-expire" in values or "all" in values
        for x in values:
            if x not in all_cascades:
                raise exceptions.ArgumentError("Invalid cascade option '%s'" % x)

    def __contains__(self, item):
        return getattr(self, item.replace("-", "_"), False)

    def __repr__(self):
        return "CascadeOptions(arg=%s)" % repr(",".join(
            [x for x in ['delete', 'save_update', 'merge', 'expunge',
                         'delete_orphan', 'refresh-expire']
             if getattr(self, x, False) is True]))

def polymorphic_union(table_map, typecolname, aliasname='p_union'):
    """Create a ``UNION`` statement used by a polymorphic mapper.

    See the `SQLAlchemy` advanced mapping docs for an example of how
    this is used.
    """

    colnames = util.Set()
    colnamemaps = {}
    types = {}
    for key in table_map.keys():
        table = table_map[key]

        # mysql doesnt like selecting from a select; make it an alias of the select
        if isinstance(table, sql.Select):
            table = table.alias()
            table_map[key] = table

        m = {}
        for c in table.c:
            colnames.add(c.name)
            m[c.name] = c
            types[c.name] = c.type
        colnamemaps[table] = m

    def col(name, table):
        try:
            return colnamemaps[table][name]
        except KeyError:
            return sql.cast(sql.null(), types[name]).label(name)

    result = []
    for type, table in table_map.iteritems():
        if typecolname is not None:
            result.append(sql.select([col(name, table) for name in colnames] +
                                     [sql.literal_column("'%s'" % type).label(typecolname)],
                                     from_obj=[table]))
        else:
            result.append(sql.select([col(name, table) for name in colnames], from_obj=[table]))
    return sql.union_all(*result).alias(aliasname)


class ExtensionCarrier(object):
    """stores a collection of MapperExtension objects.
    
    allows an extension methods to be called on contained MapperExtensions
    in the order they were added to this object.  Also includes a 'methods' dictionary
    accessor which allows for a quick check if a particular method
    is overridden on any contained MapperExtensions.
    """
    
    def __init__(self, _elements=None):
        self.methods = {}
        if _elements is not None:
            self.__elements = [self.__inspect(e) for e in _elements]
        else:
            self.__elements = []
        
    def copy(self):
        return ExtensionCarrier(list(self.__elements))
        
    def __iter__(self):
        return iter(self.__elements)

    def insert(self, extension):
        """Insert a MapperExtension at the beginning of this ExtensionCarrier's list."""

        self.__elements.insert(0, self.__inspect(extension))

    def append(self, extension):
        """Append a MapperExtension at the end of this ExtensionCarrier's list."""

        self.__elements.append(self.__inspect(extension))

    def __inspect(self, extension):
        for meth in MapperExtension.__dict__.keys():
            if meth not in self.methods and hasattr(extension, meth) and getattr(extension, meth) is not getattr(MapperExtension, meth):
                self.methods[meth] = self.__create_do(meth)
        return extension
           
    def __create_do(self, funcname):
        def _do(*args, **kwargs):
            for elem in self.__elements:
                ret = getattr(elem, funcname)(*args, **kwargs)
                if ret is not EXT_CONTINUE:
                    return ret
            else:
                return EXT_CONTINUE

        try:
            _do.__name__ = funcname
        except:
            # cant set __name__ in py 2.3 
            pass
        return _do
    
    def _pass(self, *args, **kwargs):
        return EXT_CONTINUE
        
    def __getattr__(self, key):
        return self.methods.get(key, self._pass)

class AliasedClauses(object):
    """Creates aliases of a mapped tables for usage in ORM queries, and provides expression adaptation."""

    def __init__(self, alias, equivalents=None, chain_to=None):
        if _is_aliased_class(alias):
            self.aliased_class = alias
        else:
            self.aliased_class = None
        self.selectable = _orm_selectable(alias)
        self.equivalents = equivalents
        self.chain_to = chain_to
        self.local_adapter = sql_util.ClauseAdapter(self.selectable, equivalents=equivalents)
        self.adapter = visitors.VisitorContainer(self._iterate_adapters, self.local_adapter.__traverse_options__)
        self.__wrap = None
        self.row_decorator = self._create_row_adapter()
        
    def wrap(self, aliasedclauses):
        ac = AliasedClauses.__new__(AliasedClauses)
        ac.__dict__ = self.__dict__.copy()
        ac.__wrap = aliasedclauses
        ac.row_decorator = ac._create_row_adapter()
        return ac

    def _iterate(self):
        a = self
        while a:
            yield a
            a = a.chain_to
    
    def _iterate_adapters(self):
        a = self
        while a:
            yield a.local_adapter
            a = a.chain_to
            
    def remove_link(self, link):
        head = self
        
        if link is head:
            head = head.chain_to
            link.chain_to = None
        else:
            a = head
            while a and a.chain_to is not link:
                a = a.chain_to
            if a:
                a.chain_to = link.chain_to
                link.chain_to = None

        return head
    
    def tail(self):
        for a in self._iterate():
            if not a.chain_to:
                return a
    tail = property(tail)
    
    def append(self, link):
        self.tail.chain_to = link
    
    def prepend(self, link):
        link.chain_to = self
        return link
        
    def aliased_column(self, column):
        if self.__wrap:
            column = self.__wrap.aliased_column(column)
            
        conv = self.selectable.corresponding_column(column)
        if conv:
            return conv
        
        # process column-level subqueries    
        aliased_column = self.adapter.traverse(column, clone=True)

        # add to row decorator explicitly
        # TODO: refactor
        self.row_decorator({}).map[column] = aliased_column
        return aliased_column

    def adapt_clause(self, clause):
        if self.__wrap:
            clause = self.__wrap.adapt_clause(clause)
            
        return self.adapter.traverse(clause, clone=True)
    
    def adapt_list(self, clauses):
        if self.__wrap:
            clauses = self.clauses.adapt_list(clauses)
            
        return self.adapter.copy_and_process(clauses)
        
    def _create_row_adapter(self):
        if self.__wrap:
            adapter = self.__wrap.row_decorator
            return adapter.wrap(create_row_adapter(self.selectable, equivalent_columns=self.equivalents))
        else:
            return create_row_adapter(self.selectable, equivalent_columns=self.equivalents)


class AliasedClass(object):
    def __init__(self, cls, alias=None):
        self.mapper = _class_to_mapper(cls) # urgh ?
        self.__target = self.mapper.class_
        alias = alias or self.mapper._with_polymorphic_selectable().alias()
        self.adapter = sql_util.ClauseAdapter(alias, equivalents=self.mapper._equivalent_columns)
        self.alias = alias
        self.__name__ = 'AliasedClass_' + str(self.__target)
    
    def __adapt_prop(self, prop):
        existing = getattr(self.__target, prop.key)
        comparator = AliasedComparator(self, self.adapter, existing.comparator)
        queryattr = attributes.QueryableAttribute(
            existing.impl, parententity=self, comparator=comparator)
        setattr(self, prop.key, queryattr)
        return queryattr
        
    def __getattr__(self, key):
        prop = self.mapper._get_property(key, raiseerr=False)
        if prop:
            return self.__adapt_prop(prop)
            
        for base in self.__target.__mro__:
            try:
                attr = object.__getattribute__(base, key)
            except AttributeError:
                continue
            else:
                break
        else:
            raise AttributeError(key)

        if hasattr(attr, 'func_code'):
            is_method = getattr(self.__target, key, None)
            if is_method and is_method.im_self is not None:
                return new.instancemethod(attr.im_func, self, self)
            else:
                return None
        elif hasattr(attr, '__get__'):
            return attr.__get__(None, self)
        else:
            return attr

    def __repr__(self):
        return '<AliasedClass at 0x%x; %s>' % (
            id(self), self.__target.__name__)

class AliasedComparator(PropComparator):
    def __init__(self, aliasedclass, adapter, comparator):
        self.aliasedclass = aliasedclass
        self.comparator = comparator
        self.adapter = adapter
        
    def clause_element(self):
        # this is a HACK since some ProperrtyLoader comparators return the mapped table,
        # using the adapter to "traverse" it is not the right approach
        # (its probably not for ColumnLoaders either)
        ca = self.comparator.clause_element()
        if ca is self.aliasedclass.mapper.mapped_table:
            return self.adapter.selectable
        else:
            return self.adapter.traverse(ca, clone=True)
    clause_element = util.cache_decorator(clause_element)
    
    def operate(self, op, *other, **kwargs):
        return self.adapter.traverse(self.comparator.operate(op, *other, **kwargs), clone=True)

    def reverse_operate(self, op, other, **kwargs):
        return self.adapter.traverse(self.comparator.reverse_operate(op, *other, **kwargs), clone=True)

def _orm_columns(entity):
    if _is_aliased_class(entity):
        return [c for c in entity.alias.c]
    elif _is_mapped_class(entity):
        return [c for c in _class_to_mapper(entity)._with_polymorphic_selectable().c]
    elif isinstance(entity, expression.Selectable):
        return [c for c in entity.c]
    else:
        return [entity]
        
def _orm_selectable(selectable, entity_name=None):
    if _is_aliased_class(selectable):
        return selectable.alias
    elif _is_mapped_class(selectable):
        return _class_to_mapper(selectable, entity_name)._with_polymorphic_selectable()
    else:
        return expression._selectable(selectable)
        
_literal_as_column = expression._literal_as_column
def _orm_literal_as_column(c):
    if _is_aliased_class(c):
        return c.alias
    elif _is_mapped_class(c):
        return _class_to_mapper(c)._with_polymorphic_selectable()
    else:
        return _literal_as_column(c)
# uncommenting this allows a mapped class or AliasedClass to be used i.e. select([MyClass])
#expression._literal_as_column = _orm_literal_as_column  

class _ORMJoin(expression.Join):

    __visit_name__ = expression.Join.__visit_name__
    
    def __init__(self, left, right, onclause=None, isouter=False):
        if hasattr(left, '_orm_mappers'):
            left_mapper = left._orm_mappers[1]
            adapt_from = left.right
            
        elif _is_mapped_class(left):
            left_mapper = _class_to_mapper(left)
            if _is_aliased_class(left):
                adapt_from = left.alias
            else:
                adapt_from = None
        else:
            adapt_from = left
            left_mapper = None
        
        if _is_mapped_class(right):
            right_mapper = _class_to_mapper(right)
        else:
            right_mapper = None
        
        if left_mapper or right_mapper:
            self._orm_mappers = (left_mapper, right_mapper)
            
            if isinstance(onclause, basestring):
                prop = left_mapper.get_property(onclause)
            elif isinstance(onclause, attributes.QueryableAttribute):
                adapt_from = onclause.clause_element()
                prop = onclause.property
            elif isinstance(onclause, MapperProperty):
                prop = onclause
            else:
                prop = None

            if prop:
                if _is_aliased_class(right):
                    adapt_to = right.alias
                else:
                    adapt_to = None

                pj, sj, source, dest, secondary, target_adapter = prop._create_joins(source_selectable=adapt_from, dest_selectable=adapt_to, source_polymorphic=True, dest_polymorphic=True)

                if sj:
                    left = sql.join(_orm_selectable(left), secondary, pj, isouter)
                    onclause = sj
                else:
                    onclause = pj
                
                self._target_adapter = target_adapter
                
        expression.Join.__init__(self, _orm_selectable(left), _orm_selectable(right), onclause, isouter)

    def join(self, right, onclause=None, isouter=False):
        return _ORMJoin(self, right, onclause, isouter)

    def outerjoin(self, right, onclause=None):
        return _ORMJoin(self, right, onclause, True)

def join(left, right, onclause=None, isouter=False):
    return _ORMJoin(left, right, onclause, isouter)

def outerjoin(left, right, onclause=None):
    return _ORMJoin(left, right, onclause, True)

def with_parent(instance, prop):
    """Return criterion which selects instances with a given parent.
    
    instance
      a parent instance, which should be persistent or detached.

     property
       a class-attached descriptor, MapperProperty or string property name
       attached to the parent instance.  

     \**kwargs
       all extra keyword arguments are propagated to the constructor of
       Query.

    """
    if isinstance(prop, basestring):
        mapper = object_mapper(instance)
        prop = mapper.get_property(prop, resolve_synonyms=True)
    elif isinstance(prop, attributes.QueryableAttribute):
        prop = prop.property

    return prop.compare(operators.eq, instance, value_is_parent=True)

def has_identity(object):
    return hasattr(object, '_instance_key')

def _state_has_identity(state):
    return '_instance_key' in state.dict

def _is_mapped_class(cls):
    return hasattr(cls, '_class_state')

def _is_aliased_class(obj):
    return isinstance(obj, AliasedClass)
    
def has_mapper(object):
    """Return True if the given object has had a mapper association
    set up, either through loading, or via insertion in a session.
    """

    return hasattr(object, '_entity_name')

def _state_mapper(state, entity_name=None):
    return state.class_._class_state.mappers[state.dict.get('_entity_name', entity_name)]

def object_mapper(object, entity_name=None, raiseerror=True):
    """Given an object, return the primary Mapper associated with the object instance.

        object
            The object instance.

        entity_name
            Entity name of the mapper to retrieve, if the given instance is
            transient.  Otherwise uses the entity name already associated
            with the instance.

        raiseerror
            Defaults to True: raise an ``InvalidRequestError`` if no mapper can
            be located.  If False, return None.

    """

    try:
        mapper = object.__class__._class_state.mappers[getattr(object, '_entity_name', entity_name)]
    except (KeyError, AttributeError):
        if raiseerror:
            raise exceptions.InvalidRequestError("Class '%s' entity name '%s' has no mapper associated with it" % (object.__class__.__name__, getattr(object, '_entity_name', entity_name)))
        else:
            return None
    return mapper

def class_mapper(class_, entity_name=None, compile=True, raiseerror=True):
    """Given a class and optional entity_name, return the primary Mapper associated with the key.

    If no mapper can be located, raises ``InvalidRequestError``.
    """

    try:
        mapper = class_._class_state.mappers[entity_name]
    except (KeyError, AttributeError):
        if raiseerror:
            raise exceptions.InvalidRequestError("Class '%s' entity name '%s' has no mapper associated with it" % (class_.__name__, entity_name))
        else:
            return None
    if compile:
        return mapper.compile()
    else:
        return mapper

def _class_to_mapper(class_or_mapper, entity_name=None, compile=True):
    if isinstance(class_or_mapper, type):
        return class_mapper(class_or_mapper, entity_name=entity_name, compile=compile)
    elif isinstance(class_or_mapper, AliasedClass):
        return class_or_mapper.mapper
    else:
        if compile:
            return class_or_mapper.compile()
        else:
            return class_or_mapper

def instance_str(instance):
    """Return a string describing an instance."""

    return instance.__class__.__name__ + "@" + hex(id(instance))

def state_str(state):
    """Return a string describing an instance."""
    if state is None:
        return "None"
    else:
        return state.class_.__name__ + "@" + hex(id(state.obj()))

def attribute_str(instance, attribute):
    return instance_str(instance) + "." + attribute

def identity_equal(a, b):
    if a is b:
        return True
    id_a = getattr(a, '_instance_key', None)
    id_b = getattr(b, '_instance_key', None)
    if id_a is None or id_b is None:
        return False
    return id_a == id_b

attributes.identity_equal = identity_equal
