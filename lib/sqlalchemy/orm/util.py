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
    """Provide aliasing services for ORM join operations."""

    def __init__(self, alias, equivalents=None, chain_to=None):
        mapper, self.selectable, is_aliased_class = _entity_info(alias)
        if is_aliased_class:
            self.aliased_class = alias
        else:
            self.aliased_class = None
        self.equivalents = equivalents
        self.adapter = sql_util.ClauseAdapter(self.selectable, equivalents=equivalents)
        if chain_to:
            self.adapter.chain(chain_to.adapter)
        self.__wrap = None
        self.row_decorator = self._create_row_adapter()

    def wrap(self, aliasedclauses):
        """Wrap the operations performed by this AliasedClauses by another AliasedClauses."""
        ac = AliasedClauses.__new__(AliasedClauses)
        ac.__dict__ = self.__dict__.copy()
        ac.__wrap = aliasedclauses
        ac.row_decorator = ac._create_row_adapter()
        return ac

    def aliased_column(self, column):
        """adapt the given column to this AliasedClauses' selectable, and add it to the row adapter."""

        if self.__wrap:
            column = self.__wrap.aliased_column(column)

        conv = self.selectable.corresponding_column(column)
        if conv:
            return conv

        # process column-level subqueries
        aliased_column = self.adapter.traverse(column)

        # add to row decorator explicitly
        self.row_decorator.map[column] = aliased_column
        return aliased_column

    def adapt_clause(self, clause):
        if self.__wrap:
            clause = self.__wrap.adapt_clause(clause)

        return self.adapter.traverse(clause)

    def adapt_list(self, clauses):
        if self.__wrap:
            clauses = self.__wrap.adapt_list(clauses)

        return self.adapter.copy_and_process(clauses)

    def _create_row_adapter(self):
        adapter = create_row_adapter(self.selectable, equivalent_columns=self.equivalents)
        if self.__wrap:
            return self.__wrap.row_decorator.wrap(adapter)
        else:
            return adapter

class AliasedClass(object):
    def __init__(self, cls, alias=None):
        self.__mapper = _class_to_mapper(cls)
        self.__target = self.__mapper.class_
        alias = alias or self.__mapper._with_polymorphic_selectable.alias()
        self.__adapter = sql_util.ClauseAdapter(alias, equivalents=self.__mapper._equivalent_columns)
        self.__alias = alias
        self.__name__ = 'AliasedClass_' + str(self.__target)

    def __adapt_prop(self, prop):
        existing = getattr(self.__target, prop.key)
        comparator = AliasedComparator(self, self.__adapter, existing.comparator)
        queryattr = attributes.QueryableAttribute(
            existing.impl, parententity=self, comparator=comparator)
        setattr(self, prop.key, queryattr)
        return queryattr

    def __getattr__(self, key):
        prop = self.__mapper._get_property(key, raiseerr=False)
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
        self.__clause_element = self.adapter.traverse(self.comparator.__clause_element__())._annotate('parententity', aliasedclass)

    def __clause_element__(self):
        return self.__clause_element

    def operate(self, op, *other, **kwargs):
        return self.adapter.traverse(self.comparator.operate(op, *other, **kwargs))

    def reverse_operate(self, op, other, **kwargs):
        return self.adapter.traverse(self.comparator.reverse_operate(op, *other, **kwargs))

class _ORMJoin(expression.Join):

    __visit_name__ = expression.Join.__visit_name__

    def __init__(self, left, right, onclause=None, isouter=False):
        if hasattr(left, '_orm_mappers'):
            left_mapper = left._orm_mappers[1]
            adapt_from = left.right

        else:
            left_mapper, left, left_is_aliased = _entity_info(left)
            if left_is_aliased or not left_mapper:
                adapt_from = left
            else:
                adapt_from = None

        right_mapper, right, right_is_aliased = _entity_info(right)
        if right_is_aliased:
            adapt_to = right
        else:
            adapt_to = None

        if left_mapper or right_mapper:
            self._orm_mappers = (left_mapper, right_mapper)

            if isinstance(onclause, basestring):
                prop = left_mapper.get_property(onclause)
            elif isinstance(onclause, attributes.QueryableAttribute):
                adapt_from = onclause.__clause_element__()
                prop = onclause.property
            elif isinstance(onclause, MapperProperty):
                prop = onclause
            else:
                prop = None

            if prop:
                pj, sj, source, dest, secondary, target_adapter = prop._create_joins(source_selectable=adapt_from, dest_selectable=adapt_to, source_polymorphic=True, dest_polymorphic=True)

                if sj:
                    left = sql.join(left, secondary, pj, isouter)
                    onclause = sj
                else:
                    onclause = pj

                self._target_adapter = target_adapter

        expression.Join.__init__(self, left, right, onclause, isouter)

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

def _entity_info(entity, entity_name=None, compile=True):
    if isinstance(entity, AliasedClass):
        return entity._AliasedClass__mapper, entity._AliasedClass__alias, True
    elif hasattr(entity, '_class_state'):
        if isinstance(entity, type):
            mapper = class_mapper(entity, entity_name, compile)
        else:
            if compile:
                mapper = entity.compile()
            else:
                mapper = entity
        return mapper, mapper._with_polymorphic_selectable, False
    else:
        return None, entity, False

def _entity_descriptor(entity, key):
    if isinstance(entity, AliasedClass):
        desc = getattr(entity, key)
        return desc, desc.property
    elif isinstance(entity, type):
        desc = getattr(entity, key)  # class_state thing
        return desc, desc.property
    else:
        desc = getattr(entity.class_, key)
        return desc, desc.property

def _orm_columns(entity):
    mapper, selectable, is_aliased_class = _entity_info(entity)
    if isinstance(selectable, expression.Selectable):
        return [c for c in selectable.c]
    else:
        return [selectable]

def _orm_selectable(entity):
    mapper, selectable, is_aliased_class = _entity_info(entity)
    return selectable

def _is_mapped_class(entity):
    return hasattr(entity, '_class_state')

def _is_aliased_class(entity):
    return isinstance(entity, AliasedClass)

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
attributes._is_aliased_class = _is_aliased_class
attributes._entity_info = _entity_info
