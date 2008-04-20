# mapper/util.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy import sql, util, exceptions
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql.util import row_adapter as create_row_adapter
from sqlalchemy.sql import visitors
from sqlalchemy.orm.interfaces import MapperExtension, EXT_CONTINUE, PropComparator

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

def identity_key(*args, **kwargs):
    """Get an identity key.

    Valid call signatures:

    * ``identity_key(class, ident, entity_name=None)``

      class
          mapped class (must be a positional argument)

      ident
          primary key, if the key is composite this is a tuple

      entity_name
          optional entity name

    * ``identity_key(instance=instance)``

      instance
          object instance (must be given as a keyword arg)

    * ``identity_key(class, row=row, entity_name=None)``

      class
          mapped class (must be a positional argument)

      row
          result proxy row (must be given as a keyword arg)

      entity_name
          optional entity name (must be given as a keyword arg)
    """
    from sqlalchemy.orm import class_mapper, object_mapper
    if args:
        if len(args) == 1:
            class_ = args[0]
            try:
                row = kwargs.pop("row")
            except KeyError:
                ident = kwargs.pop("ident")
            entity_name = kwargs.pop("entity_name", None)
        elif len(args) == 2:
            class_, ident = args
            entity_name = kwargs.pop("entity_name", None)
        elif len(args) == 3:
            class_, ident, entity_name = args
        else:
            raise exceptions.ArgumentError("expected up to three "
                "positional arguments, got %s" % len(args))
        if kwargs:
            raise exceptions.ArgumentError("unknown keyword arguments: %s"
                % ", ".join(kwargs.keys()))
        mapper = class_mapper(class_, entity_name=entity_name)
        if "ident" in locals():
            return mapper.identity_key_from_primary_key(ident)
        return mapper.identity_key_from_row(row)
    instance = kwargs.pop("instance")
    if kwargs:
        raise exceptions.ArgumentError("unknown keyword arguments: %s"
            % ", ".join(kwargs.keys()))
    mapper = object_mapper(instance)
    return mapper.identity_key_from_instance(instance)
    
class ExtensionCarrier(object):
    """Fronts an ordered collection of MapperExtension objects.

    Bundles multiple MapperExtensions into a unified callable unit,
    encapsulating ordering, looping and EXT_CONTINUE logic.  The
    ExtensionCarrier implements the MapperExtension interface, e.g.::

      carrier.after_insert(...args...)

    Also includes a 'methods' dictionary accessor which allows for a quick
    check if a particular method is overridden on any contained
    MapperExtensions.

    """

    interface = util.Set([method for method in dir(MapperExtension)
                          if not method.startswith('_')])

    def __init__(self, extensions=None):
        self.methods = {}
        self._extensions = []
        for ext in extensions or ():
            self.append(ext)

    def copy(self):
        return ExtensionCarrier(self._extensions)

    def push(self, extension):
        """Insert a MapperExtension at the beginning of the collection."""
        self._register(extension)
        self._extensions.insert(0, extension)

    def append(self, extension):
        """Append a MapperExtension at the end of the collection."""
        self._register(extension)
        self._extensions.append(extension)

    def __iter__(self):
        """Iterate over MapperExtensions in the collection."""
        return iter(self._extensions)

    def _register(self, extension):
        """Register callable fronts for overridden interface methods."""
        for method in self.interface:
            if method in self.methods:
                continue
            impl = getattr(extension, method, None)
            if impl and impl is not getattr(MapperExtension, method):
                self.methods[method] = self._create_do(method)

    def _create_do(self, method):
        """Return a closure that loops over impls of the named method."""
        def _do(*args, **kwargs):
            for ext in self._extensions:
                ret = getattr(ext, method)(*args, **kwargs)
                if ret is not EXT_CONTINUE:
                    return ret
            else:
                return EXT_CONTINUE
        try:
            _do.__name__ = funcname
        except:
            pass
        return _do

    def _pass(*args, **kwargs):
        return EXT_CONTINUE
    _pass = staticmethod(_pass)

    def __getattr__(self, key):
        """Delegate MapperExtension methods to bundled fronts."""
        if key not in self.interface:
            raise AttributeError(key)
        return self.methods.get(key, self._pass)

class AliasedClauses(object):
    """Creates aliases of a mapped tables for usage in ORM queries, and provides expression adaptation."""

    def __init__(self, alias, equivalents=None, chain_to=None, should_adapt=True):
        self.alias = alias
        self.equivalents = equivalents
        self.row_decorator = self._create_row_adapter()
        self.should_adapt = should_adapt
        if should_adapt:
            self.adapter = sql_util.ClauseAdapter(self.alias, equivalents=equivalents)
        else:
            self.adapter = visitors.NullVisitor()

        if chain_to:
            self.adapter.chain(chain_to.adapter)
            
    def aliased_column(self, column):
        if not self.should_adapt:
            return column
            
        conv = self.alias.corresponding_column(column)
        if conv:
            return conv
        
        # process column-level subqueries    
        aliased_column = sql_util.ClauseAdapter(self.alias, equivalents=self.equivalents).traverse(column, clone=True)

        # add to row decorator explicitly
        self.row_decorator({}).map[column] = aliased_column
        return aliased_column

    def adapt_clause(self, clause):
        return self.adapter.traverse(clause, clone=True)
    
    def adapt_list(self, clauses):
        return self.adapter.copy_and_process(clauses)
        
    def _create_row_adapter(self):
        return create_row_adapter(self.alias, equivalent_columns=self.equivalents)


class PropertyAliasedClauses(AliasedClauses):
    """extends AliasedClauses to add support for primary/secondary joins on a relation()."""
    
    def __init__(self, prop, primaryjoin, secondaryjoin, parentclauses=None, alias=None, should_adapt=True):
        self.prop = prop
        self.mapper = self.prop.mapper
        self.table = self.prop.table
        self.parentclauses = parentclauses

        if not alias:
            from_obj = self.mapper._with_polymorphic_selectable()
            alias = from_obj.alias()

        super(PropertyAliasedClauses, self).__init__(alias, equivalents=self.mapper._equivalent_columns, chain_to=parentclauses, should_adapt=should_adapt)
        
        if prop.secondary:
            self.secondary = prop.secondary.alias()
            primary_aliasizer = sql_util.ClauseAdapter(self.secondary)
            secondary_aliasizer = sql_util.ClauseAdapter(self.alias, equivalents=self.equivalents).chain(sql_util.ClauseAdapter(self.secondary))

            if parentclauses is not None:
                primary_aliasizer.chain(sql_util.ClauseAdapter(parentclauses.alias, equivalents=parentclauses.equivalents))

            self.secondaryjoin = secondary_aliasizer.traverse(secondaryjoin, clone=True)
            self.primaryjoin = primary_aliasizer.traverse(primaryjoin, clone=True)
        else:
            primary_aliasizer = sql_util.ClauseAdapter(self.alias, exclude=prop.local_side, equivalents=self.equivalents)
            if parentclauses is not None: 
                primary_aliasizer.chain(sql_util.ClauseAdapter(parentclauses.alias, exclude=prop.remote_side, equivalents=parentclauses.equivalents))
            
            self.primaryjoin = primary_aliasizer.traverse(primaryjoin, clone=True)
            self.secondary = None
            self.secondaryjoin = None
        
        if prop.order_by:
            if prop.secondary:
                # usually this is not used but occasionally someone has a sort key in their secondary
                # table, even tho SA does not support writing this column directly
                self.order_by = secondary_aliasizer.copy_and_process(util.to_list(prop.order_by))
            else:
                self.order_by = primary_aliasizer.copy_and_process(util.to_list(prop.order_by))
                
        else:
            self.order_by = None

class AliasedClass(object):
    def __new__(cls, target):
        from sqlalchemy.orm import attributes
        mapper = _class_to_mapper(target)
        alias = mapper.mapped_table.alias()
        retcls = type(target.__name__ + "Alias", (cls,), {'alias':alias})
        retcls.mapper = mapper  # TEMPORARY
        for prop in mapper.iterate_properties:
            existing = mapper.class_manager[prop.key]
            setattr(retcls, prop.key, attributes.InstrumentedAttribute(existing.impl, comparator=AliasedComparator(alias, existing.comparator)))

        return retcls

    def __init__(self, alias):
        self.alias = alias

class AliasedComparator(PropComparator):
    def __init__(self, alias, comparator):
        self.alias = alias
        self.comparator = comparator
        self.adapter = sql_util.ClauseAdapter(alias) 

    def clause_element(self):
        return self.adapter.traverse(self.comparator.clause_element(), clone=True)

    def operate(self, op, *other, **kwargs):
        return self.adapter.traverse(self.comparator.operate(op, *other, **kwargs), clone=True)

    def reverse_operate(self, op, other, **kwargs):
        return self.adapter.traverse(self.comparator.reverse_operate(op, *other, **kwargs), clone=True)

from sqlalchemy.sql import expression
_selectable = expression._selectable
def _orm_selectable(selectable):
    if _is_aliased_class(selectable):
        return selectable.alias
    elif _is_mapped_class(selectable):
        return _class_to_mapper(selectable)._with_polymorphic_selectable()
    else:
        return _selectable(selectable)
#expression._selectable = _orm_selectable

class _ORMJoin(expression.Join):
    """future functionality."""

    __visit_name__ = expression.Join.__visit_name__
    
    def __init__(self, left, right, onclause=None, isouter=False):
        if _is_mapped_class(left) or _is_mapped_class(right):
            if hasattr(left, '_orm_mappers'):
                left_mapper = left._orm_mappers[1]
                adapt_from = left.right
            else:
                left_mapper = _class_to_mapper(left)
                if _is_aliased_class(left):
                    adapt_from = left.alias
                else:
                    adapt_from = None

            right_mapper = _class_to_mapper(right)
            self._orm_mappers = (left_mapper, right_mapper)
            
            if isinstance(onclause, basestring):
                prop = left_mapper.get_property(onclause)

                if _is_aliased_class(right):
                    adapt_to = right.alias
                else:
                    adapt_to = None

                pj, sj, source, dest, target_adapter = prop._create_joins(source_selectable=adapt_from, dest_selectable=adapt_to, source_polymorphic=True, dest_polymorphic=True)

                if sj:
                    left = sql.join(_orm_selectable(left), prop.secondary, onclause=pj)
                    onclause = sj
                else:
                    onclause = pj
        expression.Join.__init__(self, _orm_selectable(left), _orm_selectable(right), onclause, isouter)

    def join(self, right, onclause=None, isouter=False):
        return _ORMJoin(self, right, onclause, isouter)

    def outerjoin(self, right, onclause=None):
        return _ORMJoin(self, right, onclause, True)

def _join(left, right, onclause=None):
    """future functionality."""
    
    return _ORMJoin(left, right, onclause, False)

def _outerjoin(left, right, onclause=None):
    """future functionality."""

    return _ORMJoin(left, right, onclause, True)



def _state_mapper(state, entity_name=None):
    if state.entity_name is not attributes.NO_ENTITY_NAME:
        # Override the given entity name if the object is not transient.
        entity_name = state.entity_name
    return state.manager.mappers[entity_name]

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
    state = attributes.instance_state(object)
    if state.entity_name is not attributes.NO_ENTITY_NAME:
        # Override the given entity name if the object is not transient.
        entity_name = state.entity_name
    return class_mapper(
        type(object), entity_name=entity_name,
        compile=False, raiseerror=raiseerror)

def class_mapper(class_, entity_name=None, compile=True, raiseerror=True):
    """Given a class (or an object) and optional entity_name, return the primary Mapper associated with the key.

    If no mapper can be located, raises ``InvalidRequestError``.

    """
    
    if not isinstance(class_, type):
        class_ = type(class_)
    try:
        ### TEMPORARY until query_columns is merged
        if issubclass(class_, AliasedClass):
            return class_.mapper
        
        class_manager = attributes.manager_of_class(class_)
        mapper = class_manager.mappers[entity_name]
    except (KeyError, AttributeError):
        if not raiseerror:
            return
        raise exceptions.InvalidRequestError(
            "Class '%s' entity name '%s' has no mapper associated with it" %
            (class_.__name__, entity_name))
    if compile:
        mapper = mapper.compile()
    return mapper

def _class_to_mapper(class_or_mapper, entity_name=None, compile=True):
    if isinstance(class_or_mapper, type):
        return class_mapper(class_or_mapper, entity_name=entity_name, compile=compile)
    else:
        if compile:
            return class_or_mapper.compile()
        else:
            return class_or_mapper

def has_identity(object):
    state = attributes.instance_state(object)
    return _state_has_identity(state)

def _state_has_identity(state):
    return bool(state.key)

def has_mapper(object):
    state = attributes.instance_state(object)
    return _state_has_mapper(state)

def _state_has_mapper(state):
    return state.entity_name is not attributes.NO_ENTITY_NAME

def _is_aliased_class(obj):
    return isinstance(obj, type) and issubclass(obj, AliasedClass)

def _is_mapped_class(cls):
    return _is_aliased_class(cls) or bool(attributes.manager_of_class(cls))

def instance_str(instance):
    """Return a string describing an instance."""

    return state_str(attributes.instance_state(instance))

def state_str(state):
    """Return a string describing an instance."""
    if state is None:
        return "None"
    else:
        return state.class_.__name__ + "@" + hex(id(state.obj()))

def attribute_str(instance, attribute):
    return instance_str(instance) + "." + attribute

def state_attribute_str(state, attribute):
    return state_str(state) + "." + attribute

def identity_equal(a, b):
    if a is b:
        return True
    if a is None or b is None:
        return False
    try:
        state_a = attributes.instance_state(a)
        state_b = attributes.instance_state(b)
    except (KeyError, AttributeError):
        return False
    if state_a.key is None or state_b.key is None:
        return False
    return state_a.key == state_b.key

# TODO: Avoid circular import.
from sqlalchemy.orm import attributes
