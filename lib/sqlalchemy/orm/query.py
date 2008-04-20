# orm/query.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""The Query class and support.

Defines the [sqlalchemy.orm.query#Query] class, the central construct used by
the ORM to construct database queries.

The ``Query`` class should not be confused with the
[sqlalchemy.sql.expression#Select] class, which defines database SELECT
operations at the SQL (non-ORM) level.  ``Query`` differs from ``Select`` in
that it returns ORM-mapped objects and interacts with an ORM session, whereas
the ``Select`` construct interacts directly with the database to return
iterable result sets.
"""

from itertools import chain
from sqlalchemy import sql, util, exceptions, logging
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql import expression, visitors, operators
from sqlalchemy.orm import mapper, object_mapper

from sqlalchemy.orm.util import _state_mapper, _is_mapped_class, _is_aliased_class, _entity_descriptor, _entity_info, _class_to_mapper, _orm_columns, AliasedClass, _orm_selectable
from sqlalchemy.orm import util as mapperutil
from sqlalchemy.orm import interfaces
from sqlalchemy.orm import attributes


__all__ = ['Query', 'QueryContext', 'aliased']
aliased = AliasedClass

def _generative(*assertions):
    """mark a method as generative."""
    
    def decorate(fn):
        argspec = util.format_argspec_plus(fn)
        run_assertions = assertions
        code = "\n".join([
            "def %s%s:",
            "    %r",
            "    self = self._clone()",
            "    for a in run_assertions:",
            "        a(self, %r)",
            "    fn%s",
            "    return self"
        ]) % (fn.__name__, argspec['args'], fn.__doc__, fn.__name__, argspec['apply_pos'])
        env = locals().copy()
        exec code in env
        return env[fn.__name__]
    return decorate
    
class Query(object):
    """Encapsulates the object-fetching operations provided by Mappers."""

    def __init__(self, entities, session=None, entity_name=None):
        self._session = session
        
        self._with_options = []
        self._lockmode = None
        self._order_by = False
        self._group_by = False
        self._distinct = False
        self._offset = None
        self._limit = None
        self._statement = None
        self._params = {}
        self._yield_per = None
        self._criterion = None
        self._correlate = util.Set()
        self._joinpoint = None
        self.__joinable_tables = None
        self._having = None
        self._populate_existing = False
        self._version_check = False
        self._autoflush = True
        self._attributes = {}
        self._current_path = ()
        self._only_load_props = None
        self._refresh_instance = None
        self._from_obj = None
        self._entities = []
        self._polymorphic_adapters = {}
        self._filter_aliases = None
        self._from_obj_alias = None
        self.__currenttables = util.Set()

        for ent in util.to_list(entities):
            if _is_mapped_class(ent):
                _MapperEntity(self, ent, entity_name=entity_name)
            else:
                _ColumnEntity(self, ent, None)
                
        self.__setup_aliasizers(self._entities)
        
    def __setup_aliasizers(self, entities):
        d = {}
        for ent in entities:
            for entity in ent.entities:
                if entity not in d:
                    mapper, selectable, is_aliased_class = _entity_info(entity, ent.entity_name)
                    if not is_aliased_class and mapper.with_polymorphic:
                        with_polymorphic = mapper._with_polymorphic_mappers
                        self.__mapper_loads_polymorphically_with(mapper, mapperutil.AliasedClauses(selectable, equivalents=mapper._equivalent_columns))
                        adapter = None
                    elif is_aliased_class:
                        adapter = mapperutil.AliasedClauses(selectable, equivalents=mapper._equivalent_columns)
                        with_polymorphic = None
                    else:
                        with_polymorphic = adapter = None
                
                    d[entity] = (mapper, adapter, selectable, is_aliased_class, with_polymorphic)
                ent.setup_entity(entity, *d[entity])
    
    def __mapper_loads_polymorphically_with(self, mapper, adapter):
        for m2 in mapper._with_polymorphic_mappers:
            for m in m2.iterate_to_root():
                self._polymorphic_adapters[m.mapped_table] = self._polymorphic_adapters[m.local_table] = adapter
        
    def __set_select_from(self, from_obj):
        if isinstance(from_obj, expression._SelectBaseMixin):
            # alias SELECTs and unions
            from_obj = from_obj.alias()

        self._from_obj = from_obj
        equivs = self.__all_equivs()

        if isinstance(from_obj, expression.Alias):
            # dont alias a regular join (since its not an alias itself)
            self._from_obj_alias = mapperutil.AliasedClauses(self._from_obj, equivalents=equivs)

    def _get_polymorphic_adapter(self, entity, selectable):
        self.__mapper_loads_polymorphically_with(entity.mapper, mapperutil.AliasedClauses(selectable, equivalents=entity.mapper._equivalent_columns))
    
    def _reset_polymorphic_adapter(self, mapper):
        for m2 in mapper._with_polymorphic_mappers:
            for m in m2.iterate_to_root():
                self._polymorphic_adapters.pop(m.mapped_table, None)
                self._polymorphic_adapters.pop(m.local_table, None)
        
    def __reset_joinpoint(self):
        self._joinpoint = None
        self._filter_aliases = None
        
    def __adapt_polymorphic_element(self, element):
        if getattr(element, '_Query__no_adapt', False):
            # statements returned by a previous Query are immutable
            return element
            
        if isinstance(element, expression.FromClause):
            search = element
        elif hasattr(element, 'table'):
            search = element.table
        else:
            search = None
            
        if search:
            alias = self._polymorphic_adapters.get(search, None)
            if alias:
                return alias.adapt_clause(element)

    def _adapt_clause(self, clause, as_filter):
        if as_filter and self._filter_aliases:
            clause = self._filter_aliases.adapt_clause(clause)
        
        if self._polymorphic_adapters:
            clause = visitors.traverse(clause, before_clone=self.__adapt_polymorphic_element, clone=True)
            
        if self._from_obj_alias:
            clause = self._from_obj_alias.adapt_clause(clause)
        return clause
        
    def _entity_zero(self):
        if not getattr(self._entities[0], 'primary_entity', False):
            raise exceptions.InvalidRequestError("No primary mapper set up for this Query.")
        return self._entities[0]

    def _mapper_zero(self):
        return self._entity_zero().mapper
    
    def _extension_zero(self):
        ent = self._entity_zero()
        return getattr(ent, 'extension', ent.mapper.extension)
            
    def _mapper_entities(self):
        for ent in self._entities:
            if hasattr(ent, 'primary_entity'):
                yield ent
    _mapper_entities = property(_mapper_entities)
    
    def _joinpoint_zero(self):
        return self._joinpoint or self._entity_zero().entity
        
    def _mapper_zero_or_none(self):
        if not getattr(self._entities[0], 'primary_entity', False):
            return None
        return self._entities[0].mapper
        
    def _only_mapper_zero(self):
        if len(self._entities) > 1:
            raise exceptions.InvalidRequestError("This operation requires a Query against a single mapper.")
        return self._mapper_zero()
    
    def _only_entity_zero(self):
        if len(self._entities) > 1:
            raise exceptions.InvalidRequestError("This operation requires a Query against a single mapper.")
        return self._entity_zero()

    def _generate_mapper_zero(self):
        if not getattr(self._entities[0], 'primary_entity', False):
            raise exceptions.InvalidRequestError("No primary mapper set up for this Query.")
        entity = self._entities[0]._clone()
        self._entities = [entity] + self._entities[1:]
        return entity
        
    def __mapper_zero_from_obj(self):
        if self._from_obj:
            return self._from_obj
        else:
            return self._entity_zero().selectable

        
    def __all_equivs(self):
        equivs = {}
        for ent in self._mapper_entities:
            equivs.update(ent.mapper._equivalent_columns)
        return equivs
    
    def __no_criterion_condition(self, meth):
        if self._criterion or self._statement or self._from_obj:
            util.warn(
                ("Query.%s() being called on a Query with existing criterion; "
                 "criterion is being ignored.  This usage is deprecated.") % meth)

        self._statement = self._criterion = self._from_obj = None
        self._order_by = self._group_by = self._distinct = False
        self.__joined_tables = {}
    
    def __no_from_condition(self, meth):
        if self._from_obj:
            raise exceptions.InvalidRequestError("Query.%s() being called on a Query which already has a FROM clause established.  This usage is deprecated.")

    def __no_statement_condition(self, meth):
        if self._statement:
            raise exceptions.InvalidRequestError(
                ("Query.%s() being called on a Query with an existing full "
                 "statement - can't apply criterion.") % meth)
    
    def __no_limit_offset(self, meth):
        if self._limit or self._offset:
            util.warn("Query.%s() being called on a Query which already has LIMIT or OFFSET applied. "
            "This usage is deprecated. Apply filtering and joins before LIMIT or OFFSET are applied, "
            "or to filter/join to the row-limited results of the query, call from_self() first."
            "In release 0.5, from_self() will be called automatically in this scenario."
            )
            
    def __no_criterion(self):
        """generate a Query with no criterion, warn if criterion was present"""
    __no_criterion = _generative(__no_criterion_condition)(__no_criterion)

    def __get_options(self, populate_existing=None, version_check=None, only_load_props=None, refresh_instance=None):
        if populate_existing:
            self._populate_existing = populate_existing
        if version_check:
            self._version_check = version_check
        if refresh_instance:
            self._refresh_instance = refresh_instance
        if only_load_props:
            self._only_load_props = util.Set(only_load_props)
        return self

    def _clone(self):
        q = Query.__new__(Query)
        q.__dict__ = self.__dict__.copy()
        return q

    def session(self):
        if self._session is None:
            return self._mapper_zero().get_session()
        else:
            return self._session
    session = property(session)

    def statement(self):
        """return the full SELECT statement represented by this Query."""
        return self._compile_context().statement
    statement = property(statement)

    def whereclause(self):
        """return the WHERE criterion for this Query."""
        return self._criterion
    whereclause = property(whereclause)

    def _with_current_path(self, path):
        """indicate that this query applies to objects loaded within a certain path.
        
        Used by deferred loaders (see strategies.py) which transfer query 
        options from an originating query to a newly generated query intended
        for the deferred load.
        
        """
        self._current_path = path
    _with_current_path = _generative()(_with_current_path)
    
    def with_polymorphic(self, cls_or_mappers, selectable=None):
        """Load columns for descendant mappers of this Query's mapper.
        
        Using this method will ensure that each descendant mapper's
        tables are included in the FROM clause, and will allow filter() 
        criterion to be used against those tables.  The resulting 
        instances will also have those columns already loaded so that
        no "post fetch" of those columns will be required.
        
        ``cls_or_mappers`` is a single class or mapper, or list of class/mappers,
        which inherit from this Query's mapper.  Alternatively, it
        may also be the string ``'*'``, in which case all descending 
        mappers will be added to the FROM clause.
        
        ``selectable`` is a table or select() statement that will 
        be used in place of the generated FROM clause.  This argument
        is required if any of the desired mappers use concrete table 
        inheritance, since SQLAlchemy currently cannot generate UNIONs 
        among tables automatically.  If used, the ``selectable`` 
        argument must represent the full set of tables and columns mapped 
        by every desired mapper.  Otherwise, the unaccounted mapped columns
        will result in their table being appended directly to the FROM 
        clause which will usually lead to incorrect results.

        """
        entity = self._generate_mapper_zero()
        entity.set_with_polymorphic(self, cls_or_mappers, selectable=selectable)
    with_polymorphic = _generative(__no_from_condition, __no_criterion_condition)(with_polymorphic)
        
    def yield_per(self, count):
        """Yield only ``count`` rows at a time.

        WARNING: use this method with caution; if the same instance is present
        in more than one batch of rows, end-user changes to attributes will be
        overwritten.

        In particular, it's usually impossible to use this setting with
        eagerly loaded collections (i.e. any lazy=False) since those
        collections will be cleared for a new load when encountered in a
        subsequent result batch.

        """
        self._yield_per = count
    yield_per = _generative()(yield_per)
    
    def get(self, ident, **kwargs):
        """Return an instance of the object based on the given identifier, or None if not found.

        The `ident` argument is a scalar or tuple of primary key column values
        in the order of the table def's primary key columns.

        """
        
        ret = self._extension_zero().get(self, ident, **kwargs)
        if ret is not mapper.EXT_CONTINUE:
            return ret

        # convert composite types to individual args
        # TODO: account for the order of columns in the
        # ColumnProperty it corresponds to
        if hasattr(ident, '__composite_values__'):
            ident = ident.__composite_values__()

        key = self._only_mapper_zero().identity_key_from_primary_key(ident)
        return self._get(key, ident, **kwargs)

    def load(self, ident, raiseerr=True, **kwargs):
        """Return an instance of the object based on the given identifier.

        If not found, raises an exception.  The method will **remove all
        pending changes** to the object already existing in the Session.  The
        `ident` argument is a scalar or tuple of primary key column values in
        the order of the table def's primary key columns.

        """
        ret = self._extension_zero().load(self, ident, **kwargs)
        if ret is not mapper.EXT_CONTINUE:
            return ret
        key = self._only_mapper_zero().identity_key_from_primary_key(ident)
        instance = self.populate_existing()._get(key, ident, **kwargs)
        if instance is None and raiseerr:
            raise exceptions.InvalidRequestError("No instance found for identity %s" % repr(ident))
        return instance

    def query_from_parent(cls, instance, property, **kwargs):
        """Return a new Query with criterion corresponding to a parent instance.

        Return a newly constructed Query object, with criterion corresponding
        to a relationship to the given parent instance.

        instance
          a persistent or detached instance which is related to class
          represented by this query.

         property
           string name of the property which relates this query's class to the
           instance.

         \**kwargs
           all extra keyword arguments are propagated to the constructor of
           Query.
           
       deprecated.  use sqlalchemy.orm.with_parent in conjunction with 
       filter().

        """
        mapper = object_mapper(instance)
        prop = mapper.get_property(property, resolve_synonyms=True)
        target = prop.mapper
        criterion = prop.compare(operators.eq, instance, value_is_parent=True)
        return Query(target, **kwargs).filter(criterion)
    query_from_parent = classmethod(util.deprecated(None, False)(query_from_parent))
    
    def correlate(self, *args):
        self._correlate = self._correlate.union([_orm_selectable(s) for s in args])
    correlate = _generative()(correlate)
    
    def autoflush(self, setting):
        """Return a Query with a specific 'autoflush' setting.

        Note that a Session with autoflush=False will
        not autoflush, even if this flag is set to True at the 
        Query level.  Therefore this flag is usually used only
        to disable autoflush for a specific Query.
        
        """
        self._autoflush = setting
    autoflush = _generative()(autoflush)
    
    def populate_existing(self):
        """Return a Query that will refresh all instances loaded.

        This includes all entities accessed from the database, including
        secondary entities, eagerly-loaded collection items.

        All changes present on entities which are already present in the
        session will be reset and the entities will all be marked "clean".

        An alternative to populate_existing() is to expire the Session
        fully using session.expire_all().
        
        """
        self._populate_existing = True
    populate_existing = _generative()(populate_existing)
    
    def with_parent(self, instance, property=None):
        """add a join criterion corresponding to a relationship to the given parent instance.

            instance
                a persistent or detached instance which is related to class represented
                by this query.

            property
                string name of the property which relates this query's class to the
                instance.  if None, the method will attempt to find a suitable property.

        currently, this method only works with immediate parent relationships, but in the
        future may be enhanced to work across a chain of parent mappers.

        """
        from sqlalchemy.orm import properties
        mapper = object_mapper(instance)
        if property is None:
            for prop in mapper.iterate_properties:
                if isinstance(prop, properties.PropertyLoader) and prop.mapper is self._mapper_zero():
                    break
            else:
                raise exceptions.InvalidRequestError("Could not locate a property which relates instances of class '%s' to instances of class '%s'" % (self._mapper_zero().class_.__name__, instance.__class__.__name__))
        else:
            prop = mapper.get_property(property, resolve_synonyms=True)
        return self.filter(prop.compare(operators.eq, instance, value_is_parent=True))

    def add_entity(self, entity, alias=None, id=None):
        """add a mapped entity to the list of result columns to be returned."""

        if alias:
            entity = aliased(entity, alias)
            
        self._entities = list(self._entities)
        m = _MapperEntity(self, entity, id_=id)
        self.__setup_aliasizers([m])
    add_entity = _generative()(add_entity)
    
    def from_self(self, *entities):
        """return a Query that selects from this Query's SELECT statement.
        
        \*entities - optional list of entities which will replace 
        those being selected.  
        """
        
        fromclause = self.compile().correlate(None)
        self._statement = self._criterion = None
        self._order_by = self._group_by = self._distinct = False
        self._limit = self._offset = None
        self.__set_select_from(fromclause)
        if entities:
            self._entities = []
            for ent in entities:
                if _is_mapped_class(ent):
                    _MapperEntity(self, ent)
                else:
                    _ColumnEntity(self, ent, None)
            self.__setup_aliasizers(self._entities)
            
    from_self = _generative()(from_self)
    _from_self = from_self

    def values(self, *columns):
        """Return an iterator yielding result tuples corresponding to the given list of columns"""
        
        if not columns:
            return iter(())
        q = self._clone()
        q._entities = []
        for column in columns:
            _ColumnEntity(q, column, None)
        q.__setup_aliasizers(q._entities)
        if not q._yield_per:
            q._yield_per = 10
        return iter(q)
    _values = values
    
    def add_column(self, column, id=None):
        """Add a SQL ColumnElement to the list of result columns to be returned."""
        
        self._entities = list(self._entities)
        c = _ColumnEntity(self, column, id)
        self.__setup_aliasizers([c])
        
    add_column = _generative()(add_column)
    
    def options(self, *args):
        """Return a new Query object, applying the given list of
        MapperOptions.

        """
        return self.__options(False, *args)

    def _conditional_options(self, *args):
        return self.__options(True, *args)

    def __options(self, conditional, *args):
        # most MapperOptions write to the '_attributes' dictionary,
        # so copy that as well
        self._attributes = self._attributes.copy()
        opts = [o for o in util.flatten_iterator(args)]
        self._with_options = self._with_options + opts
        if conditional:
            for opt in opts:
                opt.process_query_conditionally(self)
        else:
            for opt in opts:
                opt.process_query(self)
    __options = _generative()(__options)
    
    def with_lockmode(self, mode):
        """Return a new Query object with the specified locking mode."""
        
        self._lockmode = mode
    with_lockmode = _generative()(with_lockmode)
    
    def params(self, *args, **kwargs):
        """add values for bind parameters which may have been specified in filter().

        parameters may be specified using \**kwargs, or optionally a single dictionary
        as the first positional argument.  The reason for both is that \**kwargs is
        convenient, however some parameter dictionaries contain unicode keys in which case
        \**kwargs cannot be used.

        """
        if len(args) == 1:
            kwargs.update(args[0])
        elif len(args) > 0:
            raise exceptions.ArgumentError("params() takes zero or one positional argument, which is a dictionary.")
        self._params = self._params.copy()
        self._params.update(kwargs)
    params = _generative()(params)
    
    def filter(self, criterion):
        """apply the given filtering criterion to the query and return the newly resulting ``Query``

        the criterion is any sql.ClauseElement applicable to the WHERE clause of a select.

        """
        if isinstance(criterion, basestring):
            criterion = sql.text(criterion)

        if criterion is not None and not isinstance(criterion, sql.ClauseElement):
            raise exceptions.ArgumentError("filter() argument must be of type sqlalchemy.sql.ClauseElement or string")
            
        criterion = self._adapt_clause(criterion, True)
        
        if self._criterion is not None:
            self._criterion = self._criterion & criterion
        else:
            self._criterion = criterion
    filter = _generative(__no_statement_condition, __no_limit_offset)(filter)
    
    def filter_by(self, **kwargs):
        """apply the given filtering criterion to the query and return the newly resulting ``Query``."""

        clauses = [_entity_descriptor(self._joinpoint_zero(), key)[0] == value
            for key, value in kwargs.iteritems()]

        return self.filter(sql.and_(*clauses))


    def min(self, col):
        """Execute the SQL ``min()`` function against the given column."""

        return self._col_aggregate(col, sql.func.min)

    def max(self, col):
        """Execute the SQL ``max()`` function against the given column."""

        return self._col_aggregate(col, sql.func.max)

    def sum(self, col):
        """Execute the SQL ``sum()`` function against the given column."""

        return self._col_aggregate(col, sql.func.sum)

    def avg(self, col):
        """Execute the SQL ``avg()`` function against the given column."""

        return self._col_aggregate(col, sql.func.avg)

    def order_by(self, *criterion):
        """apply one or more ORDER BY criterion to the query and return the newly resulting ``Query``"""
        
        criterion = [self._adapt_clause(expression._literal_as_text(o), True) for o in criterion]

        if self._order_by is False:
            self._order_by = criterion
        else:
            self._order_by = self._order_by + criterion
    order_by = util.array_as_starargs_decorator(order_by)
    order_by = _generative(__no_statement_condition)(order_by)
    
    def group_by(self, *criterion):
        """apply one or more GROUP BY criterion to the query and return the newly resulting ``Query``"""

        criterion = list(chain(*[_orm_columns(c) for c in criterion]))
        
        if self._group_by is False:
            self._group_by = criterion
        else:
            self._group_by = self._group_by + criterion
    group_by = util.array_as_starargs_decorator(group_by)
    group_by = _generative(__no_statement_condition)(group_by)
    
    def having(self, criterion):
        """apply a HAVING criterion to the query and return the newly resulting ``Query``."""

        if isinstance(criterion, basestring):
            criterion = sql.text(criterion)

        if criterion is not None and not isinstance(criterion, sql.ClauseElement):
            raise exceptions.ArgumentError("having() argument must be of type sqlalchemy.sql.ClauseElement or string")

        criterion = self._adapt_clause(criterion, True)

        if self._having is not None:
            self._having = self._having & criterion
        else:
            self._having = criterion
    having = _generative(__no_statement_condition)(having)
    
    def join(self, *props, **kwargs):
        """Create a join against this ``Query`` object's criterion
        and apply generatively, retunring the newly resulting ``Query``.

        each element in \*props may be:
          * a string property name, i.e. "rooms".  This will join along
          the relation of the same name from this Query's "primary"
          mapper, if one is present.
          
          * a class-mapped attribute, i.e. Houses.rooms.  This will create a 
          join from "Houses" table to that of the "rooms" relation.
          
          * a 2-tuple containing one of the above, combined with a selectable
            which derives from the destination table.   This will cause the join
            to link to the given selectable instead of the relation's 
            usual target table.  This argument can be used to join to table 
            or class aliases, or "polymorphic" selectables.

        e.g.::

            session.query(Company).join('employees')
            session.query(Company).join('employees', 'tasks')
            
            PAlias = aliased(Person)
            session.query(Person).join((Person.friends, Palias))
            
            session.query(Houses).join(Colonials.rooms, Room.closets)
            session.query(Company).join(('employees', people.join(engineers)), Engineer.computers)

        \**kwargs include:
        
            aliased - when joining, create anonymous aliases of each table.  This is
            used for self-referential joins or multiple joins to the same table.
            Consider usage of the aliased(SomeClass) construct as a more explicit
            approach to this.
            
            from_joinpoint - when joins are specified using string property names,
            locate the property from the mapper found in the most recent join() call,
            instead of from the root entity.
        """
        aliased, from_joinpoint = kwargs.pop('aliased', False), kwargs.pop('from_joinpoint', False)
        if kwargs:
            raise TypeError("unknown arguments: %s" % ','.join(kwargs.keys()))
        return self.__join(props, outerjoin=False, create_aliases=aliased, from_joinpoint=from_joinpoint)
    join = util.array_as_starargs_decorator(join)
    
    def outerjoin(self, *props, **kwargs):
        """Create a left outer join against this ``Query`` object's criterion
        and apply generatively, retunring the newly resulting ``Query``.

        each element in \*props may be:
          * a string property name, i.e. "rooms".  This will join along
          the relation of the same name from this Query's "primary"
          mapper, if one is present.
          
          * a class-mapped attribute, i.e. Houses.rooms.  This will create a 
          join from "Houses" table to that of the "rooms" relation.
          
          * a 2-tuple containing one of the above, combined with a selectable
            which derives from the destination table.   This will cause the join
            to link to the given selectable instead of the relation's 
            usual target table.  This argument can be used to join to table 
            or class aliases, or "polymorphic" selectables.

        e.g.::

            session.query(Company).outerjoin('employees')
            session.query(Company).outerjoin('employees', 'tasks')
            
            PAlias = aliased(Person)
            session.query(Person).outerjoin((Person.friends, Palias))
            
            session.query(Houses).outerjoin(Colonials.rooms, Room.closets)
            session.query(Company).outerjoin(('employees', people.outerjoin(engineers)), Engineer.computers)

        \**kwargs include:

            aliased - when joining, create anonymous aliases of each table.  This is
            used for self-referential joins or multiple joins to the same table.
            Consider usage of the aliased(SomeClass) construct as a more explicit
            approach to this.

            from_joinpoint - when joins are specified using string property names,
            locate the property from the mapper found in the most recent join() call,
            instead of from the root entity.
        """
        aliased, from_joinpoint = kwargs.pop('aliased', False), kwargs.pop('from_joinpoint', False)
        if kwargs:
            raise TypeError("unknown arguments: %s" % ','.join(kwargs.keys()))
        return self.__join(props, outerjoin=True, create_aliases=aliased, from_joinpoint=from_joinpoint)
    outerjoin = util.array_as_starargs_decorator(outerjoin)
    
    def __join(self, keys, outerjoin, create_aliases, from_joinpoint):
        self.__currenttables = util.Set(self.__currenttables)
        self._polymorphic_adapters = self._polymorphic_adapters.copy()
        
        if not from_joinpoint:
            self.__reset_joinpoint()
        
        clause = self._from_obj
        target = None
        
        for key in util.to_list(keys):
            use_selectable = None   # pre-chosen selectable to join to, either user-specified or mapper.with_polymorphic
            alias_criterion = False  # indicate to adapt future filter(), order_by(), etc. criterion to this selectable
            aliased_entity = False
            
            if isinstance(key, tuple):
                key, use_selectable = key

            if isinstance(key, interfaces.PropComparator):
                descriptor = key
                prop = key.property

                of_type = getattr(descriptor, '_of_type', None)
                if of_type and not use_selectable:
                    use_selectable = of_type #.mapped_table
                
                if not clause:
                    entity = descriptor.parententity
                    for ent in self._mapper_entities:
                        if ent.corresponds_to(entity):
                            clause = ent.selectable
                            break
                    else:
                        clause = descriptor.__clause_element__()

            else:
                if not target:
                    target = self._joinpoint_zero()

                    if not clause:
                        for ent in self._mapper_entities:
                            if ent.corresponds_to(target):
                                clause = ent.selectable
                                break
                        else:
                            raise exceptions.InvalidRequestError("No clause to join from")

                descriptor, prop = _entity_descriptor(target, key)

            if use_selectable:
                if _is_aliased_class(use_selectable):
                    target = use_selectable
                else:
                    if _is_mapped_class(use_selectable):
                        mapper = _class_to_mapper(use_selectable)
                        if mapper.with_polymorphic or isinstance(mapper.mapped_table, expression.Join):
                            aliased_entity = True
                        if create_aliases or aliased_entity:
                            target = aliased(use_selectable)
                            alias_criterion = True
                        else:
                            target = use_selectable
                    else:
                        if not use_selectable.is_derived_from(prop.mapper.mapped_table):
                            raise exceptions.InvalidRequestError("Selectable '%s' is not derived from '%s'" % (use_selectable.description, prop.mapper.mapped_table.description))
            
                        if not isinstance(use_selectable, expression.Alias):
                            use_selectable = use_selectable.alias()
                    
                        target = aliased(prop.mapper, use_selectable)
                        alias_criterion = True
            else:
                if not create_aliases:
                    if prop.table in self.__currenttables:
                        if prop.secondary is not None and prop.secondary not in self.__currenttables:
                            # TODO: this check is not strong enough for different paths to the same endpoint which
                            # does not use secondary tables
                            raise exceptions.InvalidRequestError("Can't join to property '%s'; a path to this table along a different secondary table already exists.  Use the `alias=True` argument to `join()`." % descriptor)

                        target = prop.mapper
                        continue

                    if prop.secondary:
                        self.__currenttables.add(prop.secondary)
                    self.__currenttables.add(prop.table)
                
                if prop.mapper.with_polymorphic:
                    aliased_entity = True

                if create_aliases or aliased_entity:
                    target = aliased(prop.mapper)
                    alias_criterion = True
                else:
                    target = prop.mapper

            if prop._is_self_referential() and not create_aliases and not use_selectable:
                raise exceptions.InvalidRequestError("Self-referential join on %s requires target selectable, or the aliased=True flag" % descriptor)

            clause = mapperutil.join(clause, target, prop, isouter=outerjoin)
            if alias_criterion: 
                self._filter_aliases = mapperutil.AliasedClauses(target, 
                        equivalents=prop.mapper._equivalent_columns, chain_to=self._filter_aliases)

                if aliased_entity:
                    self.__mapper_loads_polymorphically_with(prop.mapper, mapperutil.AliasedClauses(target, equivalents=prop.mapper._equivalent_columns))

        self._from_obj = clause
        self._joinpoint = target

    __join = _generative(__no_statement_condition, __no_limit_offset)(__join)

    def reset_joinpoint(self):
        """return a new Query reset the 'joinpoint' of this Query reset
        back to the starting mapper.  Subsequent generative calls will
        be constructed from the new joinpoint.

        Note that each call to join() or outerjoin() also starts from
        the root.

        """
        self.__reset_joinpoint()
    reset_joinpoint = _generative(__no_statement_condition)(reset_joinpoint)
    
    def select_from(self, from_obj):
        """Set the `from_obj` parameter of the query and return the newly
        resulting ``Query``.  This replaces the table which this Query selects
        from with the given table.


        `from_obj` is a single table or selectable.

        """
        if isinstance(from_obj, (tuple, list)):
            util.warn_deprecated("select_from() now accepts a single Selectable as its argument, which replaces any existing FROM criterion.")
            from_obj = from_obj[-1]

        self.__set_select_from(from_obj)
    select_from = _generative(__no_from_condition, __no_criterion_condition)(select_from)
    
    def __getitem__(self, item):
        if isinstance(item, slice):
            start = item.start
            stop = item.stop
            # if we slice from the end we need to execute the query
            if (isinstance(start, int) and start < 0) or \
               (isinstance(stop, int) and stop < 0):
                return list(self)[item]
            else:
                res = self._clone()
                if start is not None and stop is not None:
                    res._offset = (self._offset or 0) + start
                    res._limit = stop - start
                elif start is None and stop is not None:
                    res._limit = stop
                elif start is not None and stop is None:
                    res._offset = (self._offset or 0) + start
                if item.step is not None:
                    return list(res)[None:None:item.step]
                else:
                    return res
        else:
            return list(self[item:item+1])[0]

    def limit(self, limit):
        """Apply a ``LIMIT`` to the query and return the newly resulting

        ``Query``.

        """
        return self[:limit]

    def offset(self, offset):
        """Apply an ``OFFSET`` to the query and return the newly resulting
        ``Query``.

        """
        return self[offset:]

    def distinct(self):
        """Apply a ``DISTINCT`` to the query and return the newly resulting
        ``Query``.

        """
        self._distinct = True
    distinct = _generative(__no_statement_condition)(distinct)
    
    def all(self):
        """Return the results represented by this ``Query`` as a list.

        This results in an execution of the underlying query.

        """
        return list(self)

    def from_statement(self, statement):
        """Execute the given SELECT statement and return results.

        This method bypasses all internal statement compilation, and the
        statement is executed without modification.

        The statement argument is either a string, a ``select()`` construct,
        or a ``text()`` construct, and should return the set of columns
        appropriate to the entity class represented by this ``Query``.

        Also see the ``instances()`` method.

        """
        if isinstance(statement, basestring):
            statement = sql.text(statement)
        self._statement = statement
    from_statement = _generative(__no_criterion_condition)(from_statement)
    
    def first(self):
        """Return the first result of this ``Query`` or None if the result doesn't contain any row.

        This results in an execution of the underlying query.

        """
        if hasattr(self, '_column_aggregate'):
            return self._col_aggregate(*self._column_aggregate)

        ret = list(self[0:1])
        if len(ret) > 0:
            return ret[0]
        else:
            return None

    def one(self):
        """Return the first result of this ``Query``, raising an exception if more than one row exists.

        This results in an execution of the underlying query.

        """
        if hasattr(self, '_column_aggregate'):
            return self._col_aggregate(*self._column_aggregate)

        ret = list(self[0:2])

        if len(ret) == 1:
            return ret[0]
        elif len(ret) == 0:
            raise exceptions.InvalidRequestError('No rows returned for one()')
        else:
            raise exceptions.InvalidRequestError('Multiple rows returned for one()')

    def __iter__(self):
        context = self._compile_context()
        context.statement.use_labels = True
        if self._autoflush and not self._populate_existing:
            self.session._autoflush()
        return self._execute_and_instances(context)

    def _execute_and_instances(self, querycontext):
        result = self.session.execute(querycontext.statement, params=self._params, mapper=self._mapper_zero_or_none(), instance=self._refresh_instance)
        return self.iterate_instances(result, querycontext=querycontext)

    def instances(self, cursor, *mappers_or_columns, **kwargs):
        return list(self.iterate_instances(cursor, *mappers_or_columns, **kwargs))

    def iterate_instances(self, cursor, *mappers_or_columns, **kwargs):
        session = self.session

        context = kwargs.pop('querycontext', None)
        if context is None:
            context = QueryContext(self)

        context.runid = _new_runid()

        entities = self._entities + [_QueryEntity.legacy_guess_type(self, mc) for mc in mappers_or_columns]
        
        filtered = as_instances = False
        process = []
        for query_entity in entities:
            if isinstance(query_entity, _MapperEntity):
                filtered = True
            process.append(query_entity.row_processor(self, context))
        
        if filtered:
            if getattr(self._entities[0], 'primary_entity', False) and len(entities) == 1:
                as_instances = True
                filter = util.OrderedIdentitySet
            else:
                filter = util.OrderedSet
        else:
            filter = None
        
        while True:
            context.progress = util.Set()
            context.partials = {}

            if self._yield_per:
                fetch = cursor.fetchmany(self._yield_per)
                if not fetch:
                    break
            else:
                fetch = cursor.fetchall()

            if as_instances:
                rows = [process[0](context, row) for row in fetch]
            else:
                rows = [tuple([proc(context, row) for proc in process]) for row in fetch]

            if filter:
                rows = filter(rows)

            if context.refresh_instance and self._only_load_props and context.refresh_instance in context.progress:
                context.refresh_instance.commit(self._only_load_props)
                context.progress.remove(context.refresh_instance)

            for ii in context.progress:
                ii.commit_all()
                
            for ii, attrs in context.partials.items():
                ii.commit(attrs)
                
            for row in rows:
                yield row

            if not self._yield_per:
                break

    def _get(self, key=None, ident=None, refresh_instance=None, lockmode=None, only_load_props=None):
        lockmode = lockmode or self._lockmode
        if not self._populate_existing and not refresh_instance and not self._mapper_zero().always_refresh and lockmode is None:
            try:
                # TODO: expire check here
                return self.session.identity_map[key]
            except KeyError:
                pass

        if ident is None:
            if key is not None:
                ident = key[1]
        else:
            ident = util.to_list(ident)

        if refresh_instance is None:
            q = self.__no_criterion()
        else:
            q = self._clone()
        
        if ident is not None:
            mapper = q._mapper_zero()
            params = {}
            (_get_clause, _get_params) = mapper._get_clause

            _get_clause = q._adapt_clause(_get_clause, True)
            q._criterion = _get_clause

            for i, primary_key in enumerate(mapper.primary_key):
                try:
                    params[_get_params[primary_key].key] = ident[i]
                except IndexError:
                    raise exceptions.InvalidRequestError("Could not find enough values to formulate primary key for query.get(); primary key columns are %s" % ', '.join(["'%s'" % str(c) for c in q.mapper.primary_key]))
            q._params = params

        if lockmode is not None:
            q._lockmode = lockmode
        q.__get_options(populate_existing=bool(refresh_instance), version_check=(lockmode is not None), only_load_props=only_load_props, refresh_instance=refresh_instance)
        q._order_by = None
        try:
            # call using all() to avoid LIMIT compilation complexity
            return q.all()[0]
        except IndexError:
            return None

    def _select_args(self):
        return {'limit':self._limit, 'offset':self._offset, 'distinct':self._distinct, 'group_by':self._group_by or None, 'having':self._having or None}
    _select_args = property(_select_args)
    
    def _should_nest_selectable(self):
        kwargs = self._select_args
        return (kwargs.get('limit') is not None or kwargs.get('offset') is not None or kwargs.get('distinct', False))
    _should_nest_selectable = property(_should_nest_selectable)

    def count(self, whereclause=None, params=None, **kwargs):
        """Apply this query's criterion to a SELECT COUNT statement.

        the whereclause, params and \**kwargs arguments are deprecated.  use filter()
        and other generative methods to establish modifiers.

        """
        q = self
        if whereclause is not None:
            q = q.filter(whereclause)
        if params is not None:
            q = q.params(params)
        q = q._legacy_select_kwargs(**kwargs)
        return q._count()

    def _count(self):
        """Apply this query's criterion to a SELECT COUNT statement.

        this is the purely generative version which will become
        the public method in version 0.5.

        """
        return self._col_aggregate(sql.literal_column('1'), sql.func.count, nested_cols=list(self._mapper_zero().primary_key))

    def _col_aggregate(self, col, func, nested_cols=None):
        whereclause = self._criterion
        
        context = QueryContext(self)
        from_obj = self.__mapper_zero_from_obj()

        if self._should_nest_selectable:
            if not nested_cols:
                nested_cols = [col]
            s = sql.select(nested_cols, whereclause, from_obj=from_obj, **self._select_args)
            s = s.alias()
            s = sql.select([func(s.corresponding_column(col) or col)]).select_from(s)
        else:
            s = sql.select([func(col)], whereclause, from_obj=from_obj, **self._select_args)
            
        if self._autoflush and not self._populate_existing:
            self.session._autoflush()
        return self.session.scalar(s, params=self._params, mapper=self._mapper_zero())

    def compile(self):
        """compiles and returns a SQL statement based on the criterion and conditions within this Query."""

        return self._compile_context().statement

    def _compile_context(self):
        context = QueryContext(self)

        if self._statement:
            self._statement.use_labels = True
            context.statement = self._statement
            return context

        if self._lockmode:
            try:
                for_update = {'read':'read','update':True,'update_nowait':'nowait',None:False}[self._lockmode]
            except KeyError:
                raise exceptions.ArgumentError("Unknown lockmode '%s'" % self._lockmode)
        else:
            for_update = False
            
        context.from_clause = self._from_obj
        context.whereclause = self._criterion
        context.order_by = self._order_by
        
        for entity in self._entities:
            entity.setup_context(self, context)

        if context.order_by:
            context.order_by = [expression._literal_as_text(o) for o in util.to_list(context.order_by)]
        
        eager_joins = context.eager_joins.values()

        if context.from_clause:
            froms = [context.from_clause]  # "load from a single FROM" mode, i.e. when select_from() or join() is used
        else:
            froms = context.froms   # "load from discrete FROMs" mode, i.e. when each _MappedEntity has its own FROM
         
        if eager_joins and self._should_nest_selectable:
            # for eager joins present and LIMIT/OFFSET/DISTINCT, wrap the query inside a select,
            # then append eager joins onto that
            
            if context.order_by:
                order_by_col_expr = list(chain(*[sql_util.find_columns(o) for o in context.order_by]))
            else:
                context.order_by = None
                order_by_col_expr = []
            
            inner = sql.select(context.primary_columns + order_by_col_expr, context.whereclause, from_obj=froms, use_labels=True, correlate=False, order_by=context.order_by, **self._select_args)
            
            if self._correlate:
                inner = inner.correlate(*self._correlate)
                
            inner = inner.alias()
            
            equivs = self.__all_equivs()

            context.row_adapter = mapperutil.create_row_adapter(inner, equivalent_columns=equivs)

            statement = sql.select([inner] + context.secondary_columns, for_update=for_update, use_labels=True)

            from_clause = inner
            for eager_join in context.eager_joins.values():
                from_clause = sql_util.splice_joins(from_clause, eager_join)

            statement.append_from(from_clause)

            if context.order_by:
                local_adapter = sql_util.ClauseAdapter(inner)
                statement.append_order_by(*local_adapter.copy_and_process(context.order_by))

            statement.append_order_by(*context.eager_order_by)
        else:
            if not context.order_by:
                context.order_by = None
            
            if self._distinct and context.order_by:
                order_by_col_expr = list(chain(*[sql_util.find_columns(o) for o in context.order_by]))
                context.primary_columns += order_by_col_expr

            froms += context.eager_joins.values()
                
            statement = sql.select(context.primary_columns + context.secondary_columns, context.whereclause, from_obj=froms, use_labels=True, for_update=for_update, correlate=False, order_by=context.order_by, **self._select_args)
            if self._correlate:
                statement = statement.correlate(*self._correlate)
                
            if context.eager_order_by:
                statement.append_order_by(*context.eager_order_by)
            
        context.statement = statement._annotate('_Query__no_adapt', True)

        return context

    def __log_debug(self, msg):
        self.logger.debug(msg)

    def __str__(self):
        return str(self.compile())

    # DEPRECATED LAND !

    def _generative_col_aggregate(self, col, func):
        """apply the given aggregate function to the query and return the newly
        resulting ``Query``. (deprecated)
        """
        
        if getattr(self, '_column_aggregate', None):
            raise exceptions.InvalidRequestError("Query already contains an aggregate column or function")
        self._column_aggregate = (col, func)
    _generative_col_aggregate = _generative(__no_statement_condition)(_generative_col_aggregate)
    
    def apply_min(self, col):
        """apply the SQL ``min()`` function against the given column to the
        query and return the newly resulting ``Query``.
        
        DEPRECATED.
        """
        return self._generative_col_aggregate(col, sql.func.min)

    def apply_max(self, col):
        """apply the SQL ``max()`` function against the given column to the
        query and return the newly resulting ``Query``.

        DEPRECATED.
        """
        return self._generative_col_aggregate(col, sql.func.max)

    def apply_sum(self, col):
        """apply the SQL ``sum()`` function against the given column to the
        query and return the newly resulting ``Query``.

        DEPRECATED.
        """
        return self._generative_col_aggregate(col, sql.func.sum)

    def apply_avg(self, col):
        """apply the SQL ``avg()`` function against the given column to the
        query and return the newly resulting ``Query``.

        DEPRECATED.
        """
        return self._generative_col_aggregate(col, sql.func.avg)

    def list(self): #pragma: no cover
        """DEPRECATED.  use all()"""

        return list(self)

    def scalar(self): #pragma: no cover
        """DEPRECATED.  use first()"""

        return self.first()

    def _legacy_filter_by(self, *args, **kwargs): #pragma: no cover
        return self.filter(self._legacy_join_by(args, kwargs, start=self._joinpoint_zero()))

    def count_by(self, *args, **params): #pragma: no cover
        """DEPRECATED.  use query.filter_by(\**params).count()"""

        return self.count(self.join_by(*args, **params))


    def select_whereclause(self, whereclause=None, params=None, **kwargs): #pragma: no cover
        """DEPRECATED.  use query.filter(whereclause).all()"""

        q = self.filter(whereclause)._legacy_select_kwargs(**kwargs)
        if params is not None:
            q = q.params(params)
        return list(q)

    def _legacy_select_from(self, from_obj):
        q = self._clone()
        if len(from_obj) > 1:
            raise exceptions.ArgumentError("Multiple-entry from_obj parameter no longer supported")
        q._from_obj = from_obj[0]
        return q

    def _legacy_select_kwargs(self, **kwargs): #pragma: no cover
        q = self
        if "order_by" in kwargs and kwargs['order_by']:
            q = q.order_by(kwargs['order_by'])
        if "group_by" in kwargs:
            q = q.group_by(kwargs['group_by'])
        if "from_obj" in kwargs:
            q = q._legacy_select_from(kwargs['from_obj'])
        if "lockmode" in kwargs:
            q = q.with_lockmode(kwargs['lockmode'])
        if "distinct" in kwargs:
            q = q.distinct()
        if "limit" in kwargs:
            q = q.limit(kwargs['limit'])
        if "offset" in kwargs:
            q = q.offset(kwargs['offset'])
        return q

    def get_by(self, *args, **params): #pragma: no cover
        """DEPRECATED.  use query.filter_by(\**params).first()"""

        ret = self._entity.zero().extension.get_by(self, *args, **params)
        if ret is not mapper.EXT_CONTINUE:
            return ret

        return self._legacy_filter_by(*args, **params).first()

    def select_by(self, *args, **params): #pragma: no cover
        """DEPRECATED. use use query.filter_by(\**params).all()."""

        ret = self._extension_zero().select_by(self, *args, **params)
        if ret is not mapper.EXT_CONTINUE:
            return ret

        return self._legacy_filter_by(*args, **params).list()

    def join_by(self, *args, **params): #pragma: no cover
        """DEPRECATED. use join() to construct joins based on attribute names."""

        return self._legacy_join_by(args, params, start=self._joinpoint_zero())

    def _build_select(self, arg=None, params=None, **kwargs): #pragma: no cover
        if isinstance(arg, sql.FromClause) and arg.supports_execution():
            return self.from_statement(arg)
        else:
            return self.filter(arg)._legacy_select_kwargs(**kwargs)

    def selectfirst(self, arg=None, **kwargs): #pragma: no cover
        """DEPRECATED.  use query.filter(whereclause).first()"""

        return self._build_select(arg, **kwargs).first()

    def selectone(self, arg=None, **kwargs): #pragma: no cover
        """DEPRECATED.  use query.filter(whereclause).one()"""

        return self._build_select(arg, **kwargs).one()

    def select(self, arg=None, **kwargs): #pragma: no cover
        """DEPRECATED.  use query.filter(whereclause).all(), or query.from_statement(statement).all()"""

        ret = self._extension_zero().select(self, arg=arg, **kwargs)
        if ret is not mapper.EXT_CONTINUE:
            return ret
        return self._build_select(arg, **kwargs).all()

    def execute(self, clauseelement, params=None, *args, **kwargs): #pragma: no cover
        """DEPRECATED.  use query.from_statement().all()"""

        return self._select_statement(clauseelement, params, **kwargs)

    def select_statement(self, statement, **params): #pragma: no cover
        """DEPRECATED.  Use query.from_statement(statement)"""

        return self._select_statement(statement, params)

    def select_text(self, text, **params): #pragma: no cover
        """DEPRECATED.  Use query.from_statement(statement)"""

        return self._select_statement(text, params)

    def _select_statement(self, statement, params=None, **kwargs): #pragma: no cover
        q = self.from_statement(statement)
        if params is not None:
            q = q.params(params)
        q.__get_options(**kwargs)
        return list(q)

    def join_to(self, key): #pragma: no cover
        """DEPRECATED. use join() to create joins based on property names."""

        [keys, p] = self._locate_prop(key)
        return self.join_via(keys)

    def join_via(self, keys): #pragma: no cover
        """DEPRECATED. use join() to create joins based on property names."""

        mapper = self._joinpoint_zero()
        clause = None
        for key in keys:
            prop = mapper.get_property(key, resolve_synonyms=True)
            if clause is None:
                clause = prop._get_join(mapper)
            else:
                clause &= prop._get_join(mapper)
            mapper = prop.mapper

        return clause

    def _legacy_join_by(self, args, params, start=None): #pragma: no cover
        import properties

        clause = None
        for arg in args:
            if clause is None:
                clause = arg
            else:
                clause &= arg

        for key, value in params.iteritems():
            (keys, prop) = self._locate_prop(key, start=start)
            if isinstance(prop, properties.PropertyLoader):
                c = prop.compare(operators.eq, value) & self.join_via(keys[:-1])
            else:
                c = prop.compare(operators.eq, value) & self.join_via(keys)
            if clause is None:
                clause =  c
            else:
                clause &= c
        return clause

    def _locate_prop(self, key, start=None): #pragma: no cover
        import properties
        keys = []
        seen = util.Set()
        def search_for_prop(mapper_):
            if mapper_ in seen:
                return None
            seen.add(mapper_)

            prop = mapper_.get_property(key, resolve_synonyms=True, raiseerr=False)
            if prop is not None:
                if isinstance(prop, properties.PropertyLoader):
                    keys.insert(0, prop.key)
                return prop
            else:
                for prop in mapper_.iterate_properties:
                    if not isinstance(prop, properties.PropertyLoader):
                        continue
                    x = search_for_prop(prop.mapper)
                    if x:
                        keys.insert(0, prop.key)
                        return x
                else:
                    return None
        p = search_for_prop(start or self._only_mapper_zero())
        if p is None:
            raise exceptions.InvalidRequestError("Can't locate property named '%s'" % key)
        return [keys, p]

    def selectfirst_by(self, *args, **params): #pragma: no cover
        """DEPRECATED. Use query.filter_by(\**kwargs).first()"""

        return self._legacy_filter_by(*args, **params).first()

    def selectone_by(self, *args, **params): #pragma: no cover
        """DEPRECATED. Use query.filter_by(\**kwargs).one()"""

        return self._legacy_filter_by(*args, **params).one()

    for deprecated_method in ('list', 'scalar', 'count_by',
                              'select_whereclause', 'get_by', 'select_by',
                              'join_by', 'selectfirst', 'selectone', 'select',
                              'execute', 'select_statement', 'select_text',
                              'join_to', 'join_via', 'selectfirst_by',
                              'selectone_by', 'apply_max', 'apply_min',
                              'apply_avg', 'apply_sum'):
        locals()[deprecated_method] = \
            util.deprecated(None, False)(locals()[deprecated_method])

class _QueryEntity(object):
    """represent an entity column returned within a Query result."""
    
    def legacy_guess_type(self, query, e):
        if isinstance(e, type):
            ent = _MapperEntity(None, mapper.class_mapper(e), False)
        elif isinstance(e, mapper.Mapper):
            ent = _MapperEntity(None, e, False)
        else:
            ent = _ColumnEntity(None, column=e)
            
        query._Query__setup_aliasizers([ent])
        return ent
            
    legacy_guess_type = classmethod(legacy_guess_type)

    def _clone(self):
        q = self.__class__.__new__(self.__class__)
        q.__dict__ = self.__dict__.copy()
        return q

class _MapperEntity(_QueryEntity):
    """mapper/class/AliasedClass entity"""
    
    def __init__(self, query, entity, id_=None, entity_name=None):
        if query:
            self.primary_entity = not query._entities
            query._entities.append(self)
        else:
            self.primary_entity = False

        self.entities = [entity]
        self.entity_name = entity_name
        self.alias_id = id_

    def setup_entity(self, entity, mapper, adapter, from_obj, is_aliased_class, with_polymorphic):
        self.mapper = mapper
        self.adapter = adapter
        self.selectable  = from_obj
        self._with_polymorphic = with_polymorphic
        self.is_aliased_class = is_aliased_class
        if is_aliased_class:
            self.path_entity = self.entity = entity
        else:
            self.path_entity = mapper.base_mapper
            self.entity = mapper

    def set_with_polymorphic(self, query, cls_or_mappers, selectable):
        if cls_or_mappers is None:
            query._reset_polymorphic_adapter(self.mapper)
            return
            
        mappers, from_obj = self.mapper._with_polymorphic_args(cls_or_mappers, selectable)
        self._with_polymorphic = mappers
        
        # TODO: do the wrapped thing here too so that with_polymorphic() can be
        # applied to aliases
        if not self.is_aliased_class:
            self.selectable = from_obj
            self.adapter = query._get_polymorphic_adapter(self, from_obj)

    def corresponds_to(self, entity):
        if _is_aliased_class(entity):
            return entity is self.path_entity
        else:
            return entity.base_mapper is self.path_entity
        
    def _get_entity_clauses(self, query, context):

        adapter = None
        if not self.is_aliased_class and query._polymorphic_adapters:
            for mapper in self.mapper.iterate_to_root():
                adapter = query._polymorphic_adapters.get(mapper.mapped_table, None)
                if adapter:
                    break

        if not adapter and self.adapter:
            adapter = self.adapter
                
        if adapter:
            if query._from_obj_alias:
                ret = query._from_obj_alias.wrap(adapter)
            else:
                ret = adapter
        else:
            ret = query._from_obj_alias
        
        return ret
    
    def row_processor(self, query, context):
        row_adapter = None
        
        clauses = self._get_entity_clauses(query, context)
        if clauses:
            row_adapter = clauses.row_decorator
        
        if context.row_adapter:
            if row_adapter:
                row_adapter = row_adapter.wrap(context.row_adapter)
            else:
                row_adapter = context.row_adapter
                
        # polymorphic mappers which have concrete tables in their hierarchy usually
        # require row aliasing unconditionally.  
        if not row_adapter and self.mapper._requires_row_aliasing:
            row_adapter = mapperutil.create_row_adapter(self.selectable, equivalent_columns=self.mapper._equivalent_columns)
        
        if self.primary_entity:
            kwargs = dict(extension=getattr(self, 'extension', None), only_load_props=query._only_load_props, refresh_instance=context.refresh_instance)
        else:
            kwargs = {}
                
        if row_adapter:
            def main(context, row):
                return self.mapper._instance(context, self.path_entity, row_adapter(row), None, **kwargs)
        else:
            def main(context, row):
                return self.mapper._instance(context, self.path_entity, row, None, **kwargs)
        return main
            
    def setup_context(self, query, context):
        # if single-table inheritance mapper, add "typecol IN (polymorphic)" criterion so
        # that we only load the appropriate types
        if self.mapper.single and self.mapper.inherits is not None and self.mapper.polymorphic_on is not None and self.mapper.polymorphic_identity is not None:
            context.whereclause = sql.and_(context.whereclause, self.mapper.polymorphic_on.in_([m.polymorphic_identity for m in self.mapper.polymorphic_iterator()]))
        
        context.froms.append(self.selectable)

        adapter = self._get_entity_clauses(query, context)

        if self.primary_entity:
            if context.order_by is False:
                # the "default" ORDER BY use case applies only to "mapper zero".  the "from clause" default should
                # go away in 0.5 (or...maybe 0.6).
                if self.mapper.order_by:
                    context.order_by = self.mapper.order_by
                elif context.from_clause:
                    context.order_by = context.from_clause.default_order_by()
                else:
                    context.order_by = self.selectable.default_order_by()
            if context.order_by and adapter:
                context.order_by = adapter.adapt_list(util.to_list(context.order_by))
        
        for value in self.mapper._iterate_polymorphic_properties(self._with_polymorphic):
            if query._only_load_props and value.key not in query._only_load_props:
                continue
            context.exec_with_path(self.path_entity, value.key, value.setup, context, only_load_props=query._only_load_props, entity=self, column_collection=context.primary_columns, parentclauses=adapter)
        
    def __str__(self):
        return str(self.mapper)

        
class _ColumnEntity(_QueryEntity):
    """Column/expression based entity."""

    def __init__(self, query, column, id):
        if query:
            query._entities.append(self)

        if isinstance(column, basestring):
            column = sql.literal_column(column)
        elif isinstance(column, (attributes.QueryableAttribute, mapper.Mapper._CompileOnAttr)):
            column = column.__clause_element__()
        elif not isinstance(column, sql.ColumnElement):
            raise exceptions.InvalidRequestError("Invalid column expression '%r'" % column)

        if not hasattr(column, '_label'):
            column = column.label(None)
        
        self.column = column
        self.alias_id = id
        self.entity_name = None
        self.froms = util.Set()
        self.entities = util.Set([elem.parententity for elem in visitors.iterate(column) if hasattr(elem, 'parententity')])
            
    def setup_entity(self, entity, mapper, adapter, from_obj, is_aliased_class, with_polymorphic):
        self.froms.add(from_obj)

    def __resolve_expr_against_query_aliases(self, query, expr, context):
        expr = query._adapt_clause(expr, False)
        while hasattr(expr, '__clause_element__'):
            expr = expr.__clause_element__()
        return expr
        
    def row_processor(self, query, context):
        column = self.__resolve_expr_against_query_aliases(query, self.column, context)
            
        if context.row_adapter:
            column = context.row_adapter.translate_col(column)
            
        def proc(context, row):
            return row[column]
        return proc
    
    def setup_context(self, query, context):
        column = self.__resolve_expr_against_query_aliases(query, self.column, context)
        context.froms += list(self.froms)
        context.primary_columns.append(column)
    
    def __str__(self):
        return str(self.column)

        
Query.logger = logging.class_logger(Query)

class QueryContext(object):
    def __init__(self, query):
        self.query = query
        self.session = query.session
        self.populate_existing = query._populate_existing
        self.version_check = query._version_check
        self.refresh_instance = query._refresh_instance
        self.path = ()
        self.primary_columns = []
        self.secondary_columns = []
        self.eager_order_by = []
        
        self.eager_joins = {}
        self.froms = []
        self.from_clause = None
        self.row_adapter = None
        
        self.options = query._with_options
        self.attributes = query._attributes.copy()
    
    def exec_with_path(self, path_entity, propkey, fn, *args, **kwargs):
        oldpath = self.path
        self.path += (path_entity, propkey)
        try:
            return fn(*args, **kwargs)
        finally:
            self.path = oldpath

_runid = 1L
_id_lock = util.threading.Lock()

def _new_runid():
    global _runid
    _id_lock.acquire()
    try:
        _runid += 1
        return _runid
    finally:
        _id_lock.release()
