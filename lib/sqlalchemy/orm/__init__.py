# mapper/__init__.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
The mapper package provides object-relational functionality, building upon
the schema and sql packages and tying operations to class properties and
constructors.
"""

from sqlalchemy import util as sautil
from sqlalchemy.orm.mapper import Mapper, object_mapper, class_mapper, mapper_registry
from sqlalchemy.orm.interfaces import SynonymProperty, MapperExtension, EXT_CONTINUE, EXT_STOP, EXT_PASS, ExtensionOption, PropComparator
from sqlalchemy.orm.properties import PropertyLoader, ColumnProperty, CompositeProperty, BackRef
from sqlalchemy.orm import mapper as mapperlib
from sqlalchemy.orm import strategies
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.util import polymorphic_union, create_row_adapter
from sqlalchemy.orm.session import Session as _Session
from sqlalchemy.orm.session import object_session, attribute_manager, sessionmaker
from sqlalchemy.orm.scoping import ScopedSession


__all__ = [ 'relation', 'column_property', 'composite', 'backref', 'eagerload',
            'eagerload_all', 'lazyload', 'noload', 'deferred', 'defer',
            'undefer', 'undefer_group', 'extension', 'mapper', 'clear_mappers',
            'compile_mappers', 'class_mapper', 'object_mapper', 'sessionmaker',
            'scoped_session', 'dynamic_loader', 'MapperExtension',
            'polymorphic_union',
            'create_session', 'synonym', 'contains_alias', 'Query',
            'contains_eager', 'EXT_CONTINUE', 'EXT_STOP', 'EXT_PASS',
            'object_session', 'PropComparator' ]


def scoped_session(session_factory, scopefunc=None):
  """Provides thread-local management of Sessions.
  
  This is a front-end function to the [sqlalchemy.orm.scoping#ScopedSession]
  class.

  Usage::

    Session = scoped_session(sessionmaker(autoflush=True))

  To instantiate a Session object which is part of the scoped
  context, instantiate normally::
  
    session = Session()
    
  Most session methods are available as classmethods from
  the scoped session::
  
    Session.commit()
    Session.close()
    
  To map classes so that new instances are saved in the current
  Session automatically, as well as to provide session-aware
  class attributes such as "query", use the `mapper` classmethod
  from the scoped session::
    
    mapper = Session.mapper
    mapper(Class, table, ...)

  """
  
  return ScopedSession(session_factory, scopefunc=scopefunc)
  
def create_session(bind=None, **kwargs):
    """create a new [sqlalchemy.orm.session#Session].
    
    The session by default does not begin a transaction, and requires that
    flush() be called explicitly in order to persist results to the database.
    
    It is recommended to use the [sqlalchemy.orm#sessionmaker()] function 
    instead of create_session().
    """
    kwargs.setdefault('autoflush', False)
    kwargs.setdefault('transactional', False)
    return _Session(bind=bind, **kwargs)
    
def relation(argument, secondary=None, **kwargs):
    """Provide a relationship of a primary Mapper to a secondary Mapper.

    This corresponds to a parent-child or associative table relationship.  
    The constructed class is an instance of [sqlalchemy.orm.properties#PropertyLoader].

      argument
          a class or Mapper instance, representing the target of the relation.

      secondary
        for a many-to-many relationship, specifies the intermediary table. The
        ``secondary`` keyword argument should generally only be used for a table
        that is not otherwise expressed in any class mapping. In particular,
        using the Association Object Pattern is
        generally mutually exclusive against using the ``secondary`` keyword
        argument.

      \**kwargs follow:

        association
          Deprecated; as of version 0.3.0 the association keyword is synonomous
          with applying the "all, delete-orphan" cascade to a "one-to-many"
          relationship. SA can now automatically reconcile a "delete" and
          "insert" operation of two objects with the same "identity" in a flush()
          operation into a single "update" statement, which is the pattern that
          "association" used to indicate. 
      
        backref
          indicates the name of a property to be placed on the related mapper's
          class that will handle this relationship in the other direction,
          including synchronizing the object attributes on both sides of the
          relation. Can also point to a ``backref()`` construct for more
          configurability. 
      
        cascade
          a string list of cascade rules which determines how persistence
          operations should be "cascaded" from parent to child. 
      
        collection_class
          a class or function that returns a new list-holding object. will be
          used in place of a plain list for storing elements. 
      
        foreign_keys
          a list of columns which are to be used as "foreign key" columns.
          this parameter should be used in conjunction with explicit
          ``primaryjoin`` and ``secondaryjoin`` (if needed) arguments, and the
          columns within the ``foreign_keys`` list should be present within
          those join conditions. Normally, ``relation()`` will inspect the
          columns within the join conditions to determine which columns are
          the "foreign key" columns, based on information in the ``Table``
          metadata. Use this argument when no ForeignKey's are present in the
          join condition, or to override the table-defined foreign keys.

        foreignkey
          deprecated. use the ``foreign_keys`` argument for foreign key
          specification, or ``remote_side`` for "directional" logic.

        join_depth=None
          when non-``None``, an integer value indicating how many levels
          deep eagerload joins should be constructed on a self-referring
          or cyclical relationship.  The number counts how many times
          the same Mapper shall be present in the loading condition along
          a particular join branch.  When left at its default of ``None``, 
          eager loads will automatically stop chaining joins when they encounter 
          a mapper which is already higher up in the chain.
          
        lazy=(True|False|None|'dynamic')
          specifies how the related items should be loaded. Values include:
          
            True - items should be loaded lazily when the property is first
                   accessed. 
          
            False - items should be loaded "eagerly" in the same query as that 
                    of the parent, using a JOIN or LEFT OUTER JOIN.
                    
            None - no loading should occur at any time.  This is to support
                   "write-only" attrbitutes, or attributes which are populated in
                   some manner specific to the application.
                   
            'dynamic' - a ``DynaLoader`` will be attached, which returns a
                        ``Query`` object for all read operations.  The dynamic-
                        collection supports only ``append()`` and ``remove()``
                        for write operations; changes to the dynamic property 
                        will not be visible until the data is flushed to the 
                        database.  

        order_by
          indicates the ordering that should be applied when loading these items.

        passive_deletes=False
          Indicates the behavior of delete operations. 
          A value of True indicates that unloaded child items should not be loaded
          during a delete operation on the parent.  Normally, when a parent
          item is deleted, all child items are loaded so that they can either be
          marked as deleted, or have their foreign key to the parent set to NULL.
          Marking this flag as True usually implies an ON DELETE <CASCADE|SET NULL>
          rule is in place which will handle updating/deleting child rows on the
          database side.
          
          Additionally, setting the flag to the string value 'all' will disable
          the "nulling out" of the child foreign keys, when there is no delete or
          delete-orphan cascade enabled.  This is typically used when a triggering
          or error raise scenario is in place on the database side.  Note that
          the foreign key attributes on in-session child objects will not be changed
          after a flush occurs so this is a very special use-case setting.

        post_update
          this indicates that the relationship should be handled by a second
          UPDATE statement after an INSERT or before a DELETE. Currently, it also
          will issue an UPDATE after the instance was UPDATEd as well, although
          this technically should be improved. This flag is used to handle saving
          bi-directional dependencies between two individual rows (i.e. each row
          references the other), where it would otherwise be impossible to INSERT
          or DELETE both rows fully since one row exists before the other. Use
          this flag when a particular mapping arrangement will incur two rows
          that are dependent on each other, such as a table that has a
          one-to-many relationship to a set of child rows, and also has a column
          that references a single child row within that list (i.e. both tables
          contain a foreign key to each other). If a ``flush()`` operation returns
          an error that a "cyclical dependency" was detected, this is a cue that
          you might want to use ``post_update`` to "break" the cycle.

        primaryjoin
          a ClauseElement that will be used as the primary join of this child
          object against the parent object, or in a many-to-many relationship the
          join of the primary object to the association table. By default, this
          value is computed based on the foreign key relationships of the parent
          and child tables (or association table).

        private=False
          deprecated. setting ``private=True`` is the equivalent of setting
          ``cascade="all, delete-orphan"``, and indicates the lifecycle of child
          objects should be contained within that of the parent. 

        remote_side
          used for self-referential relationships, indicates the column or list
          of columns that form the "remote side" of the relationship. 

        secondaryjoin
          a ClauseElement that will be used as the join of an association table
          to the child object. By default, this value is computed based on the
          foreign key relationships of the association and child tables.

        uselist=(True|False)
          a boolean that indicates if this property should be loaded as a list or
          a scalar. In most cases, this value is determined automatically by
          ``relation()``, based on the type and direction of the relationship - one
          to many forms a list, many to one forms a scalar, many to many is a
          list. If a scalar is desired where normally a list would be present,
          such as a bi-directional one-to-one relationship, set uselist to False.

        viewonly=False
          when set to True, the relation is used only for loading objects within
          the relationship, and has no effect on the unit-of-work flush process.
          Relations with viewonly can specify any kind of join conditions to
          provide additional views of related objects onto a parent object. Note
          that the functionality of a viewonly relationship has its limits -
          complicated join conditions may not compile into eager or lazy loaders
          properly. If this is the case, use an alternative method.

    """

    return PropertyLoader(argument, secondary=secondary, **kwargs)

def dynamic_loader(argument, secondary=None, primaryjoin=None, secondaryjoin=None, entity_name=None, 
    foreign_keys=None, backref=None, post_update=False, cascade=None, remote_side=None, enable_typechecks=True):
    """construct a dynamically-loading mapper property.
    
    This property is similar to relation(), except read operations
    return an active Query object, which reads from the database in all 
    cases.  Items may be appended to the attribute via append(), or
    removed via remove(); changes will be persisted
    to the database during a flush().  However, no other list mutation
    operations are available.
    
    A subset of arguments available to relation() are available here.
    """

    from sqlalchemy.orm.strategies import DynaLoader
    
    return PropertyLoader(argument, secondary=secondary, primaryjoin=primaryjoin, 
            secondaryjoin=secondaryjoin, entity_name=entity_name, foreign_keys=foreign_keys, backref=backref, 
            post_update=post_update, cascade=cascade, remote_side=remote_side, enable_typechecks=enable_typechecks, 
            strategy_class=DynaLoader)

#def _relation_loader(mapper, secondary=None, primaryjoin=None, secondaryjoin=None, lazy=True, **kwargs):

def column_property(*args, **kwargs):
    """Provide a column-level property for use with a Mapper.

    Column-based properties can normally be applied to the mapper's
    ``properties`` dictionary using the ``schema.Column`` element directly.
    Use this function when the given column is not directly present within
    the mapper's selectable; examples include SQL expressions, functions,
    and scalar SELECT queries.

    Columns that arent present in the mapper's selectable won't be persisted
    by the mapper and are effectively "read-only" attributes.

      \*cols
          list of Column objects to be mapped.
    
      group
          a group name for this property when marked as deferred.
        
      deferred
          when True, the column property is "deferred", meaning that
          it does not load immediately, and is instead loaded when the
          attribute is first accessed on an instance.  See also 
          [sqlalchemy.orm#deferred()].

    """
    
    return ColumnProperty(*args, **kwargs)

def composite(class_, *cols, **kwargs):
    """Return a composite column-based property for use with a Mapper.
    
    This is very much like a column-based property except the given class
    is used to represent "composite" values composed of one or more columns.

    The class must implement a constructor with positional arguments matching
    the order of columns supplied here, as well as a __composite_values__()
    method which returns values in the same order.

    A simple example is representing separate two columns in a table as a
    single, first-class "Point" object::

      class Point(object):
          def __init__(self, x, y):
              self.x = x
              self.y = y
          def __composite_values__(self):
              return (self.x, self.y)

      # and then in the mapping:
      ... composite(Point, mytable.c.x, mytable.c.y) ...

    Arguments are:
    
    class\_
      The "composite type" class.
          
    \*cols
      List of Column objects to be mapped.
      
    group
      A group name for this property when marked as deferred.
          
    deferred
      When True, the column property is "deferred", meaning that
      it does not load immediately, and is instead loaded when the
      attribute is first accessed on an instance.  See also 
      [sqlalchemy.orm#deferred()].
          
    comparator
      An optional instance of [sqlalchemy.orm#PropComparator] which
      provides SQL expression generation functions for this composite
      type.
    """
    
    return CompositeProperty(class_, *cols, **kwargs)
    

def backref(name, **kwargs):
    """Create a BackRef object with explicit arguments, which are the same arguments one
    can send to ``relation()``.

    Used with the `backref` keyword argument to ``relation()`` in
    place of a string argument.
    """

    return BackRef(name, **kwargs)

def deferred(*columns, **kwargs):
    """Return a ``DeferredColumnProperty``, which indicates this
    object attributes should only be loaded from its corresponding
    table column when first accessed.

    Used with the `properties` dictionary sent to ``mapper()``.
    """

    return ColumnProperty(deferred=True, *columns, **kwargs)

def mapper(class_, local_table=None, *args, **params):
    """Return a new [sqlalchemy.orm#Mapper] object.

      class\_
        The class to be mapped.

      local_table
        The table to which the class is mapped, or None if this
        mapper inherits from another mapper using concrete table
        inheritance.

      entity_name
        A name to be associated with the `class`, to allow alternate
        mappings for a single class.

      always_refresh
        If True, all query operations for this mapped class will
        overwrite all data within object instances that already
        exist within the session, erasing any in-memory changes with
        whatever information was loaded from the database.  Usage
        of this flag is highly discouraged; as an alternative, 
        see the method `populate_existing()` on [sqlalchemy.orm.query#Query].

      allow_column_override
        If True, allows the usage of a ``relation()`` which has the
        same name as a column in the mapped table.  The table column
        will no longer be mapped.

      allow_null_pks
        Indicates that composite primary keys where one or more (but
        not all) columns contain NULL is a valid primary key.
        Primary keys which contain NULL values usually indicate that
        a result row does not contain an entity and should be
        skipped.

      batch
        Indicates that save operations of multiple entities can be
        batched together for efficiency.  setting to False indicates
        that an instance will be fully saved before saving the next
        instance, which includes inserting/updating all table rows
        corresponding to the entity as well as calling all
        ``MapperExtension`` methods corresponding to the save
        operation.

      column_prefix
        A string which will be prepended to the `key` name of all
        Columns when creating column-based properties from the given
        Table.  Does not affect explicitly specified column-based
        properties

      concrete
        If True, indicates this mapper should use concrete table
        inheritance with its parent mapper.

      extension
        A [sqlalchemy.orm#MapperExtension] instance or list of
        ``MapperExtension`` instances which will be applied to all
        operations by this ``Mapper``.

      inherits
        Another ``Mapper`` for which this ``Mapper`` will have an
        inheritance relationship with.

      inherit_condition
        For joined table inheritance, a SQL expression (constructed
        ``ClauseElement``) which will define how the two tables are
        joined; defaults to a natural join between the two tables.

      inherit_foreign_keys
        when inherit_condition is used and the condition contains no
        ForeignKey columns, specify the "foreign" columns of the join 
        condition in this list.  else leave as None.
        
      order_by
        A single ``Column`` or list of ``Columns`` for which
        selection operations should use as the default ordering for
        entities.  Defaults to the OID/ROWID of the table if any, or
        the first primary key column of the table.

      non_primary
        Construct a ``Mapper`` that will define only the selection
        of instances, not their persistence.  Any number of non_primary
        mappers may be created for a particular class.

      polymorphic_on
        Used with mappers in an inheritance relationship, a ``Column``
        which will identify the class/mapper combination to be used
        with a particular row.  requires the polymorphic_identity
        value to be set for all mappers in the inheritance
        hierarchy.

      _polymorphic_map
        Used internally to propigate the full map of polymorphic
        identifiers to surrogate mappers.

      polymorphic_identity
        A value which will be stored in the Column denoted by
        polymorphic_on, corresponding to the *class identity* of
        this mapper.

      polymorphic_fetch
        specifies how subclasses mapped through joined-table 
        inheritance will be fetched.  options are 'union', 
        'select', and 'deferred'.  if the select_table argument 
        is present, defaults to 'union', otherwise defaults to
        'select'.

      properties
        A dictionary mapping the string names of object attributes
        to ``MapperProperty`` instances, which define the
        persistence behavior of that attribute.  Note that the
        columns in the mapped table are automatically converted into
        ``ColumnProperty`` instances based on the `key` property of
        each ``Column`` (although they can be overridden using this
        dictionary).

      include_properties
        An inclusive list of properties to map.  Columns present in the
        mapped table but not present in this list will not be automatically
        converted into properties.

      exclude_properties
        A list of properties not to map.  Columns present in the
        mapped table and present in this list will not be automatically
        converted into properties.  Note that neither this option nor
        include_properties will allow an end-run around Python inheritance.
        If mapped class ``B`` inherits from mapped class ``A``, no combination
        of includes or excludes will allow ``B`` to have fewer properties
        than its superclass, ``A``.

      primary_key
        A list of ``Column`` objects which define the *primary key*
        to be used against this mapper's selectable unit.  This is
        normally simply the primary key of the `local_table`, but
        can be overridden here.

      select_table
        A [sqlalchemy.schema#Table] or any [sqlalchemy.sql#Selectable] 
        which will be used to select instances of this mapper's class.  
        usually used to provide polymorphic loading among several 
        classes in an inheritance hierarchy.

      version_id_col
        A ``Column`` which must have an integer type that will be
        used to keep a running *version id* of mapped entities in
        the database.  this is used during save operations to ensure
        that no other thread or process has updated the instance
        during the lifetime of the entity, else a
        ``ConcurrentModificationError`` exception is thrown.
    """

    return Mapper(class_, local_table, *args, **params)

def synonym(name, proxy=False):
    """Set up `name` as a synonym to another ``MapperProperty``.

    Used with the `properties` dictionary sent to ``mapper()``.
    """

    return SynonymProperty(name, proxy=proxy)

def compile_mappers():
    """Compile all mappers that have been defined.

    This is equivalent to calling ``compile()`` on any individual mapper.
    """

    if not mapper_registry:
        return
    mapper_registry.values()[0].compile()

def clear_mappers():
    """Remove all mappers that have been created thus far.

    The mapped classes will return to their initial "unmapped"
    state and can be re-mapped with new mappers.
    """
    mapperlib._COMPILE_MUTEX.acquire()
    try:
        for mapper in mapper_registry.values():
            mapper.dispose()
        mapper_registry.clear()
        mapperlib.ClassKey.dispose(mapperlib.ClassKey)
        from sqlalchemy.orm import dependency
        dependency.MapperStub.dispose(dependency.MapperStub)
    finally:
        mapperlib._COMPILE_MUTEX.release()
        
def extension(ext):
    """Return a ``MapperOption`` that will insert the given
    ``MapperExtension`` to the beginning of the list of extensions
    that will be called in the context of the ``Query``.

    Used with ``query.options()``.
    """

    return ExtensionOption(ext)

def eagerload(name):
    """Return a ``MapperOption`` that will convert the property of the given name into an eager load.

    Used with ``query.options()``.
    """

    return strategies.EagerLazyOption(name, lazy=False)

def eagerload_all(name):
    """Return a ``MapperOption`` that will convert all properties along the given dot-separated path into an eager load.
    
    For example, this::

        query.options(eagerload_all('orders.items.keywords'))...
        
    will set all of 'orders', 'orders.items', and 'orders.items.keywords'
    to load in one eager load.

    Used with ``query.options()``.
    """

    return strategies.EagerLazyOption(name, lazy=False, chained=True)

def lazyload(name):
    """Return a ``MapperOption`` that will convert the property of the
    given name into a lazy load.

    Used with ``query.options()``.
    """

    return strategies.EagerLazyOption(name, lazy=True)

def fetchmode(name, type):
    return strategies.FetchModeOption(name, type)
    
def noload(name):
    """Return a ``MapperOption`` that will convert the property of the
    given name into a non-load.

    Used with ``query.options()``.
    """

    return strategies.EagerLazyOption(name, lazy=None)

def contains_alias(alias):
    """Return a ``MapperOption`` that will indicate to the query that
    the main table has been aliased.

    `alias` is the string name or ``Alias`` object representing the
    alias.
    """

    class AliasedRow(MapperExtension):
        def __init__(self, alias):
            self.alias = alias
            if isinstance(self.alias, basestring):
                self.selectable = None
            else:
                self.selectable = alias
            self._row_translators = {}
        def get_selectable(self, mapper):
            if self.selectable is None:
                self.selectable = mapper.mapped_table.alias(self.alias)
            return self.selectable
        def translate_row(self, mapper, context, row):
            if mapper in self._row_translators:
                return self._row_translators[mapper](row)
            else:
                translator = create_row_adapter(self.get_selectable(mapper), mapper.mapped_table)
                self._row_translators[mapper] = translator
                return translator(row)

    return ExtensionOption(AliasedRow(alias))

def contains_eager(key, alias=None, decorator=None):
    """Return a ``MapperOption`` that will indicate to the query that
    the given attribute will be eagerly loaded.

    Used when feeding SQL result sets directly into
    ``query.instances()``.  Also bundles an ``EagerLazyOption`` to
    turn on eager loading in case it isnt already.

    `alias` is the string name of an alias, **or** an ``sql.Alias``
    object, which represents the aliased columns in the query.  This
    argument is optional.

    `decorator` is mutually exclusive of `alias` and is a
    row-processing function which will be applied to the incoming row
    before sending to the eager load handler.  use this for more
    sophisticated row adjustments beyond a straight alias.
    """

    return (strategies.EagerLazyOption(key, lazy=False), strategies.RowDecorateOption(key, alias=alias, decorator=decorator))

def defer(name):
    """Return a ``MapperOption`` that will convert the column property
    of the given name into a deferred load.

    Used with ``query.options()``"""
    return strategies.DeferredOption(name, defer=True)

def undefer(name):
    """Return a ``MapperOption`` that will convert the column property
    of the given name into a non-deferred (regular column) load.

    Used with ``query.options()``.
    """

    return strategies.DeferredOption(name, defer=False)

def undefer_group(name):
    """Return a ``MapperOption`` that will convert the given 
    group of deferred column properties into a non-deferred (regular column) load.

    Used with ``query.options()``.
    """
    return strategies.UndeferGroupOption(name)
    
