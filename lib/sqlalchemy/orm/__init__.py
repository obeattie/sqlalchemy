# mapper/__init__.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
the mapper package provides object-relational functionality, building upon the schema and sql
packages and tying operations to class properties and constructors.
"""
from sqlalchemy import sql, schema, engine, util, exceptions
from mapper import *
from mapper import mapper_registry
import mapper as mapperlib
from query import Query
from util import polymorphic_union
import properties, strategies
from session import Session as create_session

__all__ = ['relation', 'backref', 'eagerload', 'lazyload', 'noload', 'deferred', 'defer', 'undefer',
        'mapper', 'clear_mappers', 'clear_mapper', 'sql', 'class_mapper', 'object_mapper', 'MapperExtension', 'Query', 
        'cascade_mappers', 'polymorphic_union', 'create_session', 'synonym', 'EXT_PASS'
        ]

def relation(*args, **kwargs):
    """provide a relationship of a primary Mapper to a secondary Mapper.
    
    This corresponds to a parent-child or associative table relationship."""
    if len(args) > 1 and isinstance(args[0], type):
        raise exceptions.ArgumentError("relation(class, table, **kwargs) is deprecated.  Please use relation(class, **kwargs) or relation(mapper, **kwargs).")
    return _relation_loader(*args, **kwargs)

def _relation_loader(mapper, secondary=None, primaryjoin=None, secondaryjoin=None, lazy=True, **kwargs):
    return properties.PropertyLoader(mapper, secondary, primaryjoin, secondaryjoin, lazy=lazy, **kwargs)

def backref(name, **kwargs):
    return properties.BackRef(name, **kwargs)
    
def deferred(*columns, **kwargs):
    """return a DeferredColumnProperty, which indicates this object attributes should only be loaded 
    from its corresponding table column when first accessed."""
    return properties.ColumnProperty(deferred=True, *columns, **kwargs)
    
def mapper(class_, table=None, *args, **params):
    """return a new Mapper object."""
    return Mapper(class_, table, *args, **params)

def synonym(name):
    """set up 'name' as a synonym to another MapperProperty."""
    return properties.SynonymProperty(name)
    
def clear_mappers():
    """remove all mappers that have been created thus far.  when new mappers are 
    created, they will be assigned to their classes as their primary mapper."""
    mapper_registry.clear()
    
def clear_mapper(m):
    """remove the given mapper from the storage of mappers.  when a new mapper is 
    created for the previous mapper's class, it will be used as that classes' 
    new primary mapper."""
    del mapper_registry[m.class_key]

def eagerload(name):
    """return a MapperOption that will convert the property of the given name
    into an eager load."""
    return strategies.EagerLazyOption(name, lazy=False)

def lazyload(name):
    """return a MapperOption that will convert the property of the given name
    into a lazy load"""
    return strategies.EagerLazyOption(name, lazy=True)

def noload(name):
    """return a MapperOption that will convert the property of the given name
    into a non-load."""
    return strategies.EagerLazyOption(name, lazy=None)

def defer(name):
    """returns a MapperOption that will convert the column property of the given 
    name into a deferred load.  Used with mapper.options()"""
    return strategies.DeferredOption(name, defer=True)
def undefer(name):
    """returns a MapperOption that will convert the column property of the given
    name into a non-deferred (regular column) load.  Used with mapper.options."""
    return strategies.DeferredOption(name, defer=False)
    


    
def cascade_mappers(*classes_or_mappers):
    """given a list of classes and/or mappers, identifies the foreign key relationships
    between the given mappers or corresponding class mappers, and creates relation()
    objects representing those relationships, including a backreference.  Attempts to find
    the "secondary" table in a many-to-many relationship as well.  The names of the relations
    will be a lowercase version of the related class.  In the case of one-to-many or many-to-many,
    the name will be "pluralized", which currently is based on the English language (i.e. an 's' or 
    'es' added to it)."""
    table_to_mapper = {}
    for item in classes_or_mappers:
        if isinstance(item, Mapper):
            m = item
        else:
            klass = item
            m = class_mapper(klass)
        table_to_mapper[m.mapped_table] = m
    def pluralize(name):
        # oh crap, do we need locale stuff now
        if name[-1] == 's':
            return name + "es"
        else:
            return name + "s"
    for table,mapper in table_to_mapper.iteritems():
        for fk in table.foreign_keys:
            if fk.column.table is table:
                continue
            secondary = None
            try:
                m2 = table_to_mapper[fk.column.table]
            except KeyError:
                if len(fk.column.table.primary_key):
                    continue
                for sfk in fk.column.table.foreign_keys:
                    if sfk.column.table is table:
                        continue
                    m2 = table_to_mapper.get(sfk.column.table)
                    secondary = fk.column.table
            if m2 is None:
                continue
            if secondary:
                propname = pluralize(m2.class_.__name__.lower())
                propname2 = pluralize(mapper.class_.__name__.lower())
            else:
                propname = m2.class_.__name__.lower()
                propname2 = pluralize(mapper.class_.__name__.lower())
            mapper.add_property(propname, relation(m2, secondary=secondary, backref=propname2))
            
            