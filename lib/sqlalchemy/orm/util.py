# mapper/util.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy import sql, util, exceptions
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql.util import row_adapter as create_row_adapter
from sqlalchemy.sql import visitors
from sqlalchemy.orm.interfaces import MapperExtension, EXT_CONTINUE

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
    """Creates aliases of a mapped tables for usage in ORM queries.
    """

    def __init__(self, alias, equivalents=None, chain_to=None):
        self.alias = alias
        self.equivalents = equivalents
        self.row_decorator = self._create_row_adapter()
        self.adapter = sql_util.ClauseAdapter(self.alias, equivalents=equivalents)
        if chain_to:
            self.adapter.chain(chain_to.adapter)
            
    def aliased_column(self, column):
        
        conv = self.alias.corresponding_column(column)
        if conv:
            return conv
            
        aliased_column = column
        class ModifySubquery(visitors.ClauseVisitor):
            def visit_select(s, select):
                select._should_correlate = False
                select.append_correlation(self.alias)
        aliased_column = sql_util.ClauseAdapter(self.alias, equivalents=self.equivalents).chain(ModifySubquery()).traverse(aliased_column, clone=True)
        aliased_column = aliased_column.label(None)
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
    
    def __init__(self, prop, primaryjoin, secondaryjoin, parentclauses=None, alias=None):
        self.prop = prop
        self.mapper = self.prop.mapper
        self.table = self.prop.table
        self.parentclauses = parentclauses

        if not alias:
            from_obj = self.mapper._with_polymorphic_selectable()
            alias = from_obj.alias()

        super(PropertyAliasedClauses, self).__init__(alias, equivalents=self.mapper._equivalent_columns, chain_to=parentclauses)
        
        if prop.secondary:
            self.secondary = prop.secondary.alias()
            if parentclauses is not None:
                primary_aliasizer = sql_util.ClauseAdapter(self.secondary).chain(sql_util.ClauseAdapter(parentclauses.alias, equivalents=parentclauses.equivalents))
                secondary_aliasizer = sql_util.ClauseAdapter(self.alias, equivalents=self.equivalents).chain(sql_util.ClauseAdapter(self.secondary))

            else:
                primary_aliasizer = sql_util.ClauseAdapter(self.secondary)
                secondary_aliasizer = sql_util.ClauseAdapter(self.alias, equivalents=self.equivalents).chain(sql_util.ClauseAdapter(self.secondary))
                
            self.secondaryjoin = secondary_aliasizer.traverse(secondaryjoin, clone=True)
            self.primaryjoin = primary_aliasizer.traverse(primaryjoin, clone=True)
        else:
            if parentclauses is not None: 
                primary_aliasizer = sql_util.ClauseAdapter(self.alias, exclude=prop.local_side, equivalents=self.equivalents)
                primary_aliasizer.chain(sql_util.ClauseAdapter(parentclauses.alias, exclude=prop.remote_side, equivalents=parentclauses.equivalents))
            else:
                primary_aliasizer = sql_util.ClauseAdapter(self.alias, exclude=prop.local_side, equivalents=self.equivalents)
            
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

def state_attribute_str(state, attribute):
    return state_str(state) + "." + attribute

def identity_equal(a, b):
    if a is b:
        return True
    if a is None or b is None:
        return False
    try:
        state_a = attributes.state_of(a)
        state_b = attributes.state_of(b)
    except (KeyError, AttributeError):
        return False
    if state_a.key is None or state_b.key is None:
        return False
    return state_a.key == state_b.key

# TODO: Avoid circular import.
from sqlalchemy.orm import attributes
