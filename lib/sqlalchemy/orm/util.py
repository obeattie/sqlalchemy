# mapper/util.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy import sql, util, exceptions
from sqlalchemy.orm.interfaces import MapperExtension, EXT_PASS

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

class TranslatingDict(dict):
    """A dictionary that stores ``ColumnElement`` objects as keys.

    Incoming ``ColumnElement`` keys are translated against those of an
    underling ``FromClause`` for all operations.  This way the columns
    from any ``Selectable`` that is derived from or underlying this
    ``TranslatingDict`` 's selectable can be used as keys.
    """

    def __init__(self, selectable):
        super(TranslatingDict, self).__init__()
        self.selectable = selectable

    def __translate_col(self, col):
        ourcol = self.selectable.corresponding_column(col, keys_ok=False, raiseerr=False)
        if ourcol is None:
            return col
        else:
            return ourcol

    def __getitem__(self, col):
        return super(TranslatingDict, self).__getitem__(self.__translate_col(col))

    def has_key(self, col):
        return super(TranslatingDict, self).has_key(self.__translate_col(col))

    def __setitem__(self, col, value):
        return super(TranslatingDict, self).__setitem__(self.__translate_col(col), value)

    def __contains__(self, col):
        return self.has_key(col)

    def setdefault(self, col, value):
        return super(TranslatingDict, self).setdefault(self.__translate_col(col), value)

class ExtensionCarrier(MapperExtension):
    def __init__(self, _elements=None):
        self.__elements = _elements or []

    def copy(self):
        return ExtensionCarrier(list(self.__elements))
        
    def __iter__(self):
        return iter(self.__elements)

    def insert(self, extension):
        """Insert a MapperExtension at the beginning of this ExtensionCarrier's list."""

        self.__elements.insert(0, extension)

    def append(self, extension):
        """Append a MapperExtension at the end of this ExtensionCarrier's list."""

        self.__elements.append(extension)

    def _create_do(funcname):
        def _do(self, *args, **kwargs):
            for elem in self.__elements:
                ret = getattr(elem, funcname)(*args, **kwargs)
                if ret is not EXT_PASS:
                    return ret
            else:
                return EXT_PASS
        return _do

    init_instance = _create_do('init_instance')
    init_failed = _create_do('init_failed')
    dispose_class = _create_do('dispose_class')
    get_session = _create_do('get_session')
    load = _create_do('load')
    get = _create_do('get')
    get_by = _create_do('get_by')
    select_by = _create_do('select_by')
    select = _create_do('select')
    translate_row = _create_do('translate_row')
    create_instance = _create_do('create_instance')
    append_result = _create_do('append_result')
    populate_instance = _create_do('populate_instance')
    before_insert = _create_do('before_insert')
    before_update = _create_do('before_update')
    after_update = _create_do('after_update')
    after_insert = _create_do('after_insert')
    before_delete = _create_do('before_delete')
    after_delete = _create_do('after_delete')

class BinaryVisitor(sql.ClauseVisitor):
    def __init__(self, func):
        self.func = func

    def visit_binary(self, binary):
        self.func(binary)

def instance_str(instance):
    """Return a string describing an instance."""

    return instance.__class__.__name__ + "@" + hex(id(instance))

def attribute_str(instance, attribute):
    return instance_str(instance) + "." + attribute
