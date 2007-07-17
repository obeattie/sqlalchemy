# objectstore.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy import util, exceptions, sql, engine
from sqlalchemy.orm import unitofwork, query
from sqlalchemy.orm.mapper import object_mapper as _object_mapper
from sqlalchemy.orm.mapper import class_mapper as _class_mapper
import weakref
import sqlalchemy

class SessionTransaction(object):
    """Represents a Session-level Transaction.

    This corresponds to one or more sqlalchemy.engine.Transaction
    instances behind the scenes, with one Transaction per Engine in
    use.

    The SessionTransaction object is **not** threadsafe.
    """

    def __init__(self, session, parent=None, autoflush=True, nested=False):
        self.session = session
        self.__connections = {}
        self.__parent = parent
        self.autoflush = autoflush
        self.nested = nested

    def connection(self, mapper_or_class, entity_name=None):
        if isinstance(mapper_or_class, type):
            mapper_or_class = _class_mapper(mapper_or_class, entity_name=entity_name)
        engine = self.session.get_bind(mapper_or_class)
        return self.get_or_add(engine)

    def _begin(self, **kwargs):
        return SessionTransaction(self.session, self, **kwargs)

    def add(self, bind):
        if self.__parent is not None:
            return self.__parent.add(bind)
            
        if self.__connections.has_key(bind.engine):
            raise exceptions.InvalidRequestError("Session already has a Connection associated for the given %sEngine" % (isinstance(bind, engine.Connection) and "Connection's " or ""))
        return self.get_or_add(bind)

    def _connection_dict(self):
        if self.__parent is not None and not self.nested:
            return self.__parent._connection_dict()
        else:
            return self.__connections
            
    def get_or_add(self, bind):
        if self.__parent is not None:
            if not self.nested:
                return self.__parent.get_or_add(bind)
            
            if self.__connections.has_key(bind):
                return self.__connections[bind][0]

            if bind in self.__parent._connection_dict():
                (conn, trans, autoclose) = self.__parent.__connections[bind]
                self.__connections[conn] = self.__connections[bind.engine] = (conn, conn.begin_nested(), autoclose)
                return conn
        elif self.__connections.has_key(bind):
            return self.__connections[bind][0]
            
        if not isinstance(bind, engine.Connection):
            e = bind
            c = bind.contextual_connect()
        else:
            e = bind.engine
            c = bind
            if e in self.__connections:
                raise exceptions.InvalidRequestError("Session already has a Connection associated for the given Connection's Engine")
        if self.nested:
            trans = c.begin_nested()
        else:
            trans = c.begin()
        self.__connections[c] = self.__connections[e] = (c, trans, c is not bind)
        return self.__connections[c][0]

    def commit(self):
        if self.__parent is not None and not self.nested:
            return self.__parent
        if self.autoflush:
            self.session.flush()
        for t in util.Set(self.__connections.values()):
            t[1].commit()
        self.close()
        return self.__parent

    def rollback(self):
        if self.__parent is not None and not self.nested:
            return self.__parent.rollback()
        for t in util.Set(self.__connections.values()):
            t[1].rollback()
        self.close()
        return self.__parent
        
    def close(self):
        if self.__parent is not None:
            return
        for t in util.Set(self.__connections.values()):
            if t[2]:
                t[0].close()
        self.session.transaction = None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self.session.transaction is None:
            return
        if type is None:
            self.commit()
        else:
            self.rollback()

class Session(object):
    """Encapsulates a set of objects being operated upon within an
    object-relational operation.

    The Session object is **not** threadsafe.  For thread-management
    of Sessions, see the ``sqlalchemy.ext.sessioncontext`` module.
    """

    def __init__(self, bind=None, autoflush=False, transactional=False, echo_uow=False, weak_identity_map=False):
        self.uow = unitofwork.UnitOfWork(weak_identity_map=weak_identity_map)

        self.bind = bind
        self.binds = {}
        self.echo_uow = echo_uow
        self.weak_identity_map = weak_identity_map
        self.transaction = None
        self.hash_key = id(self)
        self.autoflush = autoflush
        self.transactional = transactional or autoflush
        if self.transactional:
            self.begin()
        _sessions[self.hash_key] = self
            
    def _get_echo_uow(self):
        return self.uow.echo

    def _set_echo_uow(self, value):
        self.uow.echo = value
    echo_uow = property(_get_echo_uow,_set_echo_uow)
    
    def begin(self, **kwargs):
        """Begin a transaction on this Session."""

        if self.transaction is not None:
            self.transaction = self.transaction._begin(**kwargs)
        else:
            self.transaction = SessionTransaction(self, **kwargs)
    create_transaction = begin

    def begin_nested(self):
        return self.begin(nested=True)
    
    def rollback(self):
        if self.transaction is None:
            raise exceptions.InvalidRequestError("No transaction is begun.")
        else:
            self.transaction = self.transaction.rollback()
        if self.transaction is None and self.transactional:
            self.begin()
            
    def commit(self):
        if self.transaction is None:
            raise exceptions.InvalidRequestError("No transaction is begun.")
        else:
            self.transaction = self.transaction.commit()
        if self.transaction is None and self.transactional:
            self.begin()
        
    def connect(self, mapper=None, **kwargs):
        """Return a unique connection corresponding to the given mapper.

        This connection will not be part of any pre-existing
        transactional context.
        """

        return self.get_bind(mapper).connect(**kwargs)

    def connection(self, mapper, **kwargs):
        """Return a ``Connection`` corresponding to the given mapper.

        Used by the ``execute()`` method which performs select
        operations for ``Mapper`` and ``Query``.

        If this ``Session`` is transactional, the connection will be in
        the context of this session's transaction.  Otherwise, the
        connection is returned by the ``contextual_connect()`` method, which
        some Engines override to return a thread-local connection, and
        will have `close_with_result` set to `True`.

        The given `**kwargs` will be sent to the engine's
        ``contextual_connect()`` method, if no transaction is in
        progress.
        """

        if self.transaction is not None:
            return self.transaction.connection(mapper)
        else:
            return self.get_bind(mapper).contextual_connect(**kwargs)

    def execute(self, mapper, clause, params, **kwargs):
        """Using the given mapper to identify the appropriate ``Engine``
        or ``Connection`` to be used for statement execution, execute the
        given ``ClauseElement`` using the provided parameter dictionary.

        Return a ``ResultProxy`` corresponding to the execution's results.

        If this method allocates a new ``Connection`` for the operation,
        then the ``ResultProxy`` 's ``close()`` method will release the
        resources of the underlying ``Connection``, otherwise its a no-op.
        """
        return self.connection(mapper, close_with_result=True).execute(clause, params, **kwargs)

    def scalar(self, mapper, clause, params, **kwargs):
        """Like execute() but return a scalar result."""

        return self.connection(mapper, close_with_result=True).scalar(clause, params, **kwargs)

    def close(self):
        """Close this Session."""

        self.clear()
        if self.transaction is not None:
            self.transaction.close()

    def clear(self):
        """Remove all object instances from this ``Session``.

        This is equivalent to calling ``expunge()`` for all objects in
        this ``Session``.
        """

        for instance in self:
            self._unattach(instance)
        echo = self.uow.echo
        self.uow = unitofwork.UnitOfWork(weak_identity_map=self.weak_identity_map)
        self.uow.echo = echo

    def mapper(self, class_, entity_name=None):
        """Given a ``Class``, return the primary ``Mapper`` responsible for
        persisting it."""

        return _class_mapper(class_, entity_name = entity_name)

    def bind_mapper(self, mapper, bind):
        """Bind the given `mapper` to the given ``Engine`` or ``Connection``.

        All subsequent operations involving this ``Mapper`` will use the
        given `bind`.
        """

        self.binds[mapper] = bind

    def bind_table(self, table, bind):
        """Bind the given `table` to the given ``Engine`` or ``Connection``.

        All subsequent operations involving this ``Table`` will use the
        given `bind`.
        """

        self.binds[table] = bind

    def get_bind(self, mapper):
        """Return the ``Engine`` or ``Connection`` which is used to execute
        statements on behalf of the given `mapper`.

        Calling ``connect()`` on the return result will always result
        in a ``Connection`` object.  This method disregards any
        ``SessionTransaction`` that may be in progress.

        The order of searching is as follows:

        1. if an ``Engine`` or ``Connection`` was bound to this ``Mapper``
           specifically within this ``Session``, return that ``Engine`` or
           ``Connection``.

        2. if an ``Engine`` or ``Connection`` was bound to this `mapper` 's
           underlying ``Table`` within this ``Session`` (i.e. not to the ``Table``
           directly), return that ``Engine`` or ``Connection``.

        3. if an ``Engine`` or ``Connection`` was bound to this ``Session``,
           return that ``Engine`` or ``Connection``.

        4. finally, return the ``Engine`` which was bound directly to the
           ``Table`` 's ``MetaData`` object.

        If no ``Engine`` is bound to the ``Table``, an exception is raised.
        """

        if mapper is None:
            return self.bind
        elif self.binds.has_key(mapper):
            return self.binds[mapper]
        elif self.binds.has_key(mapper.mapped_table):
            return self.binds[mapper.mapped_table]
        elif self.bind is not None:
            return self.bind
        else:
            e = mapper.mapped_table.bind
            if e is None:
                raise exceptions.InvalidRequestError("Could not locate any Engine or Connection bound to mapper '%s'" % str(mapper))
            return e

    def query(self, mapper_or_class, *addtl_entities, **kwargs):
        """Return a new ``Query`` object corresponding to this ``Session`` and
        the mapper, or the classes' primary mapper.
        """
        
        entity_name = kwargs.pop('entity_name', None)
        
        if isinstance(mapper_or_class, type):
            q = query.Query(_class_mapper(mapper_or_class, entity_name=entity_name), self, **kwargs)
        else:
            q = query.Query(mapper_or_class, self, **kwargs)
            
        for ent in addtl_entities:
            q = q.add_entity(ent)
        return q

    def _sql(self):
        class SQLProxy(object):
            def __getattr__(self, key):
                def call(*args, **kwargs):
                    kwargs[engine] = self.engine
                    return getattr(sql, key)(*args, **kwargs)

    sql = property(_sql)

    def flush(self, objects=None):
        """Flush all the object modifications present in this session
        to the database.

        `objects` is a list or tuple of objects specifically to be
        flushed; if ``None``, all new and modified objects are flushed.
        """

        self.uow.flush(self, objects)

    def get(self, class_, ident, **kwargs):
        """Return an instance of the object based on the given
        identifier, or ``None`` if not found.

        The `ident` argument is a scalar or tuple of primary key
        column values in the order of the table def's primary key
        columns.

        The `entity_name` keyword argument may also be specified which
        further qualifies the underlying Mapper used to perform the
        query.
        """

        entity_name = kwargs.pop('entity_name', None)
        return self.query(class_, entity_name=entity_name).get(ident, **kwargs)

    def load(self, class_, ident, **kwargs):
        """Return an instance of the object based on the given
        identifier.

        If not found, raises an exception.  The method will **remove
        all pending changes** to the object already existing in the
        ``Session``.  The `ident` argument is a scalar or tuple of primary
        key columns in the order of the table def's primary key
        columns.

        The `entity_name` keyword argument may also be specified which
        further qualifies the underlying ``Mapper`` used to perform the
        query.
        """

        entity_name = kwargs.pop('entity_name', None)
        return self.query(class_, entity_name=entity_name).load(ident, **kwargs)

    def refresh(self, obj):
        """Reload the attributes for the given object from the
        database, clear any changes made.
        """

        self._validate_persistent(obj)
        if self.query(obj.__class__)._get(obj._instance_key, reload=True) is None:
            raise exceptions.InvalidRequestError("Could not refresh instance '%s'" % repr(obj))

    def expire(self, obj):
        """Mark the given object as expired.

        This will add an instrumentation to all mapped attributes on
        the instance such that when an attribute is next accessed, the
        session will reload all attributes on the instance from the
        database.
        """

        for c in [obj] + list(_object_mapper(obj).cascade_iterator('refresh-expire', obj)):
            self._expire_impl(c)

    def _expire_impl(self, obj):
        self._validate_persistent(obj)

        def exp():
            if self.query(obj.__class__)._get(obj._instance_key, reload=True) is None:
                raise exceptions.InvalidRequestError("Could not refresh instance '%s'" % repr(obj))

        attribute_manager.trigger_history(obj, exp)

    def is_expired(self, obj, unexpire=False):
        """Return True if the given object has been marked as expired."""

        ret = attribute_manager.has_trigger(obj)
        if ret and unexpire:
            attribute_manager.untrigger_history(obj)
        return ret

    def expunge(self, object):
        """Remove the given `object` from this ``Session``.

        This will free all internal references to the object.
        Cascading will be applied according to the *expunge* cascade
        rule.
        """
        self._validate_persistent(object)
        for c in [object] + list(_object_mapper(object).cascade_iterator('expunge', object)):
            if c in self:
                self.uow._remove_deleted(c)
                self._unattach(c)

    def save(self, object, entity_name=None):
        """Add a transient (unsaved) instance to this ``Session``.

        This operation cascades the `save_or_update` method to
        associated instances if the relation is mapped with
        ``cascade="save-update"``.

        The `entity_name` keyword argument will further qualify the
        specific ``Mapper`` used to handle this instance.
        """

        self._save_impl(object, entity_name=entity_name)
        _object_mapper(object).cascade_callable('save-update', object,
                                                lambda c, e:self._save_or_update_impl(c, e),
                                                halt_on=lambda c:c in self)

    def update(self, object, entity_name=None):
        """Bring the given detached (saved) instance into this
        ``Session``.

        If there is a persistent instance with the same identifier
        already associated with this ``Session``, an exception is thrown.

        This operation cascades the `save_or_update` method to
        associated instances if the relation is mapped with
        ``cascade="save-update"``.
        """

        self._update_impl(object, entity_name=entity_name)
        _object_mapper(object).cascade_callable('save-update', object,
                                                lambda c, e:self._save_or_update_impl(c, e),
                                                halt_on=lambda c:c in self)

    def save_or_update(self, object, entity_name=None):
        """Save or update the given object into this ``Session``.

        The presence of an `_instance_key` attribute on the instance
        determines whether to ``save()`` or ``update()`` the instance.
        """

        self._save_or_update_impl(object, entity_name=entity_name)
        _object_mapper(object).cascade_callable('save-update', object,
                                                lambda c, e:self._save_or_update_impl(c, e),
                                                halt_on=lambda c:c in self)

    def _save_or_update_impl(self, object, entity_name=None):
        key = getattr(object, '_instance_key', None)
        if key is None:
            self._save_impl(object, entity_name=entity_name)
        else:
            self._update_impl(object, entity_name=entity_name)

    def delete(self, object):
        """Mark the given instance as deleted.

        The delete operation occurs upon ``flush()``.
        """

        for c in [object] + list(_object_mapper(object).cascade_iterator('delete', object)):
            self.uow.register_deleted(c)

    def merge(self, object, entity_name=None, _recursive=None):
        """Copy the state of the given `object` onto the persistent
        object with the same identifier.

        If there is no persistent instance currently associated with
        the session, it will be loaded.  Return the persistent
        instance. If the given instance is unsaved, save a copy of and
        return it as a newly persistent instance. The given instance
        does not become associated with the session.

        This operation cascades to associated instances if the
        association is mapped with ``cascade="merge"``.
        """

        if _recursive is None:
            _recursive = util.Set()
        if entity_name is not None:
            mapper = _class_mapper(object.__class__, entity_name=entity_name)
        else:
            mapper = _object_mapper(object)
        if mapper in _recursive or object in _recursive:
            return None
        _recursive.add(mapper)
        _recursive.add(object)
        try:
            key = getattr(object, '_instance_key', None)
            if key is None:
                merged = mapper._create_instance(self)
            else:
                if key in self.identity_map:
                    merged = self.identity_map[key]
                else:
                    merged = self.get(mapper.class_, key[1])
                    if merged is None:
                        raise exceptions.AssertionError("Instance %s has an instance key but is not persisted" % mapperutil.instance_str(object))
            for prop in mapper.iterate_properties:
                prop.merge(self, object, merged, _recursive)
            if key is None:
                self.save(merged, entity_name=mapper.entity_name)
            return merged
        finally:
            _recursive.remove(mapper)
            
    def identity_key(self, *args, **kwargs):
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
            mapper = _class_mapper(class_, entity_name=entity_name)
            if "ident" in locals():
                return mapper.identity_key_from_primary_key(ident)
            return mapper.identity_key_from_row(row)
        instance = kwargs.pop("instance")
        if kwargs:
            raise exceptions.ArgumentError("unknown keyword arguments: %s"
                % ", ".join(kwargs.keys()))
        mapper = _object_mapper(instance)
        return mapper.identity_key_from_instance(instance)

    def _save_impl(self, object, **kwargs):
        if hasattr(object, '_instance_key'):
            if not self.identity_map.has_key(object._instance_key):
                raise exceptions.InvalidRequestError("Instance '%s' is a detached instance "
                                                     "or is already persistent in a "
                                                     "different Session" % repr(object))
        else:
            m = _class_mapper(object.__class__, entity_name=kwargs.get('entity_name', None))

            # this would be a nice exception to raise...however this is incompatible with a contextual
            # session which puts all objects into the session upon construction.
            #if m._is_orphan(object):
            #    raise exceptions.InvalidRequestError("Instance '%s' is an orphan, "
            #                                         "and must be attached to a parent "
            #                                         "object to be saved" % (repr(object)))

            m._assign_entity_name(object)
            self._register_pending(object)

    def _update_impl(self, object, **kwargs):
        if self._is_attached(object) and object not in self.deleted:
            return
        if not hasattr(object, '_instance_key'):
            raise exceptions.InvalidRequestError("Instance '%s' is not persisted" % repr(object))
        self._attach(object)

    def _register_pending(self, obj):
        self._attach(obj)
        self.uow.register_new(obj)

    def _register_persistent(self, obj):
        self._attach(obj)
        self.uow.register_clean(obj)

    def _register_deleted(self, obj):
        self._attach(obj)
        self.uow.register_deleted(obj)

    def _attach(self, obj):
        """Attach the given object to this ``Session``."""

        old_id = getattr(obj, '_sa_session_id', None)
        if old_id != self.hash_key:
            if old_id is not None and _sessions.has_key(old_id):
                raise exceptions.InvalidRequestError("Object '%s' is already attached "
                                                     "to session '%s' (this is '%s')" %
                                                     (repr(obj), old_id, id(self)))

                # auto-removal from the old session is disabled.  but if we decide to
                # turn it back on, do it as below: gingerly since _sessions is a WeakValueDict
                # and it might be affected by other threads
                #try:
                #    sess = _sessions[old]
                #except KeyError:
                #    sess = None
                #if sess is not None:
                #    sess.expunge(old)
            key = getattr(obj, '_instance_key', None)
            if key is not None:
                self.identity_map[key] = obj
            obj._sa_session_id = self.hash_key

    def _unattach(self, obj):
        if not self._is_attached(obj):
            raise exceptions.InvalidRequestError("Instance '%s' not attached to this Session" % repr(obj))
        del obj._sa_session_id

    def _validate_persistent(self, obj):
        """Validate that the given object is persistent within this
        ``Session``.
        """

        self.uow._validate_obj(obj)

    def _is_attached(self, obj):
        return getattr(obj, '_sa_session_id', None) == self.hash_key

    def __contains__(self, obj):
        return self._is_attached(obj) and (obj in self.uow.new or self.identity_map.has_key(obj._instance_key))

    def __iter__(self):
        return iter(list(self.uow.new) + self.uow.identity_map.values())

    def _get(self, key):
        return self.identity_map[key]

    def has_key(self, key):
        return self.identity_map.has_key(key)

    dirty = property(lambda s:s.uow.locate_dirty(),
                     doc="A ``Set`` of all objects marked as 'dirty' within this ``Session``")

    deleted = property(lambda s:s.uow.deleted,
                       doc="A ``Set`` of all objects marked as 'deleted' within this ``Session``")

    new = property(lambda s:s.uow.new,
                   doc="A ``Set`` of all objects marked as 'new' within this ``Session``.")

    identity_map = property(lambda s:s.uow.identity_map,
                            doc="A dictionary consisting of all objects "
                            "within this ``Session`` keyed to their `_instance_key` value.")

    def import_instance(self, *args, **kwargs):
        """Deprecated. A synynom for ``merge()``."""

        return self.merge(*args, **kwargs)

# this is the AttributeManager instance used to provide attribute behavior on objects.
# to all the "global variable police" out there:  its a stateless object.
attribute_manager = unitofwork.attribute_manager

# this dictionary maps the hash key of a Session to the Session itself, and
# acts as a Registry with which to locate Sessions.  this is to enable
# object instances to be associated with Sessions without having to attach the
# actual Session object directly to the object instance.
_sessions = weakref.WeakValueDictionary()

def object_session(obj):
    """Return the ``Session`` to which the given object is bound, or ``None`` if none."""

    hashkey = getattr(obj, '_sa_session_id', None)
    if hashkey is not None:
        return _sessions.get(hashkey)
    return None

unitofwork.object_session = object_session
from sqlalchemy.orm import mapper
mapper.attribute_manager = attribute_manager
