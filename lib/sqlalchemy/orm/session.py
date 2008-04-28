# session.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Provides the Session class and related utilities."""

import weakref

import sqlalchemy.exceptions as sa_exc
import sqlalchemy.orm.attributes
from sqlalchemy import util, sql, engine
from sqlalchemy.sql import util as sql_util
from sqlalchemy.orm import exc, unitofwork, query, attributes, \
     util as mapperutil, SessionExtension
from sqlalchemy.orm.util import object_mapper as _object_mapper
from sqlalchemy.orm.util import class_mapper as _class_mapper
from sqlalchemy.orm.util import _state_mapper, _state_has_identity, _class_to_mapper
from sqlalchemy.orm.mapper import Mapper
from sqlalchemy.orm.unitofwork import UOWTransaction
from sqlalchemy.orm import identity

__all__ = ['Session', 'SessionTransaction', 'SessionExtension']


def sessionmaker(bind=None, class_=None, autoflush=True, transactional=True, **kwargs):
    """Generate a custom-configured [sqlalchemy.orm.session#Session] class.

    The returned object is a subclass of ``Session``, which, when instantiated with no
    arguments, uses the keyword arguments configured here as its constructor arguments.

    It is intended that the `sessionmaker()` function be called within the global scope
    of an application, and the returned class be made available to the rest of the
    application as the single class used to instantiate sessions.

    e.g.::

        # global scope
        Session = sessionmaker(autoflush=False)

        # later, in a local scope, create and use a session:
        sess = Session()

    Any keyword arguments sent to the constructor itself will override the "configured"
    keywords::

        Session = sessionmaker()

        # bind an individual session to a connection
        sess = Session(bind=connection)

    The class also includes a special classmethod ``configure()``, which allows
    additional configurational options to take place after the custom ``Session``
    class has been generated.  This is useful particularly for defining the
    specific ``Engine`` (or engines) to which new instances of ``Session``
    should be bound::

        Session = sessionmaker()
        Session.configure(bind=create_engine('sqlite:///foo.db'))

        sess = Session()

    The function features a single keyword argument of its own, `class_`, which
    may be used to specify an alternate class other than ``sqlalchemy.orm.session.Session``
    which should be used by the returned class.  All other keyword arguments sent to
    `sessionmaker()` are passed through to the instantiated `Session()` object.
    """

    kwargs['bind'] = bind
    kwargs['autoflush'] = autoflush
    kwargs['transactional'] = transactional

    if class_ is None:
        class_ = Session

    class Sess(class_):
        def __init__(self, **local_kwargs):
            for k in kwargs:
                local_kwargs.setdefault(k, kwargs[k])
            super(Sess, self).__init__(**local_kwargs)

        def configure(self, **new_kwargs):
            """(re)configure the arguments for this sessionmaker.

            e.g.
                Session = sessionmaker()
                Session.configure(bind=create_engine('sqlite://'))
            """

            kwargs.update(new_kwargs)
        configure = classmethod(configure)

    return Sess


class SessionTransaction(object):
    """Represents a Session-level Transaction.

    This corresponds to one or more [sqlalchemy.engine#Transaction]
    instances behind the scenes, with one ``Transaction`` per ``Engine`` in
    use.

    Direct usage of ``SessionTransaction`` is not necessary as of
    SQLAlchemy 0.4; use the ``begin()`` and ``commit()`` methods on
    ``Session`` itself.

    The ``SessionTransaction`` object is **not** threadsafe.
    """

    def __init__(self, session, parent=None, autoflush=True, nested=False):
        self.session = session
        self._connections = {}
        self._parent = parent
        self.autoflush = autoflush
        self.nested = nested
        self._active = True
        self._prepared = False

    def is_active(self):
        return self.session is not None and self._active
    is_active = property(is_active)
    
    def _assert_is_active(self):
        self._assert_is_open()
        if not self._active:
            raise sa_exc.InvalidRequestError("The transaction is inactive due to a rollback in a subtransaction and should be closed")

    def _assert_is_open(self):
        if self.session is None:
            raise sa_exc.InvalidRequestError("The transaction is closed")

    def connection(self, bindkey, **kwargs):
        self._assert_is_active()
        engine = self.session.get_bind(bindkey, **kwargs)
        return self.get_or_add(engine)

    def _begin(self, **kwargs):
        self._assert_is_active()
        return SessionTransaction(self.session, self, **kwargs)

    def _iterate_parents(self, upto=None):
        if self._parent is upto:
            return (self,)
        else:
            if self._parent is None:
                raise sa_exc.InvalidRequestError("Transaction %s is not on the active transaction list" % upto)
            return (self,) + self._parent._iterate_parents(upto)

    def add(self, bind):
        self._assert_is_active()
        if self._parent is not None and not self.nested:
            return self._parent.add(bind)

        if bind.engine in self._connections:
            raise sa_exc.InvalidRequestError("Session already has a Connection associated for the given %sEngine" % (isinstance(bind, engine.Connection) and "Connection's " or ""))
        return self.get_or_add(bind)

    def get_or_add(self, bind):
        self._assert_is_active()
        
        if bind in self._connections:
            return self._connections[bind][0]
        
        if self._parent:
            conn = self._parent.get_or_add(bind)
            if not self.nested:
                return conn
        else:
            if isinstance(bind, engine.Connection):
                conn = bind
                if conn.engine in self._connections:
                    raise sa_exc.InvalidRequestError("Session already has a Connection associated for the given Connection's Engine")
            else:
                conn = bind.contextual_connect()

        if self.session.twophase and self._parent is None:
            transaction = conn.begin_twophase()
        elif self.nested:
            transaction = conn.begin_nested()
        else:
            transaction = conn.begin()
        
        self._connections[conn] = self._connections[conn.engine] = (conn, transaction, conn is not bind)
        return conn

    def _prepare(self):
        if self._parent is not None or not self.session.twophase:
            raise sa_exc.InvalidRequestError("Only root two phase transactions of can be prepared")
        self._prepare_impl()
    prepare = util.deprecated()(_prepare)
    
    def _prepare_impl(self):
        self._assert_is_active()
        if self.session.extension is not None and (self._parent is None or self.nested):
            self.session.extension.before_commit(self.session)
        
        if self.session.transaction is not self:
            for subtransaction in self.session.transaction._iterate_parents(upto=self):
                subtransaction.commit()
            
        if self.autoflush:
            self.session.flush()
        
        if self._parent is None and self.session.twophase:
            try:
                for t in util.Set(self._connections.values()):
                    t[1].prepare()
            except:
                self.rollback()
                raise
        
        self._deactivate()
        self._prepared = True
    
    def _commit(self):
        self._assert_is_open()
        if not self._prepared:
            self._prepare_impl()
        
        if self._parent is None or self.nested:
            for t in util.Set(self._connections.values()):
                t[1].commit()

            if self.session.extension is not None:
                self.session.extension.after_commit(self.session)

        self._close()
        return self._parent
    commit = util.deprecated()(_commit)
    
    def _rollback(self):
        self._assert_is_open()
        
        if self.session.transaction is not self:
            for subtransaction in self.session.transaction._iterate_parents(upto=self):
                subtransaction._close()
        
        if self.is_active:
            for transaction in self._iterate_parents():
                if transaction._parent is None or transaction.nested:
                    transaction._rollback_impl()
                    transaction._deactivate()
                    break
                else:
                    transaction._deactivate()
        self._close()
        return self._parent
    rollback = util.deprecated()(_rollback)
    
    def _rollback_impl(self):
        for t in util.Set(self._connections.values()):
            t[1].rollback()

        if self.session.extension is not None:
            self.session.extension.after_rollback(self.session)

    def _deactivate(self):
        self._active = False

    def _close(self):
        self.session.transaction = self._parent
        if self._parent is None:
            for connection, transaction, autoclose in util.Set(self._connections.values()):
                if autoclose:
                    connection.close()
                else:
                    transaction.close()
        self._deactivate()
        self.session = None
        self._connections = None
    close = util.deprecated()(_close)
    
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self.session.transaction is None:
            return
        if type is None:
            try:
                self._commit()
            except:
                self._rollback()
                raise
        else:
            self._rollback()

class Session(object):
    """Encapsulates a set of objects being operated upon within an object-relational operation.

    The Session is the front end to SQLAlchemy's **Unit of Work** implementation. The concept
    behind Unit of Work is to track modifications to a field of objects, and then be able to
    flush those changes to the database in a single operation.

    SQLAlchemy's unit of work includes these functions:

    * The ability to track in-memory changes on scalar- and collection-based object
      attributes, such that database persistence operations can be assembled based on those
      changes.

    * The ability to organize individual SQL queries and population of newly generated
      primary and foreign key-holding attributes during a persist operation such that
      referential integrity is maintained at all times.

    * The ability to maintain insert ordering against the order in which new instances were
      added to the session.

    * an Identity Map, which is a dictionary keying instances to their unique primary key
      identity. This ensures that only one copy of a particular entity is ever present
      within the session, even if repeated load operations for the same entity occur. This
      allows many parts of an application to get a handle to a particular object without
      any chance of modifications going to two different places.

    When dealing with instances of mapped classes, an instance may be *attached* to a
    particular Session, else it is *unattached* . An instance also may or may not correspond
    to an actual row in the database. These conditions break up into four distinct states:

    * *Transient* - an instance that's not in a session, and is not saved to the database;
      i.e. it has no database identity. The only relationship such an object has to the ORM
      is that its class has a `mapper()` associated with it.

    * *Pending* - when you `save()` a transient instance, it becomes pending. It still
      wasn't actually flushed to the database yet, but it will be when the next flush
      occurs.

    * *Persistent* - An instance which is present in the session and has a record in the
      database. You get persistent instances by either flushing so that the pending
      instances become persistent, or by querying the database for existing instances (or
      moving persistent instances from other sessions into your local session).

    * *Detached* - an instance which has a record in the database, but is not in any
      session. Theres nothing wrong with this, and you can use objects normally when
      they're detached, **except** they will not be able to issue any SQL in order to load
      collections or attributes which are not yet loaded, or were marked as "expired".

    The session methods which control instance state include ``save()``, ``update()``,
    ``save_or_update()``, ``delete()``, ``merge()``, and ``expunge()``.

    The Session object is **not** threadsafe, particularly during flush operations.  A session
    which is only read from (i.e. is never flushed) can be used by concurrent threads if it's
    acceptable that some object instances may be loaded twice.

    The typical pattern to managing Sessions in a multi-threaded environment is either to use
    mutexes to limit concurrent access to one thread at a time, or more commonly to establish
    a unique session for every thread, using a threadlocal variable.  SQLAlchemy provides
    a thread-managed Session adapter, provided by the [sqlalchemy.orm#scoped_session()] function.
    """

    def __init__(self, bind=None, autoflush=True, transactional=False, twophase=False, echo_uow=False, weak_identity_map=True, binds=None, extension=None):
        """Construct a new Session.
        
        A session is usually constructed using the [sqlalchemy.orm#create_session()] function, 
        or its more "automated" variant [sqlalchemy.orm#sessionmaker()].

        autoflush
            When ``True``, all query operations will issue a ``flush()`` call to this
            ``Session`` before proceeding. This is a convenience feature so that
            ``flush()`` need not be called repeatedly in order for database queries to
            retrieve results. It's typical that ``autoflush`` is used in conjunction with
            ``transactional=True``, so that ``flush()`` is never called; you just call
            ``commit()`` when changes are complete to finalize all changes to the
            database.

        bind
            An optional ``Engine`` or ``Connection`` to which this ``Session`` should be
            bound. When specified, all SQL operations performed by this session will
            execute via this connectable.

        binds
            An optional dictionary, which contains more granular "bind" information than
            the ``bind`` parameter provides. This dictionary can map individual ``Table``
            instances as well as ``Mapper`` instances to individual ``Engine`` or
            ``Connection`` objects. Operations which proceed relative to a particular
            ``Mapper`` will consult this dictionary for the direct ``Mapper`` instance as
            well as the mapper's ``mapped_table`` attribute in order to locate an
            connectable to use. The full resolution is described in the ``get_bind()``
            method of ``Session``. Usage looks like::

                sess = Session(binds={
                    SomeMappedClass : create_engine('postgres://engine1'),
                    somemapper : create_engine('postgres://engine2'),
                    some_table : create_engine('postgres://engine3'),
                })

            Also see the ``bind_mapper()`` and ``bind_table()`` methods.

        echo_uow
            When ``True``, configure Python logging to dump all unit-of-work
            transactions. This is the equivalent of
            ``logging.getLogger('sqlalchemy.orm.unitofwork').setLevel(logging.DEBUG)``.

        extension
            An optional [sqlalchemy.orm.session#SessionExtension] instance, which will receive
            pre- and post- commit and flush events, as well as a post-rollback event.  User-
            defined code may be placed within these hooks using a user-defined subclass
            of ``SessionExtension``.

        transactional
            Set up this ``Session`` to automatically begin transactions. Setting this
            flag to ``True`` is the rough equivalent of calling ``begin()`` after each
            ``commit()`` operation, after each ``rollback()``, and after each
            ``close()``. Basically, this has the effect that all session operations are
            performed within the context of a transaction. Note that the ``begin()``
            operation does not immediately utilize any connection resources; only when
            connection resources are first required do they get allocated into a
            transactional context.

        twophase
            When ``True``, all transactions will be started using
            [sqlalchemy.engine_TwoPhaseTransaction]. During a ``commit()``, after
            ``flush()`` has been issued for all attached databases, the ``prepare()``
            method on each database's ``TwoPhaseTransaction`` will be called. This allows
            each database to roll back the entire transaction, before each transaction is
            committed.

        weak_identity_map
            When set to the default value of ``False``, a weak-referencing map is used;
            instances which are not externally referenced will be garbage collected
            immediately. For dereferenced instances which have pending changes present,
            the attribute management system will create a temporary strong-reference to
            the object which lasts until the changes are flushed to the database, at which
            point it's again dereferenced. Alternatively, when using the value ``True``,
            the identity map uses a regular Python dictionary to store instances. The
            session will maintain all instances present until they are removed using
            expunge(), clear(), or purge().
        """
        self.echo_uow = echo_uow
        if weak_identity_map:
            self._identity_cls = identity.WeakInstanceDict
        else:
            self._identity_cls = identity.StrongInstanceDict
        self.identity_map = self._identity_cls()

        self._new = {}   # InstanceState->object, strong refs object
        self._deleted = {}  # same
        self.bind = bind
        self.__binds = {}
        self.transaction = None
        self.hash_key = id(self)
        self.autoflush = autoflush
        self.transactional = transactional
        self.twophase = twophase
        self.extension = extension
        self._query_cls = query.Query
        self._mapper_flush_opts = {}

        if binds is not None:
            for mapperortable, value in binds.iteritems():
                if isinstance(mapperortable, type):
                    mapperortable = _class_mapper(mapperortable).base_mapper
                self.__binds[mapperortable] = value
                if isinstance(mapperortable, Mapper):
                    for t in mapperortable._all_tables:
                        self.__binds[t] = value

        if self.transactional:
            self.begin()
        _sessions[self.hash_key] = self

    def begin(self, **kwargs):
        """Begin a transaction on this Session."""

        if self.transaction is not None:
            self.transaction = self.transaction._begin(**kwargs)
        else:
            self.transaction = SessionTransaction(self, **kwargs)
        return self.transaction

    create_transaction = begin


    def begin_nested(self):
        """Begin a `nested` transaction on this Session.

        This utilizes a ``SAVEPOINT`` transaction for databases
        which support this feature.
        """

        return self.begin(nested=True)

    def rollback(self):
        """Rollback the current transaction in progress.

        If no transaction is in progress, this method is a
        pass-thru.
        """

        if self.transaction is None:
            pass
        else:
            self.transaction._rollback()
        if self.transaction is None and self.transactional:
            self.begin()

    def commit(self):
        """Commit the current transaction in progress.

        If no transaction is in progress, this method raises
        an InvalidRequestError.

        If the ``begin()`` method was called on this ``Session``
        additional times subsequent to its first call,
        ``commit()`` will not actually commit, and instead
        pops an internal SessionTransaction off its internal stack
        of transactions.  Only when the "root" SessionTransaction
        is reached does an actual database-level commit occur.
        """

        if self.transaction is None:
            if self.transactional:
                self.begin()
            else:
                raise sa_exc.InvalidRequestError("No transaction is begun.")

        self.transaction._commit()
        if self.transaction is None and self.transactional:
            self.begin()
    
    def prepare(self):
        """Prepare the current transaction in progress for two phase commit.

        If no transaction is in progress, this method raises
        an InvalidRequestError.

        Only root transactions of two phase sessions can be prepared. If the current transaction is
        not such, an InvalidRequestError is raised.
        """
        if self.transaction is None:
            if self.transactional:
                self.begin()
            else:
                raise sa_exc.InvalidRequestError("No transaction is begun.")

        self.transaction._prepare()

    def connection(self, mapper=None, **kwargs):
        """Return a ``Connection`` corresponding to this session's
        transactional context, if any.

        If this ``Session`` is transactional, the connection will be in
        the context of this session's transaction.  Otherwise, the
        connection is returned by the ``contextual_connect()`` method
        on the engine.

        The `mapper` argument is a class or mapper to which a bound engine
        will be located; use this when the Session itself is either bound
        to multiple engines or connections, or is not bound to any connectable.

        \**kwargs are additional arguments which will be passed to get_bind().
        See the get_bind() method for details.  Note that the ``ShardedSession``
        subclass takes a different get_bind() argument signature.
        """

        return self.__connection(self.get_bind(mapper, **kwargs))

    def __connection(self, engine, **kwargs):
        if self.transaction is not None:
            return self.transaction.get_or_add(engine)
        else:
            return engine.contextual_connect(**kwargs)

    def execute(self, clause, params=None, mapper=None, **kwargs):
        """Using the given mapper to identify the appropriate ``Engine``
        or ``Connection`` to be used for statement execution, execute the
        given ``ClauseElement`` using the provided parameter dictionary.

        Return a ``ResultProxy`` corresponding to the execution's results.

        If this method allocates a new ``Connection`` for the operation,
        then the ``ResultProxy`` 's ``close()`` method will release the
        resources of the underlying ``Connection``.
        """

        engine = self.get_bind(mapper, clause=clause, **kwargs)

        return self.__connection(engine, close_with_result=True).execute(clause, params or {})

    def scalar(self, clause, params=None, mapper=None, **kwargs):
        """Like execute() but return a scalar result."""

        engine = self.get_bind(mapper, clause=clause)

        return self.__connection(engine, close_with_result=True).scalar(clause, params or {}, **kwargs)

    def close(self):
        """Close this Session.

        This clears all items and ends any transaction in progress.

        If this session were created with ``transactional=True``, a
        new transaction is immediately begun.  Note that this new
        transaction does not use any connection resources until they
        are first needed.
        """

        self.clear()
        if self.transaction is not None:
            for transaction in self.transaction._iterate_parents():
                transaction._close()
        if self.transactional:
            # note this doesnt use any connection resources
            self.begin()

    def close_all(cls):
        """Close *all* sessions in memory."""

        for sess in _sessions.values():
            sess.close()
    close_all = classmethod(close_all)

    def clear(self):
        """Remove all object instances from this ``Session``.

        This is equivalent to calling ``expunge()`` for all objects in
        this ``Session``.
        """
        
        for state in self.identity_map.all_states() + list(self._new):
            del state.session_id

        self.identity_map = self._identity_cls()
        self._new = {}
        self._deleted = {}

    # TODO: need much more test coverage for bind_mapper() and similar !

    def bind_mapper(self, mapper, bind, entity_name=None):
        """Bind the given `mapper` or `class` to the given ``Engine`` or ``Connection``.

        All subsequent operations involving this ``Mapper`` will use the
        given `bind`.
        """

        if isinstance(mapper, type):
            mapper = _class_mapper(mapper, entity_name=entity_name)

        self.__binds[mapper.base_mapper] = bind
        for t in mapper._all_tables:
            self.__binds[t] = bind

    def bind_table(self, table, bind):
        """Bind the given `table` to the given ``Engine`` or ``Connection``.

        All subsequent operations involving this ``Table`` will use the
        given `bind`.
        """

        self.__binds[table] = bind

    def get_bind(self, mapper, clause=None, **kwargs):
        """Return an engine corresponding to the given arguments.

        mapper
            mapper relative to the desired operation

        clause
            a ClauseElement which is to be executed.  if
            mapper is not present, this may be used to locate
            Table objects, which are then associated with mappers
            which have associated binds.

        \**kwargs
            Subclasses (i.e. ShardedSession) may add additional arguments
            to get_bind() which are passed through here.
        """

        if mapper is None and clause is None:
            if self.bind:
                return self.bind
            else:
                raise sa_exc.UnboundExecutionError("This session is not bound to any Engine or Connection; specify a mapper to get_bind()")

        elif self.__binds:
            if mapper:
                mapper = _class_to_mapper(mapper)
                if mapper.base_mapper in self.__binds:
                    return self.__binds[mapper.base_mapper]
                elif mapper.mapped_table in self.__binds:
                    return self.__binds[mapper.mapped_table]
            if clause:
                for t in sql_util.find_tables(clause):
                    if t in self.__binds:
                        return self.__binds[t]

        if self.bind:
            return self.bind
        elif isinstance(clause, sql.expression.ClauseElement) and clause.bind:
            return clause.bind
        elif not mapper:
            raise sa_exc.UnboundExecutionError("Could not locate any mapper associated with SQL expression")
        else:
            mapper = _class_to_mapper(mapper)
            e = mapper.mapped_table.bind
            if e is None:
                raise sa_exc.UnboundExecutionError("Could not locate any Engine or Connection bound to mapper '%s'" % str(mapper))
            return e

    def query(self, *entities, **kwargs):
        """Return a new ``Query`` object corresponding to this ``Session``."""
        
        return self._query_cls(entities, self, **kwargs)

    def _autoflush(self):
        if self.autoflush and (self.transaction is None or self.transaction.autoflush):
            self.flush()

    def get(self, class_, ident, entity_name=None):
        """Return an instance of the object based on the given
        identifier, or ``None`` if not found.

        The `ident` argument is a scalar or tuple of primary key
        column values in the order of the table def's primary key
        columns.

        The `entity_name` keyword argument may also be specified which
        further qualifies the underlying Mapper used to perform the
        query.
        """

        return self.query(class_, entity_name=entity_name).get(ident)

    def load(self, class_, ident, entity_name=None):
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

        return self.query(class_, entity_name=entity_name).load(ident)

    def refresh(self, instance, attribute_names=None):
        """Refresh the attributes on the given instance.

        When called, a query will be issued
        to the database which will refresh all attributes with their
        current value.

        Lazy-loaded relational attributes will remain lazily loaded, so that
        the instance-wide refresh operation will be followed
        immediately by the lazy load of that attribute.

        Eagerly-loaded relational attributes will eagerly load within the
        single refresh operation.

        The ``attribute_names`` argument is an iterable collection
        of attribute names indicating a subset of attributes to be
        refreshed.
        """

        state = attributes.instance_state(instance)
        self._validate_persistent(state)
        if self.query(_object_mapper(instance))._get(
                state.key, refresh_instance=state,
                only_load_props=attribute_names) is None:
            raise sa_exc.InvalidRequestError("Could not refresh instance '%s'" % mapperutil.instance_str(instance))

    def expire_all(self):
        """Expires all persistent instances within this Session.  
        
        """
        for state in self.identity_map.all_states():
            _expire_state(state, None)
        
    def expire(self, instance, attribute_names=None):
        """Expire the attributes on the given instance.

        The instance's attributes are instrumented such that
        when an attribute is next accessed, a query will be issued
        to the database which will refresh all attributes with their
        current value.

        The ``attribute_names`` argument is an iterable collection
        of attribute names indicating a subset of attributes to be
        expired.
        """
        state = attributes.instance_state(instance)
        self._validate_persistent(state)
        if attribute_names:
            _expire_state(state, attribute_names=attribute_names)
        else:
            # pre-fetch the full cascade since the expire is going to
            # remove associations
            cascaded = list(_cascade_state_iterator('refresh-expire', state))
            _expire_state(state, None)
            for (state, m) in cascaded:
                _expire_state(state, None)

    def prune(self):
        """Remove unreferenced instances cached in the identity map.

        Note that this method is only meaningful if "weak_identity_map"
        is set to False.

        Removes any object in this Session's identity map that is not
        referenced in user code, modified, new or scheduled for deletion.
        Returns the number of objects pruned.
        """

        return self.identity_map.prune()

    def expunge(self, instance):
        """Remove the given `instance` from this ``Session``.

        This will free all internal references to the instance.
        Cascading will be applied according to the *expunge* cascade
        rule.
        """
        self._expunge_state(attributes.instance_state(instance))
        
    def _expunge_state(self, state):
        for s, m in [(state, None)] + list(_cascade_state_iterator('expunge', state)):
            if s in self._new:
                self._new.pop(s)
                del s.session_id
            elif self.identity_map.contains_state(s):
                self._remove_persistent(s)
        
    def _remove_persistent(self, state):
        self.identity_map.discard(state)
        self._deleted.pop(state, None)
        del state.session_id

    def save(self, instance, entity_name=None):
        """Add a transient (unsaved) instance to this ``Session``.

        This operation cascades the `save_or_update` method to
        associated instances if the relation is mapped with
        ``cascade="save-update"``.

        The `entity_name` keyword argument will further qualify the
        specific ``Mapper`` used to handle this instance.
        
        """
        state = _state_for_unsaved_instance(instance, entity_name)
        self._save_impl(state)
        self._cascade_save_or_update(state, entity_name)
    
    def _save_without_cascade(self, instance, entity_name=None):
        """used by scoping.py to save on init without cascade."""
        
        state = _state_for_unsaved_instance(instance, entity_name)
        self._save_impl(state)
        
    def update(self, instance, entity_name=None):
        """Bring the given detached (saved) instance into this
        ``Session``.

        If there is a persistent instance with the same instance key, but
        different identity already associated with this ``Session``, an
        InvalidRequestError exception is thrown.

        This operation cascades the `save_or_update` method to
        associated instances if the relation is mapped with
        ``cascade="save-update"``.
        
        """
        state = attributes.instance_state(instance)
        self._update_impl(state)
        self._cascade_save_or_update(state, entity_name)

    def save_or_update(self, instance, entity_name=None):
        """Save or update the given instance into this ``Session``.

        The non-None state `key` on the instance's state determines whether
        to ``save()`` or ``update()`` the instance.

        """
        state = _state_for_unknown_persistence_instance(instance, entity_name)
        self._save_or_update_state(state, entity_name)
        
    def _save_or_update_state(self, state, entity_name):
        self._save_or_update_impl(state)
        self._cascade_save_or_update(state, entity_name)
        
    def _cascade_save_or_update(self, state, entity_name):
        for state, mapper in _cascade_unknown_state_iterator('save-update', state, halt_on=lambda c:c in self):
            self._save_or_update_impl(state)

    def delete(self, instance):
        """Mark the given instance as deleted.

        The delete operation occurs upon ``flush()``.
        """

        state = attributes.instance_state(instance)
        self._delete_impl(state)
        for state, m in _cascade_state_iterator('delete', state):
            self._delete_impl(state, ignore_transient=True)


    def merge(self, instance, entity_name=None, dont_load=False, _recursive=None):
        """Copy the state of the given `instance` onto the persistent
        instance with the same identifier.

        If there is no persistent instance currently associated with
        the session, it will be loaded.  Return the persistent
        instance. If the given instance is unsaved, save a copy of and
        return it as a newly persistent instance. The given instance
        does not become associated with the session.

        This operation cascades to associated instances if the
        association is mapped with ``cascade="merge"``.
        """

        if _recursive is None:
            _recursive = {}  # TODO: this should be an IdentityDict for instances, but will need a separate
                             # dict for PropertyLoader tuples
        if entity_name is not None:
            mapper = _class_mapper(instance.__class__, entity_name=entity_name)
        else:
            mapper = _object_mapper(instance)
        if instance in _recursive:
            return _recursive[instance]

        new_instance = False
        state = attributes.instance_state(instance)
        key = state.key
        if key is None:
            if dont_load:
                raise sa_exc.InvalidRequestError("merge() with dont_load=True option does not support objects transient (i.e. unpersisted) objects.  flush() all changes on mapped instances before merging with dont_load=True.")
            key = mapper._identity_key_from_state(state)

        merged = None
        if key:
            if key in self.identity_map:
                merged = self.identity_map[key]
            elif dont_load:
                if state.modified:
                    raise sa_exc.InvalidRequestError("merge() with dont_load=True option does not support objects marked as 'dirty'.  flush() all changes on mapped instances before merging with dont_load=True.")

                merged = mapper.class_manager.new_instance()
                merged_state = attributes.instance_state(merged)
                merged_state.key = key
                merged_state.entity_name = entity_name
                self._update_impl(merged_state)
                new_instance = True
            else:
                merged = self.get(mapper.class_, key[1])

        if merged is None:
            merged = mapper.class_manager.new_instance()
            merged_state = attributes.instance_state(merged)
            new_instance = True
            self.save(merged, entity_name=mapper.entity_name)

        _recursive[instance] = merged

        for prop in mapper.iterate_properties:
            prop.merge(self, instance, merged, dont_load, _recursive)

        if dont_load:
            attributes.instance_state(merged).commit_all()  # remove any history

        if new_instance:
            merged_state._run_on_load(merged)
        return merged

    def identity_key(cls, *args, **kwargs):
        return mapperutil.identity_key(*args, **kwargs)
    identity_key = classmethod(identity_key)

    def object_session(cls, instance):
        """Return the ``Session`` to which the given object belongs."""

        return object_session(instance)
    object_session = classmethod(object_session)

    def _validate_persistent(self, state):
        if not self.identity_map.contains_state(state):
            raise sa_exc.InvalidRequestError("Instance '%s' is not persistent within this Session" % mapperutil.state_str(state))

    def _save_impl(self, state):
        if state.key is not None:
            raise sa_exc.InvalidRequestError(
                "Object '%s' already has an identity - it can't be registered "
                "as pending" % repr(obj))
        self._attach(state)
        if state not in self._new:
            self._new[state] = state.obj()
            state.insert_order = len(self._new)

    def _update_impl(self, state):
        if self.identity_map.contains_state(state) and state not in self._deleted:
            return

        if state.key is None:
            raise sa_exc.InvalidRequestError(
                "Instance '%s' is not persisted" %
                mapperutil.state_str(state))
                
        if state.key in self.identity_map and not self.identity_map.contains_state(state):
            raise sa_exc.InvalidRequestError(
                "Could not update instance '%s', identity key %s; a different "
                "instance with the same identity key already exists in this "
                "session." % (mapperutil.state_str(state), state.key))
        self._attach(state)
        self.identity_map.add(state)
        
    def _save_or_update_impl(self, state):
        if state.key is None:
            self._save_impl(state)
        else:
            self._update_impl(state)

    def _delete_impl(self, state, ignore_transient=False):
        if self.identity_map.contains_state(state) and state in self._deleted:
            return
            
        if state.key is None:
            if ignore_transient:
                return
            else:
                raise sa_exc.InvalidRequestError("Instance '%s' is not persisted" % mapperutil.state_str(state))
        if state.key in self.identity_map and not self.identity_map.contains_state(state):
            raise sa_exc.InvalidRequestError(
                "Instance '%s' is with key %s already persisted with a "
                "different identity" % (mapperutil.state_str(state),
                                        state.key))

        self._deleted[state] = state.obj()
        self._attach(state)

    def _attach(self, state):
        if state.session_id and state.session_id is not self.hash_key:
            raise sa_exc.InvalidRequestError(
                "Object '%s' is already attached to session '%s' "
                "(this is '%s')" % (mapperutil.state_str(state),
                                    state.session_id, self.hash_key))
        state.session_id = self.hash_key

    def __contains__(self, instance):
        """Return True if the given instance is associated with this session.

        The instance may be pending or persistent within the Session for a
        result of True.

        """
        return self._contains_state(attributes.instance_state(instance))
    
    def __iter__(self):
        """Return an iterator of all instances which are pending or persistent within this Session."""

        return iter(list(self._new.values()) + self.identity_map.values())

    def _contains_state(self, state):
        return state in self._new or self.identity_map.contains_state(state)

    def _register_newly_persistent(self, state):

        mapper = _state_mapper(state)
        instance_key = mapper._identity_key_from_state(state)

        if state.key is None:
            state.key = instance_key
        elif state.key != instance_key:
            # primary key switch
            self.identity_map.remove(state)
            state.key = instance_key

        if hasattr(state, 'insert_order'):
            delattr(state, 'insert_order')

        obj = state.obj()
        # prevent against last minute dereferences of the object
        # TODO: identify a code path where state.obj() is None
        if obj is not None:
            if state.key in self.identity_map and not self.identity_map.contains_state(state):
                self.identity_map.remove_key(state.key)
            self.identity_map.add(state)
            state.commit_all()

        # remove from new last, might be the last strong ref
        self._new.pop(state, None)

    def flush(self, objects=None):
        """Flush all the object modifications present in this session
        to the database.

        `objects` is a list or tuple of objects specifically to be
        flushed; if ``None``, all new and modified objects are flushed.

        """
        if not self.identity_map.check_modified() and not self._deleted and not self._new:
            return
            
        dirty = self._dirty_states
        if not dirty and not self._deleted and not self._new:
            self.identity_map.modified = False
            return

        deleted = util.Set(self._deleted)
        new = util.Set(self._new)

        dirty = util.Set(dirty).difference(deleted)

        flush_context = UOWTransaction(self)

        if self.extension is not None:
            self.extension.before_flush(self, flush_context, objects)

        # create the set of all objects we want to operate upon
        if objects:
            # specific list passed in
            objset = util.Set([attributes.instance_state(o) for o in objects])
        else:
            # or just everything
            objset = util.Set(self.identity_map.all_states()).union(new)

        # store objects whose fate has been decided
        processed = util.Set()

        # put all saves/updates into the flush context.  detect top-level orphans and throw them into deleted.
        for state in new.union(dirty).intersection(objset).difference(deleted):
            is_orphan = _state_mapper(state)._is_orphan(state)
            if is_orphan and not _state_has_identity(state):
                raise exc.FlushError("instance %s is an unsaved, pending instance and is an orphan (is not attached to %s)" %
                    (
                        mapperutil.state_str(state),
                        ", nor ".join(["any parent '%s' instance via that classes' '%s' attribute" % (klass.__name__, key) for (key,klass) in _state_mapper(state).delete_orphans])
                    ))
            flush_context.register_object(state, isdelete=is_orphan)
            processed.add(state)

        # put all remaining deletes into the flush context.
        for state in deleted.intersection(objset).difference(processed):
            flush_context.register_object(state, isdelete=True)

        if len(flush_context.tasks) == 0:
            return

        self.create_transaction(autoflush=False)
        flush_context.transaction = self.transaction
        try:
            flush_context.execute()

            if self.extension is not None:
                self.extension.after_flush(self, flush_context)
            self.commit()
        except:
            self.rollback()
            flush_context.remove_flush_changes()
            raise

        flush_context.finalize_flush_changes()

        if not objects:
            self.identity_map.modified = False

        if self.extension is not None:
            self.extension.after_flush_postexec(self, flush_context)

    def is_modified(self, instance, include_collections=True, passive=False):
        """Return True if the given instance has modified attributes.

        This method retrieves a history instance for each instrumented attribute
        on the instance and performs a comparison of the current value to its
        previously committed value.  Note that instances present in the 'dirty'
        collection may result in a value of ``False`` when tested with this method.

        `include_collections` indicates if multivalued collections should be included
        in the operation.  Setting this to False is a way to detect only local-column
        based properties (i.e. scalar columns or many-to-one foreign keys) that would
        result in an UPDATE for this instance upon flush.

        The `passive` flag indicates if unloaded attributes and collections should
        not be loaded in the course of performing this test.
        """

        for attr in attributes.manager_of_class(instance.__class__).attributes:
            if not include_collections and hasattr(attr.impl, 'get_collection'):
                continue
            (added, unchanged, deleted) = attr.get_history(instance)
            if added or deleted:
                return True
        return False

    def _dirty_states(self):
        """Return a set of all persistent states considered dirty.

        This method returns all states that were modified including those that
        were possibly deleted.

        """
        return util.IdentitySet(
            [state for state in self.identity_map.all_states() if state.check_modified()]
        )
    _dirty_states = property(_dirty_states)

    def dirty(self):
        """Return a set of all persistent instances considered dirty.

        Instances are considered dirty when they were modified but not
        deleted.

        Note that the 'dirty' state here is 'optimistic'; most attribute-setting or collection
        modification operations will mark an instance as 'dirty' and place it in this set,
        even if there is no net change to the attribute's value.  At flush time, the value
        of each attribute is compared to its previously saved value,
        and if there's no net change, no SQL operation will occur (this is a more expensive
        operation so it's only done at flush time).

        To check if an instance has actionable net changes to its attributes, use the
        is_modified() method.

        """
        
        return util.IdentitySet(
            [state.obj() for state in self._dirty_states if state not in self._deleted]
        )

    dirty = property(dirty)

    def deleted(self):
        "Return a ``Set`` of all instances marked as 'deleted' within this ``Session``"
        
        return util.IdentitySet(self._deleted.values())
    deleted = property(deleted)

    def new(self):
        "Return a ``Set`` of all instances marked as 'new' within this ``Session``."
        
        return util.IdentitySet(self._new.values())
    new = property(new)

def _expire_state(state, attribute_names):
    """Standalone expire instance function.

    Installs a callable with the given instance's _state
    which will fire off when any of the named attributes are accessed;
    their existing value is removed.

    If the list is None or blank, the entire instance is expired.
    """

    state.expire_attributes(attribute_names)

register_attribute = unitofwork.register_attribute

_sessions = weakref.WeakValueDictionary()

def _cascade_state_iterator(cascade, state, **kwargs):
    mapper = _state_mapper(state)
    for (o, m) in mapper.cascade_iterator(cascade, state, **kwargs):
        yield attributes.instance_state(o), m

def _cascade_unknown_state_iterator(cascade, state, **kwargs):
    mapper = _state_mapper(state)
    for (o, m) in mapper.cascade_iterator(cascade, state, **kwargs):
        yield _state_for_unknown_persistence_instance(o, m.entity_name), m

def _state_for_unsaved_instance(instance, entity_name):
    manager = attributes.manager_of_class(instance.__class__)
    if manager is None:
        raise "FIXME unmapped instance"
    if manager.has_state(instance):
        state = manager.state_of(instance)
        if state.key is not None:
            raise sa_exc.InvalidRequestError(
                "Instance '%s' is already persistent" %
                mapperutil.state_str(state))
    else:
        state = manager.setup_instance(instance)
    state.entity_name = entity_name
    return state

def _state_for_unknown_persistence_instance(instance, entity_name):
    try:
        state = attributes.instance_state(instance)
        state.entity_name = entity_name
        return state
    except AttributeError:
        return self._state_for_unsaved_instance(instance, entity_name)

def object_session(instance):
    """Return the ``Session`` to which the given instance is bound, or ``None`` if none."""

    return _state_session(attributes.instance_state(instance))
    
def _state_session(state):
    if state.session_id:
        try:
            return _sessions[state.session_id]
        except KeyError:
            pass
    return None

# Lazy initialization to avoid circular imports
unitofwork.object_session = object_session
unitofwork._state_session = _state_session
from sqlalchemy.orm import mapper
mapper._expire_state = _expire_state
mapper._state_session = _state_session
