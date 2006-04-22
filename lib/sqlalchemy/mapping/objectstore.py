# objectstore.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy import util
from sqlalchemy.exceptions import *
import unitofwork, query
import weakref
import sqlalchemy
import sqlalchemy.sql as sql


class SessionTransaction(object):
    def __init__(self, session, parent=None, autoflush=True):
        self.session = session
        self.connections = {}
        self.parent = parent
        self.autoflush = autoflush
    def connection(self, mapper):
        if self.parent is not None:
            return self.parent.connection(mapper)
        engine = self.session.get_bind(mapper)
        return self.add(engine)
    def _begin(self):
        return SessionTransaction(self.session, self)
    def add(self, connection_or_engine):
        if self.connections.has_key(connection_or_engine):
            return self.connections[connection_or_engine][0]
        c = connection_or_engine.contextual_connect()
        e = c.engine
        if not self.connections.has_key(e):
            self.connections[e] = (c, c.begin())
        return self.connections[e][0]
    def commit(self):
        if self.parent is not None:
            return
        if self.autoflush:
            self.session.flush()
        for t in self.connections.values():
            t[1].commit()
        self.close()
    def rollback(self):
        if self.parent is not None:
            self.parent.rollback()
            return
        for k, t in self.connections.iteritems():
            t[1].rollback()
        self.close()
    def close(self):
        if self.parent is not None:
            return
        for t in self.connections.values():
            t[0].close()
        self.session.transaction = None
        
class Session(object):
    def __init__(self, bind_to=None, hash_key=None, new_imap=True, import_session=None):
        if import_session is not None:
            self.uow = unitofwork.UnitOfWork(identity_map=import_session.uow.identity_map)
        elif new_imap is False:
            self.uow = unitofwork.UnitOfWork(identity_map=objectstore.get_session().uow.identity_map)
        else:
            self.uow = unitofwork.UnitOfWork()
        
        self.bind_to = bind_to
        self.binds = {}
        self.transaction = None
        if hash_key is None:
            self.hash_key = id(self)
        else:
            self.hash_key = hash_key
        _sessions[self.hash_key] = self

    def create_transaction(self, **kwargs):
        """returns a new SessionTransaction corresponding to an existing or new transaction.
        if the transaction is new, the returned SessionTransaction will have commit control
        over the underlying transaction, else will have rollback control only."""
        if self.transaction is not None:
            return self.transaction._begin()
        else:
            self.transaction = SessionTransaction(self, **kwargs)
            return self.transaction
    def connect(self, mapper=None, **kwargs):
        """returns a unique connection corresponding to the given mapper.  this connection
        will not be part of any pre-existing transactional context."""
        return self.get_bind(mapper).connect(**kwargs)
    def connection(self, mapper, **kwargs):
        """returns a Connection corresponding to the given mapper.  used by the execute()
        method which performs select operations for Mapper and Query.
        if this Session is transactional, 
        the connection will be in the context of this session's transaction.  otherwise, the connection
        is returned by the contextual_connect method, which some Engines override to return a thread-local
        connection, and will have close_with_result set to True.
        
        the given **kwargs will be sent to the engine's contextual_connect() method, if no transaction is in progress."""
        if self.transaction is not None:
            return self.transaction.connection(mapper)
        else:
            return self.get_bind(mapper).contextual_connect(**kwargs)
    def execute(self, mapper, clause, params, **kwargs):
        """using the given mapper to identify the appropriate Engine or Connection to be used for statement execution, 
        executes the given ClauseElement using the provided parameter dictionary.  Returns a ResultProxy corresponding
        to the execution's results.  If this method allocates a new Connection for the operation, then the ResultProxy's close() 
        method will release the resources of the underlying Connection, otherwise its a no-op.
        """
        return self.connection(mapper).execute(clause, params, **kwargs)
    def close(self):
        """closes this Session.  
        
        TODO: what should we do here ?
        """
        if self.transaction is not None:
            self.transaction.close()
    def bind_mapper(self, mapper, bindto):
        """binds the given Mapper to the given Engine or Connection.  All subsequent operations involving this
        Mapper will use the given bindto."""
        self.binds[mapper] = bindto
    def bind_table(self, table, bindto):
        """binds the given Table to the given Engine or Connection.  All subsequent operations involving this
        Table will use the given bindto."""
        self.binds[table] = bindto
    def get_bind(self, mapper):
        """given a Mapper, returns the Engine or Connection which is used to execute statements on behalf of this 
        Mapper.  Calling connect() on the return result will always result in a Connection object.  This method 
        disregards any SessionTransaction that may be in progress.
        
        The order of searching is as follows:
        
        if an Engine or Connection was bound to this Mapper specifically within this Session, returns that 
        Engine or Connection.
        
        if an Engine or Connection was bound to this Mapper's underlying Table within this Session
        (i.e. not to the Table directly), returns that Engine or Conneciton.
        
        if an Engine or Connection was bound to this Session, returns that Engine or Connection.
        
        finally, returns the Engine which was bound directly to the Table's MetaData object.  
        
        If no Engine is bound to the Table, an exception is raised.
        """
        if mapper is None:
            return self.bind_to
        elif self.binds.has_key(mapper):
            return self.binds[mapper]
        elif self.binds.has_key(mapper.table):
            return self.binds[mapper.table]
        elif self.bind_to is not None:
            return self.bind_to
        else:
            return mapper.table.engine
    def query(self, mapper_or_class):
        """given a mapper or Class, returns a new Query object corresponding to this Session and the mapper, or the classes' primary mapper."""
        if isinstance(mapper_or_class, type):
            return query.Query(class_mapper(mapper_or_class), self)
        else:
            return query.Query(mapper_or_class, self)
    def _sql(self):
        class SQLProxy(object):
            def __getattr__(self, key):
                def call(*args, **kwargs):
                    kwargs[engine] = self.engine
                    return getattr(sql, key)(*args, **kwargs)
                    
    sql = property(_sql)
    
        
    def get_id_key(ident, class_, entity_name=None):
        """returns an identity-map key for use in storing/retrieving an item from the identity
        map, given a tuple of the object's primary key values.

        ident - a tuple of primary key values corresponding to the object to be stored.  these
        values should be in the same order as the primary keys of the table 

        class_ - a reference to the object's class

        entity_name - optional string name to further qualify the class
        """
        return (class_, tuple(ident), entity_name)
    get_id_key = staticmethod(get_id_key)

    def get_row_key(row, class_, primary_key, entity_name=None):
        """returns an identity-map key for use in storing/retrieving an item from the identity
        map, given a result set row.

        row - a sqlalchemy.dbengine.RowProxy instance or other map corresponding result-set
        column names to their values within a row.

        class_ - a reference to the object's class

        primary_key - a list of column objects that will target the primary key values
        in the given row.
        
        entity_name - optional string name to further qualify the class
        """
        return (class_, tuple([row[column] for column in primary_key]), entity_name)
    get_row_key = staticmethod(get_row_key)
    
    def begin(self, *obj):
        """deprecated"""
        raise InvalidRequestError("Session.begin() is deprecated.  use install_mod('legacy_session') to enable the old behavior")    
    def commit(self, *obj):
        """deprecated"""
        raise InvalidRequestError("Session.commit() is deprecated.  use install_mod('legacy_session') to enable the old behavior")    

    def flush(self, *obj):
        """flushes all the object modifications present in this session to the database.  if object
        arguments are given, then only those objects (and immediate dependencies) are flushed."""
        self.uow.flush(self, *obj)
    
    def load(self, class_, *ident):
        """given a class and a primary key identifier, loads the corresponding object."""
        return self.query(class_).get(*ident)
                
    def refresh(self, *obj):
        """reloads the attributes for the given objects from the database, clears
        any changes made."""
        for o in obj:
            self.uow.refresh(o)

    def expire(self, *obj):
        """invalidates the data in the given objects and sets them to refresh themselves
        the next time they are requested."""
        for o in obj:
            self.uow.expire(o)

    def expunge(self, *obj):
        """removes the given objects from this Session.  this will free all internal references to the objects."""
        for o in obj:
            self.uow.expunge(o)
            
    def save(self, *obj, **kwargs):
        """adds unsaved objects to this Session.  
        
        The 'entity_name' keyword argument can also be given which will be assigned
        to the instances if given.
        """
        for o in obj:
            for c in object_mapper(o, **kwargs).cascade_iterator('save-update', o):
                if c is o:
                    self._save_impl(c, **kwargs)
                else:
                    self.save_or_update(c, **kwargs)

    def update(self, *obj, **kwargs):
        for o in obj:
            for c in object_mapper(o, **kwargs).cascade_iterator('save-update', o):
                if c is o:
                    self._update_impl(c, **kwargs)
                else:
                    self.save_or_update(c, **kwargs)

    def save_or_update(self, *obj, **kwargs):
        for o in obj:
            for c in object_mapper(o, *kwargs).cascade_iterator('save-update', o):
                key = getattr(o, '_instance_key', None)
                if key is None:
                    self._save_impl(c, **kwargs)
                else:
                    self._update_impl(c, **kwargs)
                    
    def _save_impl(self, object, **kwargs):
        if hasattr(object, '_instance_key'):
            if not self.uow.has_key(object._instance_key):
                raise InvalidRequestError("Instance '%s' attached to a different Session" % repr(object))
        else:
            entity_name = kwargs.get('entity_name', None)
            if entity_name is not None:
                m = class_mapper(object.__class__, entity_name=entity_name)
                m._assign_entity_name(object)
            self._register_new(object)

    def _update_impl(self, object, **kwargs):
        if self._is_bound(object) and object not in self.deleted:
            return
        if not hasattr(object, '_instance_key'):
            raise InvalidRequestError("Instance '%s' is not persisted" % repr(object))
        if global_attributes.is_modified(object):
            self._register_dirty(object)
        else:
            self._register_clean(object)
        
    def _register_new(self, obj):
        self._bind_to(obj)
        self.uow.register_new(obj)
    def _register_dirty(self, obj):
        self._bind_to(obj)
        self.uow.register_dirty(obj)
    def _register_clean(self, obj):
        self._bind_to(obj)
        self.uow.register_clean(obj)
    def _register_deleted(self, obj):
        self._bind_to(obj)
        self.uow.register_deleted(obj)
    def _bind_to(self, obj):
        """given an object, binds it to this session.  changes on the object will affect
        the currently scoped UnitOfWork maintained by this session."""
        if getattr(obj, '_sa_session_id', None) != self.hash_key:
            old = getattr(obj, '_sa_session_id', None)
            # remove from old session.  we do this gingerly since _sessions is a WeakValueDict
            # and it might be affected by other threads
            if old is not None:
                try:
                    sess = _sessions[old]
                except KeyError:
                    sess = None
                if sess is not None:
                    sess.expunge(old)
            obj._sa_session_id = self.hash_key
    def _is_bound(self, obj):
        return getattr(obj, '_sa_session_id', None) == self.hash_key
    def __contains__(self, obj):
        return self._is_bound(obj) and (obj in self.uow.new or self.uow.has_key(obj._instance_key))
        
    def _get(self, key):
        return self.uow._get(key)
    def has_key(self, key):
        return self.uow.has_key(key)
    def is_expired(self, instance, **kwargs):
        return self.uow.is_expired(instance, **kwargs)
        
    dirty = property(lambda s:s.uow.dirty)
    deleted = property(lambda s:s.uow.deleted)
    new = property(lambda s:s.uow.new)
    identity_map = property(lambda s:s.uow.identity_map)
    
    def clear(self):
        """removes all object instances from this Session.  this is equivalent to calling expunge() for all
        objects in this Session."""
        self.uow = unitofwork.UnitOfWork()

    def delete(self, *obj, **kwargs):
        """registers the given objects to be deleted upon the next flush().  If the given objects are not part of this
        Session, they will be imported.  the objects are expected to either have an _instance_key
        attribute or have all of their primary key attributes populated.
        
        the keyword argument 'entity_name' can also be provided which will be used by the import."""
        for o in obj:
            for c in object_mapper(o, **kwargs).cascade_iterator('delete', o):
                print "CASCADING DELETE TO", c
                if not self._is_bound(c):
                    c = self.import_(c, **kwargs)
                self.uow.register_deleted(c)


    def merge(self, instance, entity_name=None):
        """given an instance that represents a saved item, adds it to this session.
        the return value is either the given instance, or if an instance corresponding to the 
        identity of the given instance already exists within this session, then that instance is returned;
        the returned instance should always be used following this method.
        
        if the given instance does not have an _instance_key and also does not have all 
        of its primary key attributes populated, an exception is raised.  similarly, if no
        mapper can be located for the given instance, an exception is raised.

        this method should be used for any object instance that is coming from a serialized
        storage, or was loaded by a Session other than this one.
                
        the keyword parameter entity_name is optional and is used to locate a Mapper for this
        class which also specifies the given entity name.
        """
        if instance is None:
            return None
        key = getattr(object, '_instance_key', None)
        if key is None:
            mapper = object_mapper(object, raiseerror=False)
            if mapper is None:
                mapper = class_mapper(object, entity_name=entity_name)
            ident = mapper.identity(object)
            for k in ident:
                if k is None:
                    if raiseerror:
                        raise InvalidRequestError("Instance '%s' does not have a full set of identity values, and does not represent a saved entity in the database.  Use the add() method to add unsaved instances to this Session." % str(object))
                    else:
                        return None
            key = mapper.identity_key(*ident)
        u = self.uow
        if u.identity_map.has_key(key):
            return u.identity_map[key]
        else:
            instance._instance_key = key
            u.identity_map[key] = instance
            self._bind_to(instance)
            return instance
            
    def import_instance(self, *args, **kwargs):
        """deprecated; a synynom for import()"""
        return self.merge(*args, **kwargs)

def get_id_key(ident, class_, entity_name=None):
    return Session.get_id_key(ident, class_, entity_name)

def get_row_key(row, class_, primary_key, entity_name=None):
    return Session.get_row_key(row, class_, primary_key, entity_name)

def mapper(*args, **params):
    return sqlalchemy.mapping.mapper(*args, **params)

def object_mapper(obj, **kwargs):
    return sqlalchemy.mapping.object_mapper(obj, **kwargs)

def class_mapper(class_, **kwargs):
    return sqlalchemy.mapping.class_mapper(class_, **kwargs)

# this is the AttributeManager instance used to provide attribute behavior on objects.
# to all the "global variable police" out there:  its a stateless object.
global_attributes = unitofwork.global_attributes

# this dictionary maps the hash key of a Session to the Session itself, and 
# acts as a Registry with which to locate Sessions.  this is to enable
# object instances to be associated with Sessions without having to attach the
# actual Session object directly to the object instance.
_sessions = weakref.WeakValueDictionary() 

def get_session(obj=None, raiseerror=True):
    """returns the Session corrseponding to the given object instance.  By default, if the object is not bound
    to any Session, then an error is raised (or None is returned if raiseerror=False).  This behavior can be changed
    using the "threadlocal" mod, which will add an additional step to return a Session that is bound to the current 
    thread."""
    if obj is not None:
        # does it have a hash key ?
        hashkey = getattr(obj, '_sa_session_id', None)
        if hashkey is not None:
            # ok, return that
            try:
                return _sessions[hashkey]
            except KeyError:
                if raiseerror:
                    raise InvalidRequestError("Session '%s' referenced by object '%s' no longer exists" % (hashkey, repr(obj)))
                else:
                    return None
                    
    return _default_session(obj=obj, raiseerror=raiseerror)

def _default_session(obj=None, raiseerror=True):
    if obj is None:
        if raiseerror:
            raise InvalidRequestError("Thread-local Sessions are disabled by default.  Use 'import sqlalchemy.mods.threadlocal' to enable.")
        else:
            return None
    else:
        if raiseerror:
            raise InvalidRequestError("Object '%s' not bound to any Session" % (repr(obj)))
        else:
            return None
            
unitofwork.get_session = get_session

