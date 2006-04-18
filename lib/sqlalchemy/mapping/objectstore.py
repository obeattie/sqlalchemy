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
    def __init__(self, session, parent=None):
        self.session = session
        self.connections = {}
        self.parent = parent
    def connection(self, mapper):
        if self.parent is not None:
            return self.parent.connection(mapper)
        engine = self.session.get_bind(mapper)
        try:
            return self.connections[engine][0]
        except KeyError:
            c = engine.connect()
            self.connections[engine] = (c, c.begin())
            return c
    def _begin(self):
        return SessionTransaction(self.session, self)
    def commit(self):
        if self.parent is not None:
            return
        for t in self.connections.values():
            t[1].commit()
        self.close()
    def rollback(self):
        if self.parent is not None:
            self.parent.rollback()
            return
        for t in self.connections.values():
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

    def create_transaction(self):
        """returns a new SessionTransaction corresponding to an existing or new transaction.
        if the transaction is new, the returned SessionTransaction will have commit control
        over the underlying transaction, else will have rollback control only."""
        if self.transaction is not None:
            return self.transaction._begin()
        else:
            self.transaction = SessionTransaction(self)
            return self.transaction
    def connect(self, mapper=None, **kwargs):
        """returns a unique connection corresponding to the given mapper.  this connection
        will not be part of any pre-existing transactional context."""
        return self.get_bind(mapper).connect(**kwargs)
    def connection(self, mapper):
        """returns a connection corresponding to the given mapper.  used by the execute()
        method which performs select operations for Mapper and Query.
        if this Session is transactional, 
        the connection will be in the context of this session's transaction.  otherwise, the connection
        will be unique, and will also have the close_with_result flag set to True so that the connection
        can be closed out using the result alone."""
        if self.transaction is not None:
            return self.transaction.connection(mapper)
        else:
            return self.connect(mapper, close_with_result=True)
    def execute(self, mapper, clause, params, **kwargs):
        return self.connection(mapper).execute(clause, params, **kwargs)
    def close(self):
        if self.transaction is not None:
            self.transaction.close()
    def bind_mapper(self, mapper, bindto):
        self.binds[mapper] = bindto
    def bind_table(self, table, bindto):
        self.binds[table] = bindto
    def get_bind(self, mapper):
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
        raise InvalidRequestError("Session.begin() is deprecated.  use install_mod('legacy_session') to enable the old behavior")    
    def commit(self, *obj):
        raise InvalidRequestError("Session.commit() is deprecated.  use install_mod('legacy_session') to enable the old behavior")    

    def flush(self, *obj):
        self.uow.flush(self, *obj)
            
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
        for o in obj:
            self.uow.expunge(obj)
            
    def register_clean(self, *obj):
        for o in obj:
            self._bind_to(o)
            self.uow.register_clean(o)
        
    def register_new(self, *obj):
        for o in obj:
            self._bind_to(o)
            self.uow.register_new(o)

    def _bind_to(self, obj):
        """given an object, binds it to this session.  changes on the object will affect
        the currently scoped UnitOfWork maintained by this session."""
        obj._sa_session_id = self.hash_key

    def __getattr__(self, key):
        """proxy other methods to our underlying UnitOfWork"""
        return getattr(self.uow, key)

    def clear(self):
        self.uow = unitofwork.UnitOfWork()

    def delete(self, *obj):
        """registers the given objects as to be deleted upon the next commit"""
        for o in obj:
            self.uow.register_deleted(o)
        
    def import_instance(self, instance):
        """places the given instance in the current thread's unit of work context,
        either in the current IdentityMap or marked as "new".  Returns either the object
        or the current corresponding version in the Identity Map.

        this method should be used for any object instance that is coming from a serialized
        storage, from another thread (assuming the regular threaded unit of work model), or any
        case where the instance was loaded/created corresponding to a different base unitofwork
        than the current one."""
        if instance is None:
            return None
        key = getattr(instance, '_instance_key', None)
        mapper = object_mapper(instance)
        u = self.uow
        if key is not None:
            if u.identity_map.has_key(key):
                return u.identity_map[key]
            else:
                instance._instance_key = key
                u.identity_map[key] = instance
                self._bind_to(instance)
        else:
            u.register_new(instance)
        return instance


def get_id_key(ident, class_, entity_name=None):
    return Session.get_id_key(ident, class_, entity_name)

def get_row_key(row, class_, primary_key, entity_name=None):
    return Session.get_row_key(row, class_, primary_key, entity_name)


def mapper(*args, **params):
    return sqlalchemy.mapping.mapper(*args, **params)

def object_mapper(obj):
    return sqlalchemy.mapping.object_mapper(obj)

def class_mapper(class_):
    return sqlalchemy.mapping.class_mapper(class_)

global_attributes = unitofwork.global_attributes

_sessions = weakref.WeakValueDictionary() # all referenced sessions (including user-created)

def get_session(obj=None, raiseerror=True):
    if obj is None:
        if raiseerror:
            raise InvalidRequestError("Thread-local Sessions are disabled by default.  Use install_mods('threadlocal') to enable.")
        else:
            return None
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
    else:
        if raiseerror:
            raise InvalidRequestError("Object '%s' not bound to any Session" % (repr(obj)))
        else:
            return None
unitofwork.get_session = get_session

