from sqlalchemy import util, engine, mapper
from sqlalchemy.orm import unitofwork, session
import sqlalchemy
import sys, types

"""this plugin installs thread-local behavior at the Engine and Session level.

The default Engine strategy will be "threadlocal", producing TLocalEngine instances for create_engine by default.
With this engine, connect() method will return the same connection on the same thread, if it is already checked out
from the pool.  this greatly helps functions that call multiple statements to be able to easily use just one connection
without explicit "close" statements on result handles.

on the Session side, the get_session() method will be modified to return a thread-local Session when no arguments
are sent.  It will also install module-level methods within the objectstore module, such as flush(), delete(), etc.
which call this method on the thread-local session returned by get_session().

Without this plugin in use, all statement.execute() calls must be matched by a corresponding close() statement
on the returned result (or the result must be consumed completely).  Also, all mapper operations must use 
explicit Session objects when creating instances and creating queries.
"""

get_session = session.get_session

class Objectstore(object):
    def begin(self, obj):
        return get_session().begin(obj)
    def commit(self, obj):
        return get_session().commit(obj)
    def get_session(self, obj=None):
        return get_session(obj=obj)
    def flush(self, obj=None):
        """flushes the current UnitOfWork transaction.  if a transaction was begun 
        via begin(), flushes only those objects that were created, modified, or deleted
        since that begin statement.  otherwise flushes all objects that have been
        changed.

        if individual objects are submitted, then only those objects are committed, and the 
        begin/commit cycle is not affected."""
        get_session().flush(obj)

    def clear(self):
        """removes all current UnitOfWorks and IdentityMaps for this thread and 
        establishes a new one.  It is probably a good idea to discard all
        current mapped object instances, as they are no longer in the Identity Map."""
        get_session().clear()

    def refresh(self, obj):
        """reloads the state of this object from the database, and cancels any in-memory
        changes."""
        get_session().refresh(obj)

    def expire(self, obj):
        """invalidates the data in the given objects and sets them to refresh themselves
        the next time they are requested."""
        get_session().expire(obj)

    def expunge(self, obj):
        get_session().expunge(obj)

    def delete(self, obj):
        """registers the given objects as to be deleted upon the next commit"""
        s = get_session().delete(obj)

    def has_key(self, key):
        """returns True if the current thread-local IdentityMap contains the given instance key"""
        return get_session().has_key(key)

    def has_instance(self, instance):
        """returns True if the current thread-local IdentityMap contains the given instance"""
        return get_session().has_instance(instance)

    def is_dirty(self, obj):
        """returns True if the given object is in the current UnitOfWork's new or dirty list,
        or if its a modified list attribute on an object."""
        return get_session().is_dirty(obj)

    def instance_key(self, instance):
        """returns the IdentityMap key for the given instance"""
        return get_session().instance_key(instance)

    def import_instance(self, instance):
        return get_session().import_instance(instance)

def assign_mapper(class_, *args, **params):
    params.setdefault("is_primary", True)
    if not isinstance(getattr(class_, '__init__'), types.MethodType):
        def __init__(self, **kwargs):
             for key, value in kwargs.items():
                 setattr(self, key, value)
        class_.__init__ = __init__
    m = mapper(class_, *args, **params)
    class_.mapper = m
    # TODO: get these outta here, have to go off explicit session
    class_.get = m.get
    class_.select = m.select
    class_.select_by = m.select_by
    class_.selectone = m.selectone
    class_.get_by = m.get_by
    def commit(self):
        sqlalchemy.objectstore.commit(self)
    def delete(self):
        sqlalchemy.objectstore.delete(self)
    def expire(self):
        sqlalchemy.objectstore.expire(self)
    def refresh(self):
        sqlalchemy.objectstore.refresh(self)
    def expunge(self):
        sqlalchemy.objectstore.expunge(self)
    class_.commit = commit
    class_.delete = delete
    class_.expire = expire
    class_.refresh = refresh
    class_.expunge = expunge
    
def install_plugin():
    reg = util.ScopedRegistry(session.Session)
    session.register_default_session(lambda *args, **kwargs: reg())
    engine.default_strategy = 'threadlocal'
    sqlalchemy.objectstore = Objectstore()
    sqlalchemy.assign_mapper = assign_mapper

def uninstall_plugin():
    session.register_default_session(lambda *args, **kwargs:None)
    engine.default_strategy = 'plain'
    
install_plugin()
