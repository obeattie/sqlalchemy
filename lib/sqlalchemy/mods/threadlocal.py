from sqlalchemy import util, engine
from sqlalchemy.mapping import unitofwork, objectstore

import sys

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

get_session = objectstore.get_session

def begin(*obj):
    return get_session().begin(*obj)
def commit(*obj):
    return get_session().commit(*obj)

def flush(*obj):
    """flushes the current UnitOfWork transaction.  if a transaction was begun 
    via begin(), flushes only those objects that were created, modified, or deleted
    since that begin statement.  otherwise flushes all objects that have been
    changed.

    if individual objects are submitted, then only those objects are committed, and the 
    begin/commit cycle is not affected."""
    get_session().flush(*obj)

def clear():
    """removes all current UnitOfWorks and IdentityMaps for this thread and 
    establishes a new one.  It is probably a good idea to discard all
    current mapped object instances, as they are no longer in the Identity Map."""
    get_session().clear()

def refresh(*obj):
    """reloads the state of this object from the database, and cancels any in-memory
    changes."""
    get_session().refresh(*obj)

def expire(*obj):
    """invalidates the data in the given objects and sets them to refresh themselves
    the next time they are requested."""
    get_session().expire(*obj)

def expunge(*obj):
    get_session().expunge(*obj)

def delete(*obj):
    """registers the given objects as to be deleted upon the next commit"""
    s = get_session().delete(*obj)

def has_key(key):
    """returns True if the current thread-local IdentityMap contains the given instance key"""
    return get_session().has_key(key)

def has_instance(instance):
    """returns True if the current thread-local IdentityMap contains the given instance"""
    return get_session().has_instance(instance)

def is_dirty(obj):
    """returns True if the given object is in the current UnitOfWork's new or dirty list,
    or if its a modified list attribute on an object."""
    return get_session().is_dirty(obj)

def instance_key(instance):
    """returns the IdentityMap key for the given instance"""
    return get_session().instance_key(instance)

def import_instance(instance):
    return get_session().import_instance(instance)

def install_plugin():
    mod = sys.modules[__name__]
    for name in ['import_instance', 'instance_key', 'has_instance', 'is_dirty', 'has_key', 'delete', 'expunge', 'expire', 'refresh', 'clear', 'flush', 'begin', 'commit']:
        setattr(objectstore, name, getattr(mod, name))
    reg = util.ScopedRegistry(objectstore.Session)
    objectstore._default_session = lambda *args, **kwargs: reg()
    engine.default_strategy = 'threadlocal'
install_plugin()
