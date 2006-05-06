from sqlalchemy import util, engine, mapper
from sqlalchemy.orm import session, current_session
import sqlalchemy
import sys, types

"""this plugin installs thread-local behavior at the Engine and Session level.

The default Engine strategy will be "threadlocal", producing TLocalEngine instances for create_engine by default.
With this engine, connect() method will return the same connection on the same thread, if it is already checked out
from the pool.  this greatly helps functions that call multiple statements to be able to easily use just one connection
without explicit "close" statements on result handles.

on the Session side, the current_session() method will be modified to return a thread-local Session when no arguments
are sent.  It will also install module-level methods within the objectstore module, such as flush(), delete(), etc.
which call this method on the thread-local session returned by current_session().


"""

class Objectstore(object):
    def __getattr__(self, key):
        return getattr(current_session(), key)
    def get_session(self):
        return current_session()
        
def monkeypatch_query_method(class_, name):
    def do(self, *args, **kwargs):
        query = class_.mapper.query()
        getattr(query, name)(*args, **kwargs)
    setattr(class_, name, classmethod(do))

def monkeypatch_objectstore_method(class_, name):
    def do(self, *args, **kwargs):
        session = current_session()
        getattr(session, name)(self, *args, **kwargs)
    setattr(class_, name, do)
    
def assign_mapper(class_, *args, **kwargs):
    kwargs.setdefault("is_primary", True)
    if not isinstance(getattr(class_, '__init__'), types.MethodType):
        def __init__(self, **kwargs):
             for key, value in kwargs.items():
                 setattr(self, key, value)
        class_.__init__ = __init__
    m = mapper(class_, *args, **kwargs)
    class_.mapper = m
    for name in ['get', 'select', 'select_by', 'selectone', 'get_by']:
        monkeypatch_query_method(class_, name)
    for name in ['flush', 'delete', 'expire', 'refresh', 'expunge', 'merge', 'update', 'save_or_update']:
        monkeypatch_objectstore_method(class_, name)
    
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
