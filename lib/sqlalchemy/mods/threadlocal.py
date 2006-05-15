from sqlalchemy import util, engine, mapper
from sqlalchemy.ext.sessioncontext import SessionContext
from sqlalchemy.orm.mapper import global_extensions
from sqlalchemy.orm.session import Session
import sqlalchemy
import sys, types

"""this plugin installs thread-local behavior at the Engine and Session level.

The default Engine strategy will be "threadlocal", producing TLocalEngine instances for create_engine by default.
With this engine, connect() method will return the same connection on the same thread, if it is already checked out
from the pool.  this greatly helps functions that call multiple statements to be able to easily use just one connection
without explicit "close" statements on result handles.

on the Session side, module-level methods will be installed within the objectstore module, such as flush(), delete(), etc.
which call this method on the thread-local session.

Note: this mod creates a global, thread-local session context named sqlalchemy.objectstore. All mappers created
while this mod is installed will reference this global context when creating new mapped object instances.
"""

class Objectstore(SessionContext):
    def __getattr__(self, key):
        return getattr(self.current, key)
    def get_session(self):
        return self.current

def monkeypatch_query_method(class_, name):
    def do(self, *args, **kwargs):
        query = class_.mapper.query()
        getattr(query, name)(*args, **kwargs)
    setattr(class_, name, classmethod(do))

def monkeypatch_objectstore_method(class_, name):
    def do(self, *args, **kwargs):
        session = sqlalchemy.objectstore.current
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

def _mapper_extension():
    return SessionContext._get_mapper_extension(sqlalchemy.objectstore)

objectstore = Objectstore(Session)
def install_plugin():
    sqlalchemy.objectstore = objectstore
    global_extensions.append(_mapper_extension)
    engine.default_strategy = 'threadlocal'
    sqlalchemy.assign_mapper = assign_mapper

def uninstall_plugin():
    engine.default_strategy = 'plain'
    global_extensions.remove(_mapper_extension)

install_plugin()
