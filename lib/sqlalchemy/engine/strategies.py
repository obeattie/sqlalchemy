"""defines different strategies for creating new instances of sql.Engine.  
by default there are two, one which is the "thread-local" strategy, one which is the "plain" strategy. 
new strategies can be added via constructing a new EngineStrategy object which will add itself to the
list of available strategies here, or replace one of the existing name.  
this can be accomplished via a mod; see the sqlalchemy/mods package for details."""

import re
from cgi import parse_qsl

from sqlalchemy.engine import base, default, transactional

strategies = {}

class EngineStrategy(object):
    """defines a function that receives input arguments and produces an instance of sql.Engine, typically
    an instance sqlalchemy.engine.base.ComposedSQLEngine or a subclass."""
    def __init__(self, name):
        """constructs a new EngineStrategy object and sets it in the list of available strategies
        under this name."""
        self.name = name
        strategies[self.name] = self
    def create(self, *args, **kwargs):
        """given arguments, returns a new sql.Engine instance."""
        raise NotImplementedError()
    

class PlainEngineStrategy(EngineStrategy):
    def __init__(self):
        EngineStrategy.__init__(self, 'plain')
    def create(self, name, opts=None, **kwargs):
        (module, opts) = _parse_db_args(name, opts)

        dialect = module.dialect(**kwargs)

        poolargs = kwargs.copy()
        poolargs['echo'] = poolargs.get('echo_pool', False)
        poolclass = getattr(module, 'poolclass', None)
        if poolclass is not None:
            poolargs.setdefault('poolclass', poolclass)
        poolargs['use_threadlocal'] = False
        provider = default.PoolConnectionProvider(dialect, opts, **poolargs)

        return base.ComposedSQLEngine(provider, dialect, **kwargs)
PlainEngineStrategy()

class ThreadLocalEngineStrategy(EngineStrategy):
    def __init__(self):
        EngineStrategy.__init__(self, 'threadlocal')
    def create(self, name, opts=None, **kwargs):
        (module, opts) = _parse_db_args(name, opts)

        dialect = module.dialect(**kwargs)

        poolargs = kwargs.copy()
        poolargs['echo'] = poolargs.get('echo_pool', False)
        poolclass = getattr(module, 'poolclass', None)
        if poolclass is not None:
            poolargs.setdefault('poolclass', poolclass)
        poolargs['use_threadlocal'] = True
        provider = transactional.TLocalConnectionProvider(dialect, opts, **poolargs)

        return transactional.TLEngine(provider, dialect, **kwargs)
ThreadLocalEngineStrategy()


def _parse_db_args(name, opts=None):
    ret = _parse_rfc1738_args(name, opts=opts)
    #if ret is None:
    #    ret = _parse_keyvalue_args(name, opts=opts)
    if ret is not None:
        (name, opts) = ret

    module = getattr(__import__('sqlalchemy.databases.%s' % name).databases, name)
    return (module, opts)
        
def _parse_rfc1738_args(name, opts=None):
    pattern = re.compile(r'''
            (\w+)://
            (?:
                ([^:]*)
                (?::(.*))?
            @)?
            (?:
                ([^/:]*)
                (?::([^/]*))?
            )
            (?:/(.*))?
            '''
            , re.X)
    
    m = pattern.match(name)
    if m is not None and (m.group(4) or m.group(6)):
        (name, username, password, host, port, database) = m.group(1, 2, 3, 4, 5, 6)
        opts = {'username':username,'password':password,'host':host,'port':port,'database':database,'filename':(database or host)}
        return (name, opts)
    else:
        return None

def _parse_keyvalue_args(name, opts=None):
    m = re.match( r'(\w+)://(.*)', name)
    if m is not None:
        (name, args) = m.group(1, 2)
        opts = dict( parse_qsl( args ) )
        return (name, opts)
    else:
        return None
    
    
