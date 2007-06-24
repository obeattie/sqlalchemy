"""Define different strategies for creating new instances of sql.Engine.

By default there are two, one which is the "thread-local" strategy,
one which is the "plain" strategy.

New strategies can be added via constructing a new EngineStrategy
object which will add itself to the list of available strategies here,
or replace one of the existing name.  this can be accomplished via a
mod; see the sqlalchemy/mods package for details.
"""


from sqlalchemy.engine import base, default, threadlocal, url
from sqlalchemy import util, exceptions
from sqlalchemy import pool as poollib

strategies = {}

class EngineStrategy(object):
    """Define a function that receives input arguments and produces an
    instance of sql.Engine, typically an instance
    sqlalchemy.engine.base.Engine or a subclass.
    """

    def __init__(self, name):
        """Construct a new EngineStrategy object.

        Sets it in the list of available strategies under this name.
        """

        self.name = name
        strategies[self.name] = self

    def create(self, *args, **kwargs):
        """Given arguments, returns a new sql.Engine instance."""

        raise NotImplementedError()

class DefaultEngineStrategy(EngineStrategy):
    def create(self, name_or_url, **kwargs):
        # create url.URL object
        u = url.make_url(name_or_url)

        dialect_cls = u.get_dialect()

        dialect_args = {}
        # consume dialect arguments from kwargs
        for k in util.get_cls_kwargs(dialect_cls):
            if k in kwargs:
                dialect_args[k] = kwargs.pop(k)

        dbapi = kwargs.pop('module', None)
        if dbapi is None:
            dbapi_args = {}
            for k in util.get_func_kwargs(dialect_cls.dbapi):
                if k in kwargs:
                    dbapi_args[k] = kwargs.pop(k)
            dbapi = dialect_cls.dbapi(**dbapi_args)
        
        dialect_args['dbapi'] = dbapi
        
        # create dialect
        dialect = dialect_cls(**dialect_args)

        # assemble connection arguments
        (cargs, cparams) = dialect.create_connect_args(u)
        cparams.update(kwargs.pop('connect_args', {}))

        # look for existing pool or create
        pool = kwargs.pop('pool', None)
        if pool is None:
            def connect():
                try:
                    return dbapi.connect(*cargs, **cparams)
                except Exception, e:
                    raise exceptions.DBAPIError("Connection failed", e)
            creator = kwargs.pop('creator', connect)

            poolclass = kwargs.pop('poolclass', getattr(dialect_cls, 'poolclass', poollib.QueuePool))
            pool_args = {}

            # consume pool arguments from kwargs, translating a few of the arguments
            for k in util.get_cls_kwargs(poolclass):
                tk = {'echo':'echo_pool', 'timeout':'pool_timeout', 'recycle':'pool_recycle'}.get(k, k)
                if tk in kwargs:
                    pool_args[k] = kwargs.pop(tk)
            pool_args['use_threadlocal'] = self.pool_threadlocal()
            pool = poolclass(creator, **pool_args)
        else:
            if isinstance(pool, poollib._DBProxy):
                pool = pool.get_pool(*cargs, **cparams)
            else:
                pool = pool

        # create engine.
        engineclass = self.get_engine_cls()
        engine_args = {}
        for k in util.get_cls_kwargs(engineclass):
            if k in kwargs:
                engine_args[k] = kwargs.pop(k)

        # all kwargs should be consumed
        if len(kwargs):
            raise TypeError("Invalid argument(s) %s sent to create_engine(), using configuration %s/%s/%s.  Please check that the keyword arguments are appropriate for this combination of components." % (','.join(["'%s'" % k for k in kwargs]), dialect.__class__.__name__, pool.__class__.__name__, engineclass.__name__))

        return engineclass(pool, dialect, u, **engine_args)

    def pool_threadlocal(self):
        raise NotImplementedError()

    def get_engine_cls(self):
        raise NotImplementedError()

class PlainEngineStrategy(DefaultEngineStrategy):
    def __init__(self):
        DefaultEngineStrategy.__init__(self, 'plain')

    def pool_threadlocal(self):
        return False

    def get_engine_cls(self):
        return base.Engine

PlainEngineStrategy()

class ThreadLocalEngineStrategy(DefaultEngineStrategy):
    def __init__(self):
        DefaultEngineStrategy.__init__(self, 'threadlocal')

    def pool_threadlocal(self):
        return True

    def get_engine_cls(self):
        return threadlocal.TLEngine

ThreadLocalEngineStrategy()


class MockEngineStrategy(EngineStrategy):
    """Produces a single Connection object which dispatches statement executions
    to a passed-in function"""
    def __init__(self):
        EngineStrategy.__init__(self, 'mock')
        
    def create(self, name_or_url, executor, **kwargs):
        # create url.URL object
        u = url.make_url(name_or_url)

        dialect_cls = u.get_dialect()

        dialect_args = {}
        # consume dialect arguments from kwargs
        for k in util.get_cls_kwargs(dialect_cls):
            if k in kwargs:
                dialect_args[k] = kwargs.pop(k)

        # create dialect
        dialect = dialect_cls(**dialect_args)

        return MockEngineStrategy.MockConnection(dialect, executor)

    class MockConnection(base.Connectable):
        def __init__(self, dialect, execute):
            self._dialect = dialect
            self.execute = execute

        engine = property(lambda s: s)
        dialect = property(lambda s:s._dialect)
        
        def contextual_connect(self, **kwargs):
            return self

        def compiler(self, statement, parameters, **kwargs):
            return self._dialect.compiler(statement, parameters, engine=self, **kwargs)

        def create(self, entity, **kwargs):
            kwargs['checkfirst'] = False
            entity.accept_visitor(self.dialect.schemagenerator(self, **kwargs))

        def drop(self, entity, **kwargs):
            kwargs['checkfirst'] = False
            entity.accept_visitor(self.dialect.schemadropper(self, **kwargs))

        def execute(self, object, *multiparams, **params):
            raise NotImplementedError()

MockEngineStrategy()