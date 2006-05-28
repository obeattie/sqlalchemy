from sqlalchemy import schema, exceptions, util, sql, types
import StringIO, sys, re
import base, default

"""provides a thread-local transactional wrapper around the basic ComposedSQLEngine.  multiple calls to engine.connect()
will return the same connection for the same thread. also provides begin/commit methods on the engine itself
which correspond to a thread-local transaction."""

class TLSession(object):
    def __init__(self, engine):
        self.engine = engine
        self.__tcount = 0
    def get_connection(self, close_with_result=False):
        try:
            return self.__transaction
        except AttributeError:
            return TLConnection(self, close_with_result=close_with_result)
    def set_transaction(self, tlconnection, trans):
        if self.__tcount == 0:
            self.__transaction = tlconnection
            self.__trans = trans
        self.__tcount += 1
    def reset(self):
        try:
            del self.__transaction
            del self.__trans
        except AttributeError:
            pass
        self.__tcount = 0
        
    def begin(self):
        if self.__tcount == 0:
            self.__transaction = self.get_connection()
            self.__trans = self.__transaction._begin()
        self.__tcount += 1
        return self.__trans
    def rollback(self):
        if self.__tcount > 0:
            try:
                self.__trans.rollback()
            finally:
                self.reset()
    def commit(self):
        if self.__tcount == 1:
            try:
                self.__trans.commit()
            finally:
                self.reset()
        elif self.__tcount > 1:
            self.__tcount -= 1
    def is_begun(self):
        return self.__tcount > 0

class TLConnection(base.Connection):
    def __init__(self, session, close_with_result):
        base.Connection.__init__(self, session.engine, close_with_result=close_with_result)
        self.__session = session
    session = property(lambda s:s.__session)
    def _create_transaction(self, parent):
        return TLTransaction(self, parent)
    def _begin(self):
        return base.Connection.begin(self)
    def begin(self):
        trans = base.Connection.begin(self)
        self.__session.set_transaction(self, trans)
        return trans

class TLTransaction(base.Transaction):
    def commit(self):
        base.Transaction.commit(self)
        if not self.is_active:
            self.connection.session.reset()
    def rollback(self):
        base.Transaction.rollback(self)
        if not self.is_active:
            self.connection.session.reset()
            
class TLEngine(base.ComposedSQLEngine):
    """a ComposedSQLEngine that includes support for thread-local managed transactions.  This engine
    is better suited to be used with threadlocal Pool object."""
    def __init__(self, *args, **kwargs):
        """the TLEngine relies upon the ConnectionProvider having "threadlocal" behavior,
        so that once a connection is checked out for the current thread, you get that same connection
        repeatedly."""
        base.ComposedSQLEngine.__init__(self, *args, **kwargs)
        self.context = util.ThreadLocal()
    def raw_connection(self):
        """returns a DBAPI connection."""
        return self.connection_provider.get_connection()
    def connect(self, **kwargs):
        """returns a Connection that is not thread-locally scoped.  this is the equilvalent to calling
        "connect()" on a ComposedSQLEngine."""
        return base.Connection(self, self.connection_provider.unique_connection())

    def _session(self):
        if not hasattr(self.context, 'session'):
            self.context.session = TLSession(self)
        return self.context.session
    session = property(_session, doc="returns the current thread's TLSession")

    def contextual_connect(self, **kwargs):
        """returns a TLConnection which is thread-locally scoped."""
        return self.session.get_connection(**kwargs)
        
    def begin(self):
        return self.session.begin()
    def commit(self):
        self.session.commit()
    def rollback(self):
        self.session.rollback()

class TLocalConnectionProvider(default.PoolConnectionProvider):
    def unique_connection(self):
        return self._pool.unique_connection()
