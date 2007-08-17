from sqlalchemy import util
from sqlalchemy.engine import base

"""Provide a thread-local transactional wrapper around the root Engine class.

Multiple calls to engine.connect() will return the same connection for
the same thread. also provides begin/commit methods on the engine
itself which correspond to a thread-local transaction.
"""

class TLSession(object):
    def __init__(self, engine):
        self.engine = engine
        self.__tcount = 0

    def get_connection(self, close_with_result=False):
        try:
            return self.__transaction._increment_connect()
        except AttributeError:
            return TLConnection(self, close_with_result=close_with_result)

    def reset(self):
        try:
            self.__transaction._force_close()
            del self.__transaction
            del self.__trans
        except AttributeError:
            pass
        self.__tcount = 0

    def in_transaction(self):
        return self.__tcount > 0
    
    def prepare(self):
        if self.__tcount == 1:
            try:
                self.__trans._trans.prepare()
            finally:
                self.reset()

    def begin_twophase(self, xid=None):
        if self.__tcount == 0:
            self.__transaction = self.get_connection()
            self.__trans = self.__transaction._begin_twophase(xid=xid)
        self.__tcount += 1
        return self.__trans

    def begin(self, **kwargs):
        if self.__tcount == 0:
            self.__transaction = self.get_connection()
            self.__trans = self.__transaction._begin(**kwargs)
        self.__tcount += 1
        return self.__trans

    def rollback(self):
        if self.__tcount > 0:
            try:
                self.__trans._trans.rollback()
            finally:
                self.reset()

    def commit(self):
        if self.__tcount == 1:
            try:
                self.__trans._trans.commit()
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
        self.__opencount = 1

    session = property(lambda s:s.__session)

    def _increment_connect(self):
        self.__opencount += 1
        return self

    def _begin(self, **kwargs):
        return TLTransaction(super(TLConnection, self).begin(**kwargs), self.__session)
    
    def _begin_twophase(self, xid=None):
        return TLTransaction(super(TLConnection, self).begin_twophase(xid=xid), self.__session)
        
    def in_transaction(self):
        return self.session.in_transaction()

    def begin(self, **kwargs):
        return self.session.begin(**kwargs)

    def begin_twophase(self, xid=None):
        return self.session.begin_twophase(xid=xid)

    def close(self):
        if self.__opencount == 1:
            base.Connection.close(self)
        self.__opencount -= 1

    def _force_close(self):
        self.__opencount = 0
        base.Connection.close(self)

class TLTransaction(base.Transaction):
    def __init__(self, trans, session):
        self._trans = trans
        self._session = session

    connection = property(lambda s:s._trans.connection)
    is_active = property(lambda s:s._trans.is_active)

    def rollback(self):
        self._session.rollback()

    def prepare(self):
        self._session.prepare()
        
    def commit(self):
        self._session.commit()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._trans.__exit__(type, value, traceback)


class TLEngine(base.Engine):
    """An Engine that includes support for thread-local managed transactions.

    This engine is better suited to be used with threadlocal Pool
    object.
    """

    def __init__(self, *args, **kwargs):
        """The TLEngine relies upon the Pool having
        "threadlocal" behavior, so that once a connection is checked out
        for the current thread, you get that same connection
        repeatedly.
        """

        super(TLEngine, self).__init__(*args, **kwargs)
        self.context = util.ThreadLocal()

    def raw_connection(self):
        """Return a DBAPI connection."""

        return self.pool.connect()

    def connect(self, **kwargs):
        """Return a Connection that is not thread-locally scoped.

        This is the equivalent to calling ``connect()`` on a
        ComposedSQLEngine.
        """

        return base.Connection(self, self.pool.unique_connection())

    def _session(self):
        if not hasattr(self.context, 'session'):
            self.context.session = TLSession(self)
        return self.context.session

    session = property(_session, doc="returns the current thread's TLSession")

    def contextual_connect(self, **kwargs):
        """Return a TLConnection which is thread-locally scoped."""

        return self.session.get_connection(**kwargs)

    def begin(self, **kwargs):
        return self.session.begin(**kwargs)

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()

