from sqlalchemy import schema, exceptions, util, sql, types
import StringIO, sys, re
from sqlalchemy.engine import base, default

"""Provide a thread-local transactional wrapper around the basic ComposedSQLEngine.

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

    def begin(self):
        if self.__tcount == 0:
            self.__transaction = self.get_connection()
            self.__trans = self.__transaction._begin()
        self.__tcount += 1
        return self.__trans

    def rollback(self):
        if self.__tcount > 0:
            try:
                self.__trans._rollback_impl()
            finally:
                self.reset()

    def commit(self):
        if self.__tcount == 1:
            try:
                self.__trans._commit_impl()
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

    def _create_transaction(self, parent):
        return TLTransaction(self, parent)

    def _begin(self):
        return base.Connection.begin(self)

    def in_transaction(self):
        return self.session.in_transaction()

    def begin(self):
        return self.session.begin()

    def close(self):
        if self.__opencount == 1:
            base.Connection.close(self)
        self.__opencount -= 1

    def _force_close(self):
        self.__opencount = 0
        base.Connection.close(self)

class TLTransaction(base.Transaction):
    def _commit_impl(self):
        base.Transaction.commit(self)

    def _rollback_impl(self):
        base.Transaction.rollback(self)

    def commit(self):
        self.connection.session.commit()

    def rollback(self):
        self.connection.session.rollback()

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

    def begin(self):
        return self.session.begin()

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()

