# pool.py - Connection pooling for SQLAlchemy
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


"""provides a connection pool implementation, which optionally manages connections
on a thread local basis.  Also provides a DBAPI2 transparency layer so that pools can
be managed automatically, based on module type and connect arguments,
 simply by calling regular DBAPI connect() methods."""

import Queue, weakref, string, cPickle
import util, exceptions

try:
    import thread
    import threading
except:
    import dummythread as thread
    import dummythreading as threading

proxies = {}

def manage(module, **params):
    """given a DBAPI2 module and pool management parameters, returns a proxy for the module
    that will automatically pool connections.  Options are delivered to an underlying DBProxy
    object.

    Arguments:
    module : a DBAPI2 database module.
    
    Options:
    echo=False : if set to True, connections being pulled and retrieved from/to the pool will
    be logged to the standard output, as well as pool sizing information.

    use_threadlocal=True : if set to True, repeated calls to connect() within the same
    application thread will be guaranteed to return the same connection object, if one has
    already been retrieved from the pool and has not been returned yet. This allows code to
    retrieve a connection from the pool, and then while still holding on to that connection,
    to call other functions which also ask the pool for a connection of the same arguments;
    those functions will act upon the same connection that the calling method is using.

    poolclass=QueuePool : the default class used by the pool module to provide pooling.
    QueuePool uses the Python Queue.Queue class to maintain a list of available connections.

    pool_size=5 : used by QueuePool - the size of the pool to be maintained. This is the
    largest number of connections that will be kept persistently in the pool. Note that the
    pool begins with no connections; once this number of connections is requested, that
    number of connections will remain.

    max_overflow=10 : the maximum overflow size of the pool. When the number of checked-out
    connections reaches the size set in pool_size, additional connections will be returned up
    to this limit. When those additional connections are returned to the pool, they are
    disconnected and discarded. It follows then that the total number of simultaneous
    connections the pool will allow is pool_size + max_overflow, and the total number of
    "sleeping" connections the pool will allow is pool_size. max_overflow can be set to -1 to
    indicate no overflow limit; no limit will be placed on the total number of concurrent
    connections.
    
    """
    try:
        return proxies[module]
    except KeyError:
        return proxies.setdefault(module, DBProxy(module, **params))    

def clear_managers():
    """removes all current DBAPI2 managers.  all pools and connections are disposed."""
    for manager in proxies.values():
        manager.close()
    proxies.clear()

    
class Pool(object):
    def __init__(self, echo = False, use_threadlocal = True, logger=None):
        self._threadconns = weakref.WeakValueDictionary()
        self._use_threadlocal = use_threadlocal
        self.echo = echo
        self._logger = logger or util.Logger(origin='pool')
    
    def unique_connection(self):
        return ConnectionFairy(self).checkout()
            
    def connect(self):
        if not self._use_threadlocal:
            return ConnectionFairy(self).checkout()
            
        try:
            return self._threadconns[thread.get_ident()].checkout()
        except KeyError:
            agent = ConnectionFairy(self).checkout()
            self._threadconns[thread.get_ident()] = agent
            return agent

    def return_conn(self, agent):
        if self._use_threadlocal:
            try:
                del self._threadconns[thread.get_ident()]
            except KeyError:
                pass
        self.do_return_conn(agent.connection)
        
    def get(self):
        return self.do_get()
    
    def return_invalid(self):
        self.do_return_invalid()
            
    def do_get(self):
        raise NotImplementedError()
        
    def do_return_conn(self, conn):
        raise NotImplementedError()
        
    def do_return_invalid(self):
        raise NotImplementedError()
        
    def status(self):
        raise NotImplementedError()

    def log(self, msg):
        self._logger.write(msg)

class ConnectionFairy(object):
    def __init__(self, pool, connection=None):
        self.pool = pool
        self.__counter = 0
        if connection is not None:
            self.connection = connection
        else:
            try:
                self.connection = pool.get()
            except:
                self.connection = None
                self.pool.return_invalid()
                raise
        if self.pool.echo:
            self.pool.log("Connection %s checked out from pool" % repr(self.connection))
    def invalidate(self):
        if self.pool.echo:
            self.pool.log("Invalidate connection %s" % repr(self.connection))
        self.connection.rollback()
        self.connection = None
        self.pool.return_invalid()
    def cursor(self):
        return CursorFairy(self, self.connection.cursor())
    def __getattr__(self, key):
        return getattr(self.connection, key)
    def checkout(self):
        if self.connection is None:
            raise "this connection is closed"
        self.__counter +=1
        return self    
    def close(self):
        self.__counter -=1
        if self.__counter == 0:
            self._close()
    def __del__(self):
        self._close()
    def _close(self):
        if self.connection is not None:
            if self.pool.echo:
                self.pool.log("Connection %s being returned to pool" % repr(self.connection))
            self.connection.rollback()
            self.pool.return_conn(self)
            self.pool = None
            self.connection = None
            
class CursorFairy(object):
    def __init__(self, parent, cursor):
        self.parent = parent
        self.cursor = cursor
    def __getattr__(self, key):
        return getattr(self.cursor, key)

class SingletonThreadPool(Pool):
    """Maintains one connection per each thread, never moving to another thread.  this is
    used for SQLite."""
    def __init__(self, creator, **params):
        Pool.__init__(self, **params)
        self._conns = {}
        self._creator = creator

    def dispose(self):
        pass
    def status(self):
        return "SingletonThreadPool id:%d thread:%d size: %d" % (id(self), thread.get_ident(), len(self._conns))

    def do_return_conn(self, conn):
        pass
        
    def do_return_invalid(self):
        try:
            del self._conns[thread.get_ident()]
        except KeyError:
            pass
            
    def do_get(self):
        try:
            return self._conns[thread.get_ident()]
        except KeyError:
            c = self._creator()
            self._conns[thread.get_ident()] = c
            return c
    
class QueuePool(Pool):
    """uses Queue.Queue to maintain a fixed-size list of connections."""
    def __init__(self, creator, pool_size = 5, max_overflow = 10, timeout=30, **params):
        Pool.__init__(self, **params)
        self._creator = creator
        self._pool = Queue.Queue(pool_size)

        # modify the pool's mutex to be an RLock.  this is because a rare condition can
        # occur where a ConnectionFairy's __del__ method gets called within the get() method
        # of the Queue (and then tries to do a put() within the get()), causing a re-entrant hang.
        # the RLock allows the mutex to be reentrant within that case.
        self._pool.mutex = threading.RLock()
        self._pool.not_empty = threading.Condition(self._pool.mutex)
        self._pool.not_full = threading.Condition(self._pool.mutex)
        
        self._overflow = 0 - pool_size
        self._max_overflow = max_overflow
        self._timeout = timeout
    
    def do_return_conn(self, conn):
        try:
            self._pool.put(conn, False)
        except Queue.Full:
            self._overflow -= 1

    def do_return_invalid(self):
        if self._pool.full():
            self._overflow -= 1
        
    def do_get(self):
        try:
            return self._pool.get(self._max_overflow > -1 and self._overflow >= self._max_overflow, self._timeout)
        except Queue.Empty:
            if self._overflow >= self._max_overflow:
                raise exceptions.TimeoutError("QueuePool limit of size %d overflow %d reached, connection timed out" % (self.size(), self.overflow()))
            self._overflow += 1
            return self._creator()

    def dispose(self):
        while True:
            try:
                conn = self._pool.get(False)
                conn.close()
            except Queue.Empty:
                break
    def __del__(self):
        self.dispose()

    def status(self):
        tup = (self.size(), self.checkedin(), self.overflow(), self.checkedout())
        return "Pool size: %d  Connections in pool: %d Current Overflow: %d Current Checked out connections: %d" % tup
        
    def size(self):
        return self._pool.maxsize
    
    def checkedin(self):
        return self._pool.qsize()
    
    def overflow(self):
        return self._overflow
    
    def checkedout(self):
        return self._pool.maxsize - self._pool.qsize() + self._overflow
        

class DBProxy(object):
    """proxies a DBAPI2 connect() call to a pooled connection keyed to the specific connect
    parameters."""
    
    def __init__(self, module, poolclass = QueuePool, **params):
        """
        module is a DBAPI2 module
        poolclass is a Pool class, defaulting to QueuePool.
        other parameters are sent to the Pool object's constructor.
        """
        self.module = module
        self.params = params
        self.poolclass = poolclass
        self.pools = {}

    def close(self):
        for key in self.pools.keys():
            del self.pools[key]

    def __del__(self):
        self.close()
            
    def get_pool(self, *args, **params):
        key = self._serialize(*args, **params)
        try:
            return self.pools[key]
        except KeyError:
            pool = self.poolclass(lambda: self.module.connect(*args, **params), **self.params)
            self.pools[key] = pool
            return pool
        
    def connect(self, *args, **params):
        """connects to a database using this DBProxy's module and the given connect
        arguments.  if the arguments match an existing pool, the connection will be returned
        from the pool's current thread-local connection instance, or if there is no
        thread-local connection instance it will be checked out from the set of pooled
        connections.  If the pool has no available connections and allows new connections to
        be created, a new database connection will be made."""
        return self.get_pool(*args, **params).connect()
    
    def dispose(self, *args, **params):
        """disposes the connection pool referenced by the given connect arguments."""
        key = self._serialize(*args, **params)
        try:
            del self.pools[key]
        except KeyError:
            pass
        
    def _serialize(self, *args, **params):
        return cPickle.dumps([args, params])

