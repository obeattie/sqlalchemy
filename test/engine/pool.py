import testbase
from testbase import PersistTest
import unittest, sys, os, time

import sqlalchemy.pool as pool
import sqlalchemy.exceptions as exceptions

class MockDBAPI(object):
    def connect(self, argument):
        return MockConnection()
class MockConnection(object):
    def close(self):
        pass
    def cursor(self):
        return MockCursor()
class MockCursor(object):
    def close(self):
        pass
mock_dbapi = MockDBAPI()
         
class PoolTest(PersistTest):
    
    def setUp(self):
        pool.clear_managers()

    def testmanager(self):
        manager = pool.manage(mock_dbapi)
        
        connection = manager.connect('foo.db')
        connection2 = manager.connect('foo.db')
        connection3 = manager.connect('bar.db')
        
        self.echo( "connection " + repr(connection))
        self.assert_(connection.cursor() is not None)
        self.assert_(connection is connection2)
        self.assert_(connection2 is not connection3)

    def testbadargs(self):
        manager = pool.manage(mock_dbapi)

        try:
            connection = manager.connect(None)
        except:
            pass
    
    def testnonthreadlocalmanager(self):
        manager = pool.manage(mock_dbapi, use_threadlocal = False)
        
        connection = manager.connect('foo.db')
        connection2 = manager.connect('foo.db')

        self.echo( "connection " + repr(connection))

        self.assert_(connection.cursor() is not None)
        self.assert_(connection is not connection2)

    def testqueuepool_del(self):
        self._do_testqueuepool(useclose=False)

    def testqueuepool_close(self):
        self._do_testqueuepool(useclose=True)

    def _do_testqueuepool(self, useclose=False):
        p = pool.QueuePool(creator = lambda: mock_dbapi.connect('foo.db'), pool_size = 3, max_overflow = -1, use_threadlocal = False, echo = False)
    
        def status(pool):
            tup = (pool.size(), pool.checkedin(), pool.overflow(), pool.checkedout())
            self.echo( "Pool size: %d  Connections in pool: %d Current Overflow: %d Current Checked out connections: %d" % tup)
            return tup
                
        c1 = p.connect()
        self.assert_(status(p) == (3,0,-2,1))
        c2 = p.connect()
        self.assert_(status(p) == (3,0,-1,2))
        c3 = p.connect()
        self.assert_(status(p) == (3,0,0,3))
        c4 = p.connect()
        self.assert_(status(p) == (3,0,1,4))
        c5 = p.connect()
        self.assert_(status(p) == (3,0,2,5))
        c6 = p.connect()
        self.assert_(status(p) == (3,0,3,6))
        if useclose:
            c4.close()
            c3.close()
            c2.close()
        else:
            c4 = c3 = c2 = None
        self.assert_(status(p) == (3,3,3,3))
        if useclose:
            c1.close()
            c5.close()
            c6.close()
        else:
            c1 = c5 = c6 = None
        self.assert_(status(p) == (3,3,0,0))
        c1 = p.connect()
        c2 = p.connect()
        self.assert_(status(p) == (3, 1, 0, 2))
        if useclose:
            c2.close()
        else:
            c2 = None
        self.assert_(status(p) == (3, 2, 0, 1))
    
    def test_timeout(self):
        p = pool.QueuePool(creator = lambda: mock_dbapi.connect('foo.db'), pool_size = 3, max_overflow = 0, use_threadlocal = False, echo = False, timeout=2)
        c1 = p.get()
        c2 = p.get()
        c3 = p.get()
        now = time.time()
        try:
            c4 = p.get()
            assert False
        except exceptions.TimeoutError, e:
            assert int(time.time() - now) == 2
        
    def test_mixed_close(self):
        p = pool.QueuePool(creator = lambda: mock_dbapi.connect('foo.db'), pool_size = 3, max_overflow = -1, use_threadlocal = True, echo=True)
        c1 = p.connect()
        c2 = p.connect()
        assert c1 is c2
        c1.close()
        c2 = None
        assert p.checkedout() == 1
        c1 = None
        assert p.checkedout() == 0
    
    def test_trick_the_counter(self):
        """this is a "flaw" in the connection pool; since threadlocal uses a single ConnectionFairy per thread
        with an open/close counter, you can fool the counter into giving you a ConnectionFairy with an
        ambiguous counter.  i.e. its not true reference counting."""
        p = pool.QueuePool(creator = lambda: mock_dbapi.connect('foo.db'), pool_size = 3, max_overflow = -1, use_threadlocal = True, echo=True)
        c1 = p.connect()
        c2 = p.connect()
        assert c1 is c2
        c1.close()
        c2 = p.connect()
        c2.close()
        self.assert_(p.checkedout() != 0)

        c2.close()
        self.assert_(p.checkedout() == 0)

    def test_recycle(self):
        p = pool.QueuePool(creator = lambda: mock_dbapi.connect('foo.db'), pool_size = 1, max_overflow = 0, use_threadlocal = False, echo=True, recycle=3)
        
        c1 = p.connect()
        c_id = id(c1.connection)
        c1.close()
        c2 = p.connect()
        assert id(c2.connection) == c_id
        c2.close()
        time.sleep(3)
        c3= p.connect()
        assert id(c3.connection) != c_id
        
    def testthreadlocal_del(self):
        self._do_testthreadlocal(useclose=False)

    def testthreadlocal_close(self):
        self._do_testthreadlocal(useclose=True)

    def _do_testthreadlocal(self, useclose=False):
        for p in (
            pool.QueuePool(creator = lambda: mock_dbapi.connect('foo.db'), pool_size = 3, max_overflow = -1, use_threadlocal = True, echo = True),
            pool.SingletonThreadPool(creator = lambda: mock_dbapi.connect('foo.db'), use_threadlocal = True)
        ):   
            c1 = p.connect()
            c2 = p.connect()
            self.assert_(c1 is c2)
            c3 = p.unique_connection()
            self.assert_(c3 is not c1)
            if useclose:
                c2.close()
            else:
                c2 = None
            c2 = p.connect()
            self.assert_(c1 is c2)
            self.assert_(c3 is not c1)
            if useclose:
                c2.close()
            else:
                c2 = None
        
            if useclose:
                c1 = p.connect()
                c2 = p.connect()
                c3 = p.connect()
                c3.close()
                c2.close()
                self.assert_(c1.connection is not None)
                c1.close()

            c1 = c2 = c3 = None
            
            # extra tests with QueuePool to insure connections get __del__()ed when dereferenced
            if isinstance(p, pool.QueuePool):
                self.assert_(p.checkedout() == 0)
                c1 = p.connect()
                c2 = p.connect()
                if useclose:
                    c2.close()
                    c1.close()
                else:
                    c2 = None
                    c1 = None
                self.assert_(p.checkedout() == 0)
            
    def tearDown(self):
       pool.clear_managers()
        
        
if __name__ == "__main__":
    unittest.main()        
