import testbase
import sys, time, threading

from sqlalchemy import *
from sqlalchemy.orm import *
from testlib import *


class TransactionTest(PersistTest):
    def setUpAll(self):
        global users, metadata
        metadata = MetaData()
        users = Table('query_users', metadata,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20)),
            test_needs_acid=True,
        )
        users.create(testbase.db)
    
    def tearDown(self):
        testbase.db.connect().execute(users.delete())
    def tearDownAll(self):
        users.drop(testbase.db)
    
    def testcommits(self):
        connection = testbase.db.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        transaction.commit()

        transaction = connection.begin()
        connection.execute(users.insert(), user_id=2, user_name='user2')
        connection.execute(users.insert(), user_id=3, user_name='user3')
        transaction.commit()

        transaction = connection.begin()
        result = connection.execute("select * from query_users")
        assert len(result.fetchall()) == 3
        transaction.commit()
        
    def testrollback(self):
        """test a basic rollback"""
        connection = testbase.db.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        connection.execute(users.insert(), user_id=2, user_name='user2')
        connection.execute(users.insert(), user_id=3, user_name='user3')
        transaction.rollback()
        
        result = connection.execute("select * from query_users")
        assert len(result.fetchall()) == 0
        connection.close()

    def testraise(self):
        connection = testbase.db.connect()
        
        transaction = connection.begin()
        try:
            connection.execute(users.insert(), user_id=1, user_name='user1')
            connection.execute(users.insert(), user_id=2, user_name='user2')
            connection.execute(users.insert(), user_id=1, user_name='user3')
            transaction.commit()
            assert False
        except Exception , e:
            print "Exception: ", e
            transaction.rollback()
        
        result = connection.execute("select * from query_users")
        assert len(result.fetchall()) == 0
        connection.close()
        
    def testnestedrollback(self):
        connection = testbase.db.connect()
        
        try:
            transaction = connection.begin()
            try:
                connection.execute(users.insert(), user_id=1, user_name='user1')
                connection.execute(users.insert(), user_id=2, user_name='user2')
                connection.execute(users.insert(), user_id=3, user_name='user3')
                trans2 = connection.begin()
                try:
                    connection.execute(users.insert(), user_id=4, user_name='user4')
                    connection.execute(users.insert(), user_id=5, user_name='user5')
                    raise Exception("uh oh")
                    trans2.commit()
                except:
                    trans2.rollback()
                    raise
                transaction.rollback()
            except Exception, e:
                transaction.rollback()
                raise
        except Exception, e:
            try:
                assert str(e) == 'uh oh'  # and not "This transaction is inactive"
            finally:
                connection.close()
            

    def testnesting(self):
        connection = testbase.db.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        connection.execute(users.insert(), user_id=2, user_name='user2')
        connection.execute(users.insert(), user_id=3, user_name='user3')
        trans2 = connection.begin()
        connection.execute(users.insert(), user_id=4, user_name='user4')
        connection.execute(users.insert(), user_id=5, user_name='user5')
        trans2.commit()
        transaction.rollback()
        self.assert_(connection.scalar("select count(1) from query_users") == 0)

        result = connection.execute("select * from query_users")
        assert len(result.fetchall()) == 0
        connection.close()
    
    @testing.unsupported('sqlite')
    def testnestedsubtransactionrollback(self):
        connection = testbase.db.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        trans2 = connection.begin_nested()
        connection.execute(users.insert(), user_id=2, user_name='user2')
        trans2.rollback()
        connection.execute(users.insert(), user_id=3, user_name='user3')
        transaction.commit()
        
        self.assertEquals(
            connection.execute(select([users.c.user_id]).order_by(users.c.user_id)).fetchall(),
            [(1,),(3,)]
        )
        connection.close()

    @testing.unsupported('sqlite')
    def testnestedsubtransactioncommit(self):
        connection = testbase.db.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        trans2 = connection.begin_nested()
        connection.execute(users.insert(), user_id=2, user_name='user2')
        trans2.commit()
        connection.execute(users.insert(), user_id=3, user_name='user3')
        transaction.commit()
        
        self.assertEquals(
            connection.execute(select([users.c.user_id]).order_by(users.c.user_id)).fetchall(),
            [(1,),(2,),(3,)]
        )
        connection.close()

    @testing.unsupported('sqlite')
    def testrollbacktosubtransaction(self):
        connection = testbase.db.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        trans2 = connection.begin_nested()
        connection.execute(users.insert(), user_id=2, user_name='user2')
        trans3 = connection.begin()
        connection.execute(users.insert(), user_id=3, user_name='user3')
        trans3.rollback()
        connection.execute(users.insert(), user_id=4, user_name='user4')
        transaction.commit()
        
        self.assertEquals(
            connection.execute(select([users.c.user_id]).order_by(users.c.user_id)).fetchall(),
            [(1,),(4,)]
        )
        connection.close()
    
    @testing.supported('postgres', 'mysql')
    def testtwophasetransaction(self):
        connection = testbase.db.connect()
        
        transaction = connection.begin_twophase()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        transaction.prepare()
        transaction.commit()
        
        transaction = connection.begin_twophase()
        connection.execute(users.insert(), user_id=2, user_name='user2')
        transaction.commit()
        
        transaction = connection.begin_twophase()
        connection.execute(users.insert(), user_id=3, user_name='user3')
        transaction.rollback()
        
        transaction = connection.begin_twophase()
        connection.execute(users.insert(), user_id=4, user_name='user4')
        transaction.prepare()
        transaction.rollback()
        
        self.assertEquals(
            connection.execute(select([users.c.user_id]).order_by(users.c.user_id)).fetchall(),
            [(1,),(2,)]
        )
        connection.close()

    @testing.supported('postgres', 'mysql')
    def testmixedtransaction(self):
        connection = testbase.db.connect()
        
        transaction = connection.begin_twophase()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        
        transaction2 = connection.begin()
        connection.execute(users.insert(), user_id=2, user_name='user2')
        
        transaction3 = connection.begin_nested()
        connection.execute(users.insert(), user_id=3, user_name='user3')
        
        transaction4 = connection.begin()
        connection.execute(users.insert(), user_id=4, user_name='user4')
        transaction4.commit()
        
        transaction3.rollback()
        
        connection.execute(users.insert(), user_id=5, user_name='user5')
        
        transaction2.commit()
        
        transaction.prepare()
        
        transaction.commit()
        
        self.assertEquals(
            connection.execute(select([users.c.user_id]).order_by(users.c.user_id)).fetchall(),
            [(1,),(2,),(5,)]
        )
        connection.close()
        
    @testing.supported('postgres')
    def testtwophaserecover(self):
        # MySQL recovery doesn't currently seem to work correctly
        # Prepared transactions disappear when connections are closed and even
        # when they aren't it doesn't seem possible to use the recovery id.
        connection = testbase.db.connect()
        
        transaction = connection.begin_twophase()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        transaction.prepare()
        
        connection.close()
        connection2 = testbase.db.connect()
        
        self.assertEquals(
            connection2.execute(select([users.c.user_id]).order_by(users.c.user_id)).fetchall(),
            []
        )
        
        recoverables = connection2.recover_twophase()
        self.assertTrue(
            transaction.xid in recoverables
        )
        
        connection2.commit_prepared(transaction.xid, recover=True)

        self.assertEquals(
            connection2.execute(select([users.c.user_id]).order_by(users.c.user_id)).fetchall(),
            [(1,)]
        )
        connection2.close()

class AutoRollbackTest(PersistTest):
    def setUpAll(self):
        global metadata
        metadata = MetaData()
    
    def tearDownAll(self):
        metadata.drop_all(testbase.db)
        
    @testing.unsupported('sqlite')
    def testrollback_deadlock(self):
        """test that returning connections to the pool clears any object locks."""
        conn1 = testbase.db.connect()
        conn2 = testbase.db.connect()
        users = Table('deadlock_users', metadata,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20)),
            test_needs_acid=True,
        )
        users.create(conn1)
        conn1.execute("select * from deadlock_users")
        conn1.close()
        # without auto-rollback in the connection pool's return() logic, this deadlocks in Postgres, 
        # because conn1 is returned to the pool but still has a lock on "deadlock_users"
        # comment out the rollback in pool/ConnectionFairy._close() to see !
        users.drop(conn2)
        conn2.close()

class TLTransactionTest(PersistTest):
    def setUpAll(self):
        global users, metadata, tlengine
        tlengine = create_engine(testbase.db.url, strategy='threadlocal')
        metadata = MetaData()
        users = Table('query_users', metadata,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20)),
            test_needs_acid=True,
        )
        users.create(tlengine)
    def tearDown(self):
        tlengine.execute(users.delete())
    def tearDownAll(self):
        users.drop(tlengine)
        tlengine.dispose()
        
    def testrollback(self):
        """test a basic rollback"""
        tlengine.begin()
        tlengine.execute(users.insert(), user_id=1, user_name='user1')
        tlengine.execute(users.insert(), user_id=2, user_name='user2')
        tlengine.execute(users.insert(), user_id=3, user_name='user3')
        tlengine.rollback()

        external_connection = tlengine.connect()
        result = external_connection.execute("select * from query_users")
        try:
            assert len(result.fetchall()) == 0
        finally:
            external_connection.close()

    def testcommit(self):
        """test a basic commit"""
        tlengine.begin()
        tlengine.execute(users.insert(), user_id=1, user_name='user1')
        tlengine.execute(users.insert(), user_id=2, user_name='user2')
        tlengine.execute(users.insert(), user_id=3, user_name='user3')
        tlengine.commit()

        external_connection = tlengine.connect()
        result = external_connection.execute("select * from query_users")
        try:
            assert len(result.fetchall()) == 3
        finally:
            external_connection.close()

    def testcommits(self):
        connection = tlengine.contextual_connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        transaction.commit()

        transaction = connection.begin()
        connection.execute(users.insert(), user_id=2, user_name='user2')
        connection.execute(users.insert(), user_id=3, user_name='user3')
        transaction.commit()

        transaction = connection.begin()
        result = connection.execute("select * from query_users")
        assert len(result.fetchall()) == 3
        transaction.commit()

    def testrollback_off_conn(self):
        # test that a TLTransaction opened off a TLConnection allows that
        # TLConnection to be aware of the transactional context
        conn = tlengine.contextual_connect()
        trans = conn.begin()
        conn.execute(users.insert(), user_id=1, user_name='user1')
        conn.execute(users.insert(), user_id=2, user_name='user2')
        conn.execute(users.insert(), user_id=3, user_name='user3')
        trans.rollback()

        external_connection = tlengine.connect()
        result = external_connection.execute("select * from query_users")
        try:
            assert len(result.fetchall()) == 0
        finally:
            external_connection.close()

    def testmorerollback_off_conn(self):
        # test that an existing TLConnection automatically takes place in a TLTransaction
        # opened on a second TLConnection
        conn = tlengine.contextual_connect()
        conn2 = tlengine.contextual_connect()
        trans = conn2.begin()
        conn.execute(users.insert(), user_id=1, user_name='user1')
        conn.execute(users.insert(), user_id=2, user_name='user2')
        conn.execute(users.insert(), user_id=3, user_name='user3')
        trans.rollback()

        external_connection = tlengine.connect()
        result = external_connection.execute("select * from query_users")
        try:
            assert len(result.fetchall()) == 0
        finally:
            external_connection.close()

    def testcommit_off_conn(self):
        conn = tlengine.contextual_connect()
        trans = conn.begin()
        conn.execute(users.insert(), user_id=1, user_name='user1')
        conn.execute(users.insert(), user_id=2, user_name='user2')
        conn.execute(users.insert(), user_id=3, user_name='user3')
        trans.commit()

        external_connection = tlengine.connect()
        result = external_connection.execute("select * from query_users")
        try:
            assert len(result.fetchall()) == 3
        finally:
            external_connection.close()
        
    @testing.unsupported('sqlite')
    def testnesting(self):
        """tests nesting of tranacstions"""
        external_connection = tlengine.connect()
        self.assert_(external_connection.connection is not tlengine.contextual_connect().connection)
        tlengine.begin()
        tlengine.execute(users.insert(), user_id=1, user_name='user1')
        tlengine.execute(users.insert(), user_id=2, user_name='user2')
        tlengine.execute(users.insert(), user_id=3, user_name='user3')
        tlengine.begin()
        tlengine.execute(users.insert(), user_id=4, user_name='user4')
        tlengine.execute(users.insert(), user_id=5, user_name='user5')
        tlengine.commit()
        tlengine.rollback()
        try:
            self.assert_(external_connection.scalar("select count(1) from query_users") == 0)
        finally:
            external_connection.close()

    def testmixednesting(self):
        """tests nesting of transactions off the TLEngine directly inside of 
        tranasctions off the connection from the TLEngine"""
        external_connection = tlengine.connect()
        self.assert_(external_connection.connection is not tlengine.contextual_connect().connection)
        conn = tlengine.contextual_connect()
        trans = conn.begin()
        trans2 = conn.begin()
        tlengine.execute(users.insert(), user_id=1, user_name='user1')
        tlengine.execute(users.insert(), user_id=2, user_name='user2')
        tlengine.execute(users.insert(), user_id=3, user_name='user3')
        tlengine.begin()
        tlengine.execute(users.insert(), user_id=4, user_name='user4')
        tlengine.begin()
        tlengine.execute(users.insert(), user_id=5, user_name='user5')
        tlengine.execute(users.insert(), user_id=6, user_name='user6')
        tlengine.execute(users.insert(), user_id=7, user_name='user7')
        tlengine.commit()
        tlengine.execute(users.insert(), user_id=8, user_name='user8')
        tlengine.commit()
        trans2.commit()
        trans.rollback()
        conn.close()
        try:
            self.assert_(external_connection.scalar("select count(1) from query_users") == 0)
        finally:
            external_connection.close()

    def testmoremixednesting(self):
        """tests nesting of transactions off the connection from the TLEngine
        inside of tranasctions off thbe TLEngine directly."""
        external_connection = tlengine.connect()
        self.assert_(external_connection.connection is not tlengine.contextual_connect().connection)
        tlengine.begin()
        connection = tlengine.contextual_connect()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        tlengine.begin()
        connection.execute(users.insert(), user_id=2, user_name='user2')
        connection.execute(users.insert(), user_id=3, user_name='user3')
        trans = connection.begin()
        connection.execute(users.insert(), user_id=4, user_name='user4')
        connection.execute(users.insert(), user_id=5, user_name='user5')
        trans.commit()
        tlengine.commit()
        tlengine.rollback()
        connection.close()
        try:
            self.assert_(external_connection.scalar("select count(1) from query_users") == 0)
        finally:
            external_connection.close()

    def testsessionnesting(self):
        class User(object):
            pass
        try:
            mapper(User, users)

            sess = create_session(bind=tlengine)
            tlengine.begin()
            u = User()
            sess.save(u)
            sess.flush()
            tlengine.commit()
        finally:
            clear_mappers()

            
    def testconnections(self):
        """tests that contextual_connect is threadlocal"""
        c1 = tlengine.contextual_connect()
        c2 = tlengine.contextual_connect()
        assert c1.connection is c2.connection
        c2.close()
        assert c1.connection.connection is not None

class ForUpdateTest(PersistTest):
    def setUpAll(self):
        global counters, metadata
        metadata = MetaData()
        counters = Table('forupdate_counters', metadata,
            Column('counter_id', INT, primary_key = True),
            Column('counter_value', INT),
            test_needs_acid=True,
        )
        counters.create(testbase.db)
    def tearDown(self):
        testbase.db.connect().execute(counters.delete())
    def tearDownAll(self):
        counters.drop(testbase.db)

    def increment(self, count, errors, update_style=True, delay=0.005):
        con = testbase.db.connect()
        sel = counters.select(for_update=update_style,
                              whereclause=counters.c.counter_id==1)
        
        for i in xrange(count):
            trans = con.begin()
            try:
                existing = con.execute(sel).fetchone()
                incr = existing['counter_value'] + 1

                time.sleep(delay)
                con.execute(counters.update(counters.c.counter_id==1,
                                            values={'counter_value':incr}))
                time.sleep(delay)

                readback = con.execute(sel).fetchone()
                if (readback['counter_value'] != incr):
                    raise AssertionError("Got %s post-update, expected %s" %
                                         (readback['counter_value'], incr))
                trans.commit()
            except Exception, e:
                trans.rollback()
                errors.append(e)
                break

        con.close()

    @testing.supported('mysql', 'oracle', 'postgres')
    def testqueued_update(self):
        """Test SELECT FOR UPDATE with concurrent modifications.

        Runs concurrent modifications on a single row in the users table,
        with each mutator trying to increment a value stored in user_name.
        """

        db = testbase.db
        db.execute(counters.insert(), counter_id=1, counter_value=0)

        iterations, thread_count = 10, 5
        threads, errors = [], []
        for i in xrange(thread_count):
            thread = threading.Thread(target=self.increment,
                                      args=(iterations,),
                                      kwargs={'errors': errors,
                                              'update_style': True})
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

        for e in errors:
            sys.stderr.write("Failure: %s\n" % e)

        self.assert_(len(errors) == 0)

        sel = counters.select(whereclause=counters.c.counter_id==1)
        final = db.execute(sel).fetchone()
        self.assert_(final['counter_value'] == iterations * thread_count)

    def overlap(self, ids, errors, update_style):
        sel = counters.select(for_update=update_style,
                              whereclause=counters.c.counter_id.in_(*ids))
        con = testbase.db.connect()
        trans = con.begin()
        try:
            rows = con.execute(sel).fetchall()
            time.sleep(0.25)
            trans.commit()
        except Exception, e:
            trans.rollback()
            errors.append(e)

    def _threaded_overlap(self, thread_count, groups, update_style=True, pool=5):
        db = testbase.db
        for cid in range(pool - 1):
            db.execute(counters.insert(), counter_id=cid + 1, counter_value=0)

        errors, threads = [], []
        for i in xrange(thread_count):
            thread = threading.Thread(target=self.overlap,
                                      args=(groups.pop(0), errors, update_style))
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

        return errors
        
    @testing.supported('mysql', 'oracle', 'postgres')
    def testqueued_select(self):
        """Simple SELECT FOR UPDATE conflict test"""

        errors = self._threaded_overlap(2, [(1,2,3),(3,4,5)])
        for e in errors:
            sys.stderr.write("Failure: %s\n" % e)
        self.assert_(len(errors) == 0)

    @testing.supported('oracle', 'postgres')
    def testnowait_select(self):
        """Simple SELECT FOR UPDATE NOWAIT conflict test"""

        errors = self._threaded_overlap(2, [(1,2,3),(3,4,5)],
                                        update_style='nowait')
        self.assert_(len(errors) != 0)
        
if __name__ == "__main__":
    testbase.main()        
