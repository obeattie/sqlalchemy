import testenv; testenv.configure_for_tests()
import sys, time, threading
from testlib.sa import create_engine, MetaData, Table, Column, INT, VARCHAR, \
     Sequence, select, Integer, String, func, text
from testlib import TestBase, testing


users, metadata = None, None
class TransactionTest(TestBase):
    def setUpAll(self):
        global users, metadata
        metadata = MetaData()
        users = Table('query_users', metadata,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20)),
            test_needs_acid=True,
        )
        users.create(testing.db)

    def tearDown(self):
        testing.db.connect().execute(users.delete())
    def tearDownAll(self):
        users.drop(testing.db)

    def test_commits(self):
        connection = testing.db.connect()
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

    def test_rollback(self):
        """test a basic rollback"""
        connection = testing.db.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        connection.execute(users.insert(), user_id=2, user_name='user2')
        connection.execute(users.insert(), user_id=3, user_name='user3')
        transaction.rollback()

        result = connection.execute("select * from query_users")
        assert len(result.fetchall()) == 0
        connection.close()

    def test_raise(self):
        connection = testing.db.connect()

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

    def test_nested_rollback(self):
        connection = testing.db.connect()

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


    def test_nesting(self):
        connection = testing.db.connect()
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

    def test_close(self):
        connection = testing.db.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        connection.execute(users.insert(), user_id=2, user_name='user2')
        connection.execute(users.insert(), user_id=3, user_name='user3')
        trans2 = connection.begin()
        connection.execute(users.insert(), user_id=4, user_name='user4')
        connection.execute(users.insert(), user_id=5, user_name='user5')
        assert connection.in_transaction()
        trans2.close()
        assert connection.in_transaction()
        transaction.commit()
        assert not connection.in_transaction()
        self.assert_(connection.scalar("select count(1) from query_users") == 5)

        result = connection.execute("select * from query_users")
        assert len(result.fetchall()) == 5
        connection.close()

    def test_close2(self):
        connection = testing.db.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        connection.execute(users.insert(), user_id=2, user_name='user2')
        connection.execute(users.insert(), user_id=3, user_name='user3')
        trans2 = connection.begin()
        connection.execute(users.insert(), user_id=4, user_name='user4')
        connection.execute(users.insert(), user_id=5, user_name='user5')
        assert connection.in_transaction()
        trans2.close()
        assert connection.in_transaction()
        transaction.close()
        assert not connection.in_transaction()
        self.assert_(connection.scalar("select count(1) from query_users") == 0)

        result = connection.execute("select * from query_users")
        assert len(result.fetchall()) == 0
        connection.close()

    @testing.requires.savepoints
    def test_nested_subtransaction_rollback(self):
        connection = testing.db.connect()
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

    @testing.requires.savepoints
    def test_nested_subtransaction_commit(self):
        connection = testing.db.connect()
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

    @testing.requires.savepoints
    def test_rollback_to_subtransaction(self):
        connection = testing.db.connect()
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

    @testing.requires.two_phase_transactions
    def test_two_phase_transaction(self):
        connection = testing.db.connect()

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

    @testing.requires.two_phase_transactions
    @testing.requires.savepoints
    def test_mixed_two_phase_transaction(self):
        connection = testing.db.connect()

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

    @testing.requires.two_phase_transactions
    @testing.fails_on('mysql')
    def test_two_phase_recover(self):
        # MySQL recovery doesn't currently seem to work correctly
        # Prepared transactions disappear when connections are closed and even
        # when they aren't it doesn't seem possible to use the recovery id.
        connection = testing.db.connect()

        transaction = connection.begin_twophase()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        transaction.prepare()

        connection.close()
        connection2 = testing.db.connect()

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

    @testing.requires.two_phase_transactions
    def test_multiple_two_phase(self):
        conn = testing.db.connect()

        xa = conn.begin_twophase()
        conn.execute(users.insert(), user_id=1, user_name='user1')
        xa.prepare()
        xa.commit()

        xa = conn.begin_twophase()
        conn.execute(users.insert(), user_id=2, user_name='user2')
        xa.prepare()
        xa.rollback()

        xa = conn.begin_twophase()
        conn.execute(users.insert(), user_id=3, user_name='user3')
        xa.rollback()

        xa = conn.begin_twophase()
        conn.execute(users.insert(), user_id=4, user_name='user4')
        xa.prepare()
        xa.commit()

        result = conn.execute(select([users.c.user_name]).order_by(users.c.user_id))
        self.assertEqual(result.fetchall(), [('user1',),('user4',)])

        conn.close()

class AutoRollbackTest(TestBase):
    def setUpAll(self):
        global metadata
        metadata = MetaData()

    def tearDownAll(self):
        metadata.drop_all(testing.db)

    def test_rollback_deadlock(self):
        """test that returning connections to the pool clears any object locks."""
        conn1 = testing.db.connect()
        conn2 = testing.db.connect()
        users = Table('deadlock_users', metadata,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20)),
            test_needs_acid=True,
        )
        users.create(conn1)
        conn1.execute("select * from deadlock_users")
        conn1.close()

        # without auto-rollback in the connection pool's return() logic, this
        # deadlocks in Postgres, because conn1 is returned to the pool but
        # still has a lock on "deadlock_users".
        # comment out the rollback in pool/ConnectionFairy._close() to see !
        users.drop(conn2)
        conn2.close()

foo = None
class ExplicitAutoCommitTest(TestBase):
    """test the 'autocommit' flag on select() and text() objects.

    Requires Postgres so that we may define a custom function which modifies the database.
    """

    __only_on__ = 'postgres'

    def setUpAll(self):
        global metadata, foo
        metadata = MetaData(testing.db)
        foo = Table('foo', metadata, Column('id', Integer, primary_key=True), Column('data', String(100)))
        metadata.create_all()
        testing.db.execute("create function insert_foo(varchar) returns integer as 'insert into foo(data) values ($1);select 1;' language sql")

    def tearDown(self):
        foo.delete().execute()

    def tearDownAll(self):
        testing.db.execute("drop function insert_foo(varchar)")
        metadata.drop_all()

    def test_control(self):
        # test that not using autocommit does not commit
        conn1 = testing.db.connect()
        conn2 = testing.db.connect()

        conn1.execute(select([func.insert_foo('data1')]))
        assert conn2.execute(select([foo.c.data])).fetchall() == []

        conn1.execute(text("select insert_foo('moredata')"))
        assert conn2.execute(select([foo.c.data])).fetchall() == []

        trans = conn1.begin()
        trans.commit()

        assert conn2.execute(select([foo.c.data])).fetchall() == [('data1',), ('moredata',)]

        conn1.close()
        conn2.close()

    def test_explicit_compiled(self):
        conn1 = testing.db.connect()
        conn2 = testing.db.connect()

        conn1.execute(select([func.insert_foo('data1')], autocommit=True))
        assert conn2.execute(select([foo.c.data])).fetchall() == [('data1',)]

        conn1.execute(select([func.insert_foo('data2')]).autocommit())
        assert conn2.execute(select([foo.c.data])).fetchall() == [('data1',), ('data2',)]

        conn1.close()
        conn2.close()

    def test_explicit_text(self):
        conn1 = testing.db.connect()
        conn2 = testing.db.connect()

        conn1.execute(text("select insert_foo('moredata')", autocommit=True))
        assert conn2.execute(select([foo.c.data])).fetchall() == [('moredata',)]

        conn1.close()
        conn2.close()

    def test_implicit_text(self):
        conn1 = testing.db.connect()
        conn2 = testing.db.connect()

        conn1.execute(text("insert into foo (data) values ('implicitdata')"))
        assert conn2.execute(select([foo.c.data])).fetchall() == [('implicitdata',)]

        conn1.close()
        conn2.close()


tlengine = None
class TLTransactionTest(TestBase):
    def setUpAll(self):
        global users, metadata, tlengine
        tlengine = create_engine(testing.db.url, strategy='threadlocal')
        metadata = MetaData()
        users = Table('query_users', metadata,
            Column('user_id', INT, Sequence('query_users_id_seq', optional=True), primary_key=True),
            Column('user_name', VARCHAR(20)),
            test_needs_acid=True,
        )
        users.create(tlengine)
    def tearDown(self):
        tlengine.execute(users.delete())
    def tearDownAll(self):
        users.drop(tlengine)
        tlengine.dispose()

    def test_connection_close(self):
        """test that when connections are closed for real, transactions are rolled back and disposed."""

        c = tlengine.contextual_connect()
        c.begin()
        assert tlengine.session.in_transaction()
        assert hasattr(tlengine.session, '_TLSession__transaction')
        assert hasattr(tlengine.session, '_TLSession__trans')
        c.close()
        assert not tlengine.session.in_transaction()
        assert not hasattr(tlengine.session, '_TLSession__transaction')
        assert not hasattr(tlengine.session, '_TLSession__trans')

    def test_transaction_close(self):
        c = tlengine.contextual_connect()
        t = c.begin()
        tlengine.execute(users.insert(), user_id=1, user_name='user1')
        tlengine.execute(users.insert(), user_id=2, user_name='user2')
        t2 = c.begin()
        tlengine.execute(users.insert(), user_id=3, user_name='user3')
        tlengine.execute(users.insert(), user_id=4, user_name='user4')
        t2.close()

        result = c.execute("select * from query_users")
        assert len(result.fetchall()) == 4

        t.close()

        external_connection = tlengine.connect()
        result = external_connection.execute("select * from query_users")
        try:
            assert len(result.fetchall()) == 0
        finally:
            external_connection.close()

    def test_rollback(self):
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

    def test_commit(self):
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

    def test_commits(self):
        assert tlengine.connect().execute("select count(1) from query_users").scalar() == 0

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
        l = result.fetchall()
        assert len(l) == 3, "expected 3 got %d" % len(l)
        transaction.commit()

    def test_rollback_off_conn(self):
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

    def test_morerollback_off_conn(self):
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

    def test_commit_off_connection(self):
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

    def test_nesting(self):
        """tests nesting of transactions"""
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

    def test_mixed_nesting(self):
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

    def test_more_mixed_nesting(self):
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

    def test_connections(self):
        """tests that contextual_connect is threadlocal"""
        c1 = tlengine.contextual_connect()
        c2 = tlengine.contextual_connect()
        assert c1.connection is c2.connection
        c2.close()
        assert c1.connection.connection is not None

    @testing.requires.two_phase_transactions
    def test_two_phase_transaction(self):
        tlengine.begin_twophase()
        tlengine.execute(users.insert(), user_id=1, user_name='user1')
        tlengine.prepare()
        tlengine.commit()

        tlengine.begin_twophase()
        tlengine.execute(users.insert(), user_id=2, user_name='user2')
        tlengine.commit()

        tlengine.begin_twophase()
        tlengine.execute(users.insert(), user_id=3, user_name='user3')
        tlengine.rollback()

        tlengine.begin_twophase()
        tlengine.execute(users.insert(), user_id=4, user_name='user4')
        tlengine.prepare()
        tlengine.rollback()

        self.assertEquals(
            tlengine.execute(select([users.c.user_id]).order_by(users.c.user_id)).fetchall(),
            [(1,),(2,)]
        )

counters = None
class ForUpdateTest(TestBase):
    def setUpAll(self):
        global counters, metadata
        metadata = MetaData()
        counters = Table('forupdate_counters', metadata,
            Column('counter_id', INT, primary_key = True),
            Column('counter_value', INT),
            test_needs_acid=True,
        )
        counters.create(testing.db)
    def tearDown(self):
        testing.db.connect().execute(counters.delete())
    def tearDownAll(self):
        counters.drop(testing.db)

    def increment(self, count, errors, update_style=True, delay=0.005):
        con = testing.db.connect()
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

    @testing.unsupported('sqlite', 'needs n threads -> 1 :memory: db')
    @testing.unsupported('mssql', 'FIXME: unknown')
    @testing.unsupported('firebird', 'FIXME: unknown')
    @testing.unsupported('sybase', 'FIXME: unknown')
    @testing.unsupported('access', 'FIXME: unknown')
    def test_queued_update(self):
        """Test SELECT FOR UPDATE with concurrent modifications.

        Runs concurrent modifications on a single row in the users table,
        with each mutator trying to increment a value stored in user_name.

        """
        db = testing.db
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
            sys.stdout.write("Failure: %s\n" % e)

        self.assert_(len(errors) == 0)

        sel = counters.select(whereclause=counters.c.counter_id==1)
        final = db.execute(sel).fetchone()
        self.assert_(final['counter_value'] == iterations * thread_count)

    def overlap(self, ids, errors, update_style):
        sel = counters.select(for_update=update_style,
                              whereclause=counters.c.counter_id.in_(ids))
        con = testing.db.connect()
        trans = con.begin()
        try:
            rows = con.execute(sel).fetchall()
            time.sleep(0.25)
            trans.commit()
        except Exception, e:
            trans.rollback()
            errors.append(e)
        con.close()

    def _threaded_overlap(self, thread_count, groups, update_style=True, pool=5):
        db = testing.db
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

    @testing.unsupported('sqlite', 'needs n threads -> 1 memory db')
    @testing.unsupported('mssql', 'FIXME: unknown')
    @testing.unsupported('firebird', 'FIXME: unknown')
    @testing.unsupported('sybase', 'FIXME: unknown')
    @testing.unsupported('access', 'FIXME: unknown')
    def test_queued_select(self):
        """Simple SELECT FOR UPDATE conflict test"""

        errors = self._threaded_overlap(2, [(1,2,3),(3,4,5)])
        for e in errors:
            sys.stderr.write("Failure: %s\n" % e)
        self.assert_(len(errors) == 0)

    @testing.unsupported('sqlite', 'needs n threads -> 1 memory db')
    @testing.unsupported('mssql', 'FIXME: unknown')
    @testing.unsupported('mysql', 'no support for NOWAIT')
    @testing.unsupported('firebird', 'FIXME: unknown')
    @testing.unsupported('sybase', 'FIXME: unknown')
    @testing.unsupported('access', 'FIXME: unknown')
    def test_nowait_select(self):
        """Simple SELECT FOR UPDATE NOWAIT conflict test"""

        errors = self._threaded_overlap(2, [(1,2,3),(3,4,5)],
                                        update_style='nowait')
        self.assert_(len(errors) != 0)


if __name__ == "__main__":
    testenv.main()
