import testbase
from sqlalchemy import *
import sqlalchemy.util as util
import sqlalchemy.schema as schema
from sqlalchemy.orm import mapper, create_session
from testlib import *
import datetime

class DefaultTest(PersistTest):

    def setUpAll(self):
        global t, f, f2, ts, currenttime, metadata

        db = testbase.db
        metadata = MetaData(db)
        x = {'x':50}
        def mydefault():
            x['x'] += 1
            return x['x']

        def myupdate_with_ctx(ctx):
            return len(ctx.compiled_parameters['col2'])
        
        def mydefault_using_connection(ctx):
            conn = ctx.connection
            try:
                if db.engine.name == 'oracle':
                    return conn.execute("select 12 from dual").scalar()
                else:
                    return conn.execute("select 12").scalar()
            finally:
                # ensure a "close()" on this connection does nothing,
                # since its a "branched" connection
                conn.close()
            
        use_function_defaults = db.engine.name == 'postgres' or db.engine.name == 'oracle'
        is_oracle = db.engine.name == 'oracle'
 
        # select "count(1)" returns different results on different DBs
        # also correct for "current_date" compatible as column default, value differences
        currenttime = func.current_date(type_=Date, bind=db);
        if is_oracle:
            ts = db.func.trunc(func.sysdate(), literal_column("'DAY'")).scalar()
            f = select([func.count(1) + 5], bind=db).scalar()
            f2 = select([func.count(1) + 14], bind=db).scalar()
            # TODO: engine propigation across nested functions not working
            currenttime = func.trunc(currenttime, literal_column("'DAY'"), bind=db)
            def1 = currenttime
            def2 = func.trunc(text("sysdate"), literal_column("'DAY'"))
            deftype = Date
        elif use_function_defaults:
            f = select([func.count(1) + 5], bind=db).scalar()
            f2 = select([func.count(1) + 14], bind=db).scalar()
            def1 = currenttime
            def2 = text("current_date")
            deftype = Date
            ts = db.func.current_date().scalar()
        else:
            f = select([func.count(1) + 5], bind=db).scalar()
            f2 = select([func.count(1) + 14], bind=db).scalar()
            def1 = def2 = "3"
            ts = 3
            deftype = Integer
            
        t = Table('default_test1', metadata,
            # python function
            Column('col1', Integer, primary_key=True, default=mydefault),
            
            # python literal
            Column('col2', String(20), default="imthedefault", onupdate="im the update"),
            
            # preexecute expression
            Column('col3', Integer, default=func.count(1) + 5, onupdate=func.count(1) + 14),
            
            # SQL-side default from sql expression
            Column('col4', deftype, PassiveDefault(def1)),
            
            # SQL-side default from literal expression
            Column('col5', deftype, PassiveDefault(def2)),
            
            # preexecute + update timestamp
            Column('col6', Date, default=currenttime, onupdate=currenttime),
            
            Column('boolcol1', Boolean, default=True),
            Column('boolcol2', Boolean, default=False),
            
            # python function which uses ExecutionContext
            Column('col7', Integer, default=mydefault_using_connection, onupdate=myupdate_with_ctx),
            
            # python builtin
            Column('col8', Date, default=datetime.date.today, onupdate=datetime.date.today)
        )
        t.create()

    def tearDownAll(self):
        t.drop()
    
    def tearDown(self):
        t.delete().execute()
    
    def testargsignature(self):
        def mydefault(x, y):
            pass
        try:
            c = ColumnDefault(mydefault)
            assert False
        except exceptions.ArgumentError, e:
            assert str(e) == "ColumnDefault Python function takes zero or one positional arguments", str(e)
            
    def teststandalone(self):
        c = testbase.db.engine.contextual_connect()
        x = c.execute(t.c.col1.default)
        y = t.c.col2.default.execute()
        z = c.execute(t.c.col3.default)
        self.assert_(50 <= x <= 57)
        self.assert_(y == 'imthedefault')
        self.assert_(z == f)
        # mysql/other db's return 0 or 1 for count(1)
        self.assert_(5 <= z <= 6)
        
    def testinsert(self):
        r = t.insert().execute()
        self.assert_(r.lastrow_has_defaults())
        t.insert().execute()
        t.insert().execute()

        ctexec = currenttime.scalar()
        print "Currenttime "+ repr(ctexec)
        l = t.select().execute()
        today = datetime.date.today()
        self.assert_(l.fetchall() == [(51, 'imthedefault', f, ts, ts, ctexec, True, False, 12, today), (52, 'imthedefault', f, ts, ts, ctexec, True, False, 12, today), (53, 'imthedefault', f, ts, ts, ctexec, True, False, 12, today)])

    def testinsertvalues(self):
        t.insert(values={'col3':50}).execute()
        l = t.select().execute()
        self.assert_(l.fetchone()['col3'] == 50)
        
        
    def testupdate(self):
        r = t.insert().execute()
        pk = r.last_inserted_ids()[0]
        t.update(t.c.col1==pk).execute(col4=None, col5=None)
        ctexec = currenttime.scalar()
        print "Currenttime "+ repr(ctexec)
        l = t.select(t.c.col1==pk).execute()
        l = l.fetchone()
        self.assert_(l == (pk, 'im the update', f2, None, None, ctexec, True, False, 13, datetime.date.today()))
        # mysql/other db's return 0 or 1 for count(1)
        self.assert_(14 <= f2 <= 15)

    def testupdatevalues(self):
        r = t.insert().execute()
        pk = r.last_inserted_ids()[0]
        t.update(t.c.col1==pk, values={'col3': 55}).execute()
        l = t.select(t.c.col1==pk).execute()
        l = l.fetchone()
        self.assert_(l['col3'] == 55)

    @testing.supported('postgres')
    def testpassiveoverride(self):
        """primarily for postgres, tests that when we get a primary key column back 
        from reflecting a table which has a default value on it, we pre-execute
        that PassiveDefault upon insert, even though PassiveDefault says 
        "let the database execute this", because in postgres we must have all the primary
        key values in memory before insert; otherwise we cant locate the just inserted row."""

        try:
            meta = MetaData(testbase.db)
            testbase.db.execute("""
             CREATE TABLE speedy_users
             (
                 speedy_user_id   SERIAL     PRIMARY KEY,

                 user_name        VARCHAR    NOT NULL,
                 user_password    VARCHAR    NOT NULL
             );
            """, None)

            t = Table("speedy_users", meta, autoload=True)
            t.insert().execute(user_name='user', user_password='lala')
            l = t.select().execute().fetchall()
            self.assert_(l == [(1, 'user', 'lala')])
        finally:
            testbase.db.execute("drop table speedy_users", None)

class AutoIncrementTest(PersistTest):
    @testing.supported('postgres', 'mysql')
    def testnonautoincrement(self):
        meta = MetaData(testbase.db)
        nonai_table = Table("aitest", meta, 
            Column('id', Integer, autoincrement=False, primary_key=True),
            Column('data', String(20)))
        nonai_table.create(checkfirst=True)
        try:
            try:
                # postgres will fail on first row, mysql fails on second row
                nonai_table.insert().execute(data='row 1')
                nonai_table.insert().execute(data='row 2')
                assert False
            except exceptions.SQLError, e:
                print "Got exception", str(e)
                assert True
                
            nonai_table.insert().execute(id=1, data='row 1')
        finally:
            nonai_table.drop()    

    def testwithautoincrement(self):
        meta = MetaData(testbase.db)
        table = Table("aitest", meta, 
            Column('id', Integer, primary_key=True),
            Column('data', String(20)))
        table.create(checkfirst=True)
        try:
            table.insert().execute(data='row 1')
            table.insert().execute(data='row 2')
        finally:
            table.drop()    

    def testfetchid(self):
        
        # TODO: what does this test do that all the various ORM tests dont ?
        
        meta = MetaData(testbase.db)
        table = Table("aitest", meta, 
            Column('id', Integer, primary_key=True),
            Column('data', String(20)))
        table.create(checkfirst=True)

        try:
            # simulate working on a table that doesn't already exist
            meta2 = MetaData(testbase.db)
            table2 = Table("aitest", meta2,
                Column('id', Integer, primary_key=True),
                Column('data', String(20)))
            class AiTest(object):
                pass
            mapper(AiTest, table2)
        
            s = create_session()
            u = AiTest()
            s.save(u)
            s.flush()
            assert u.id is not None
            s.clear()
        finally:
            table.drop()
        

class SequenceTest(PersistTest):
    @testing.supported('postgres', 'oracle')
    def setUpAll(self):
        global cartitems, sometable, metadata
        metadata = MetaData(testbase.db)
        cartitems = Table("cartitems", metadata, 
            Column("cart_id", Integer, Sequence('cart_id_seq'), primary_key=True),
            Column("description", String(40)),
            Column("createdate", DateTime())
        )
        sometable = Table( 'Manager', metadata,
               Column( 'obj_id', Integer, Sequence('obj_id_seq'), ),
               Column( 'name', String, ),
               Column( 'id', Integer, primary_key= True, ),
           )
        
        metadata.create_all()
    
    @testing.supported('postgres', 'oracle')
    def testseqnonpk(self):
        """test sequences fire off as defaults on non-pk columns"""
        sometable.insert().execute(name="somename")
        sometable.insert().execute(name="someother")
        assert sometable.select().execute().fetchall() == [
            (1, "somename", 1),
            (2, "someother", 2),
        ]
        
    @testing.supported('postgres', 'oracle')
    def testsequence(self):
        cartitems.insert().execute(description='hi')
        cartitems.insert().execute(description='there')
        cartitems.insert().execute(description='lala')
        
        cartitems.select().execute().fetchall()
   
   
    @testing.supported('postgres', 'oracle')
    def test_implicit_sequence_exec(self):
        s = Sequence("my_sequence", metadata=MetaData(testbase.db))
        s.create()
        try:
            x = s.execute()
            self.assert_(x == 1)
        finally:
            s.drop()

    @testing.supported('postgres', 'oracle')
    def teststandalone_explicit(self):
        s = Sequence("my_sequence")
        s.create(bind=testbase.db)
        try:
            x = s.execute(testbase.db)
            self.assert_(x == 1)
        finally:
            s.drop(testbase.db)
    
    @testing.supported('postgres', 'oracle')
    def test_checkfirst(self):
        s = Sequence("my_sequence")
        s.create(testbase.db, checkfirst=False)
        s.create(testbase.db, checkfirst=True)
        s.drop(testbase.db, checkfirst=False)
        s.drop(testbase.db, checkfirst=True)
        
    @testing.supported('postgres', 'oracle')
    def teststandalone2(self):
        x = cartitems.c.cart_id.sequence.execute()
        self.assert_(1 <= x <= 4)
        
    @testing.supported('postgres', 'oracle')
    def tearDownAll(self): 
        metadata.drop_all()

if __name__ == "__main__":
    testbase.main()
