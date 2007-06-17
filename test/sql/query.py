from testbase import PersistTest
import testbase
import unittest, sys, datetime

import sqlalchemy.databases.sqlite as sqllite

import tables
from sqlalchemy import *
from sqlalchemy.engine import ResultProxy, RowProxy
from sqlalchemy import exceptions
from testbase import Table, Column


class QueryTest(PersistTest):
    
    def setUpAll(self):
        global users, addresses, metadata
        metadata = BoundMetaData(testbase.db)
        users = Table('query_users', metadata,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20)),
        )
        addresses = Table('query_addresses', metadata, 
            Column('address_id', Integer, primary_key=True),
            Column('user_id', Integer, ForeignKey('query_users.user_id')),
            Column('address', String(30)))
        metadata.create_all()
    
    def tearDown(self):
        users.delete().execute()
    
    def tearDownAll(self):
        metadata.drop_all()
        
    def testinsert(self):
        users.insert().execute(user_id = 7, user_name = 'jack')
        assert users.count().scalar() == 1
    
    def testupdate(self):

        users.insert().execute(user_id = 7, user_name = 'jack')
        assert users.count().scalar() == 1

        users.update(users.c.user_id == 7).execute(user_name = 'fred')
        assert users.select(users.c.user_id==7).execute().fetchone()['user_name'] == 'fred'

    def test_lastrow_accessor(self):
        """test the last_inserted_ids() and lastrow_has_id() functions"""

        def insert_values(table, values):
            """insert a row into a table, return the full list of values INSERTed including defaults
            that fired off on the DB side.  
            
            detects rows that had defaults and post-fetches.
            """
            
            result = table.insert().execute(**values)
            ret = values.copy()

            for col, id in zip(table.primary_key, result.last_inserted_ids()):
                ret[col.key] = id

            if result.lastrow_has_defaults():
                criterion = and_(*[col==id for col, id in zip(table.primary_key, result.last_inserted_ids())])
                row = table.select(criterion).execute().fetchone()
                ret.update(row)
            return ret

        for supported, table, values, assertvalues in [
            (
                {'unsupported':['sqlite']},
                Table("t1", metadata, 
                    Column('id', Integer, primary_key=True),
                    Column('foo', String(30), primary_key=True)),
                {'foo':'hi'},
                {'id':1, 'foo':'hi'}
            ),
            (
                {'unsupported':['sqlite']},
                Table("t2", metadata, 
                    Column('id', Integer, primary_key=True),
                    Column('foo', String(30), primary_key=True),
                    Column('bar', String(30), PassiveDefault('hi'))
                ),
                {'foo':'hi'},
                {'id':1, 'foo':'hi', 'bar':'hi'}
            ),
            (
                {'unsupported':[]},
                Table("t3", metadata, 
                    Column("id", String(40), primary_key=True),
                    Column('foo', String(30), primary_key=True),
                    Column("bar", String(30))
                    ),
                    {'id':'hi', 'foo':'thisisfoo', 'bar':"thisisbar"},
                    {'id':'hi', 'foo':'thisisfoo', 'bar':"thisisbar"}
            ),
            (
                {'unsupported':[]},
                Table("t4", metadata, 
                    Column('id', Integer, primary_key=True),
                    Column('foo', String(30), primary_key=True),
                    Column('bar', String(30), PassiveDefault('hi'))
                ),
                {'foo':'hi', 'id':1},
                {'id':1, 'foo':'hi', 'bar':'hi'}
            ),
            (
                {'unsupported':[]},
                Table("t5", metadata, 
                    Column('id', String(10), primary_key=True),
                    Column('bar', String(30), PassiveDefault('hi'))
                ),
                {'id':'id1'},
                {'id':'id1', 'bar':'hi'},
            ),
        ]:
            if testbase.db.name in supported['unsupported']:
                continue
            try:
                table.create()
                assert insert_values(table, values) == assertvalues, repr(values) + " " + repr(assertvalues)
            finally:
                table.drop()

    def testrowiteration(self):
        users.insert().execute(user_id = 7, user_name = 'jack')
        users.insert().execute(user_id = 8, user_name = 'ed')
        users.insert().execute(user_id = 9, user_name = 'fred')
        r = users.select().execute()
        l = []
        for row in r:
            l.append(row)
        self.assert_(len(l) == 3)

    def test_fetchmany(self):
        users.insert().execute(user_id = 7, user_name = 'jack') 
        users.insert().execute(user_id = 8, user_name = 'ed') 
        users.insert().execute(user_id = 9, user_name = 'fred') 
        r = users.select().execute() 
        l = [] 
        for row in r.fetchmany(size=2): 
            l.append(row) 
        self.assert_(len(l) == 2, "fetchmany(size=2) got %s rows" % len(l))
        
    def test_compiled_execute(self):
        users.insert().execute(user_id = 7, user_name = 'jack') 
        s = select([users], users.c.user_id==bindparam('id')).compile()
        c = testbase.db.connect()
        assert c.execute(s, id=7).fetchall()[0]['user_id'] == 7

    @testbase.supported('postgres')
    def testschema(self):
        meta1 = BoundMetaData(testbase.db)
        test_table = Table('my_table', meta1,
                    Column('id', Integer, primary_key=True),
                    Column('data', String(20), nullable=False),
                    schema='alt_schema'
                 )
        test_table.create()
        try:
            # plain insert
            test_table.insert().execute(data='test')

            meta2 = BoundMetaData(testbase.db)
            test_table = Table('my_table', meta2, autoload=True, schema='alt_schema')
            test_table.insert().execute(data='test')

        finally:
            test_table.drop()

    def test_repeated_bindparams(self):
        """test that a BindParam can be used more than once.  
        this should be run for dbs with both positional and named paramstyles."""
        users.insert().execute(user_id = 7, user_name = 'jack')
        users.insert().execute(user_id = 8, user_name = 'fred')

        u = bindparam('userid')
        s = users.select(or_(users.c.user_name==u, users.c.user_name==u))
        r = s.execute(userid='fred').fetchall()
        assert len(r) == 1
    
    def test_bindparam_shortname(self):
        """test the 'shortname' field on BindParamClause."""
        users.insert().execute(user_id = 7, user_name = 'jack')
        users.insert().execute(user_id = 8, user_name = 'fred')
        u = bindparam('userid', shortname='someshortname')
        s = users.select(users.c.user_name==u)
        r = s.execute(someshortname='fred').fetchall()
        assert len(r) == 1
        
    def testdelete(self):
        users.insert().execute(user_id = 7, user_name = 'jack')
        users.insert().execute(user_id = 8, user_name = 'fred')
        print repr(users.select().execute().fetchall())

        users.delete(users.c.user_name == 'fred').execute()
        
        print repr(users.select().execute().fetchall())
        
    def testselectlimit(self):
        users.insert().execute(user_id=1, user_name='john')
        users.insert().execute(user_id=2, user_name='jack')
        users.insert().execute(user_id=3, user_name='ed')
        users.insert().execute(user_id=4, user_name='wendy')
        users.insert().execute(user_id=5, user_name='laura')
        users.insert().execute(user_id=6, user_name='ralph')
        users.insert().execute(user_id=7, user_name='fido')
        r = users.select(limit=3, order_by=[users.c.user_id]).execute().fetchall()
        self.assert_(r == [(1, 'john'), (2, 'jack'), (3, 'ed')], repr(r))
        
    @testbase.unsupported('mssql')
    def testselectlimitoffset(self):
        users.insert().execute(user_id=1, user_name='john')
        users.insert().execute(user_id=2, user_name='jack')
        users.insert().execute(user_id=3, user_name='ed')
        users.insert().execute(user_id=4, user_name='wendy')
        users.insert().execute(user_id=5, user_name='laura')
        users.insert().execute(user_id=6, user_name='ralph')
        users.insert().execute(user_id=7, user_name='fido')
        r = users.select(limit=3, offset=2, order_by=[users.c.user_id]).execute().fetchall()
        self.assert_(r==[(3, 'ed'), (4, 'wendy'), (5, 'laura')])
        r = users.select(offset=5, order_by=[users.c.user_id]).execute().fetchall()
        self.assert_(r==[(6, 'ralph'), (7, 'fido')])
        
    @testbase.supported('mssql')
    def testselectlimitoffset_mssql(self):
        try:
            r = users.select(limit=3, offset=2, order_by=[users.c.user_id]).execute().fetchall()
            assert False # InvalidRequestError should have been raised
        except exceptions.InvalidRequestError:
            pass

    @testbase.unsupported('mysql')  
    def test_scalar_select(self):
        """test that scalar subqueries with labels get their type propigated to the result set."""
        # mysql and/or mysqldb has a bug here, type isnt propigated for scalar subquery.
        datetable = Table('datetable', metadata, 
            Column('id', Integer, primary_key=True),
            Column('today', DateTime))
        datetable.create()
        try:
            datetable.insert().execute(id=1, today=datetime.datetime(2006, 5, 12, 12, 0, 0))
            s = select([datetable.alias('x').c.today], scalar=True)
            s2 = select([datetable.c.id, s.label('somelabel')])
            #print s2.c.somelabel.type
            assert isinstance(s2.execute().fetchone()['somelabel'], datetime.datetime)
        finally:
            datetable.drop()
            
    def test_column_accessor(self):
        users.insert().execute(user_id=1, user_name='john')
        users.insert().execute(user_id=2, user_name='jack')
        r = users.select(users.c.user_id==2).execute().fetchone()
        self.assert_(r.user_id == r['user_id'] == r[users.c.user_id] == 2)
        self.assert_(r.user_name == r['user_name'] == r[users.c.user_name] == 'jack')

        r = text("select * from query_users where user_id=2", engine=testbase.db).execute().fetchone()
        self.assert_(r.user_id == r['user_id'] == r[users.c.user_id] == 2)
        self.assert_(r.user_name == r['user_name'] == r[users.c.user_name] == 'jack')
        
    def test_keys(self):
        users.insert().execute(user_id=1, user_name='foo')
        r = users.select().execute().fetchone()
        self.assertEqual([x.lower() for x in r.keys()], ['user_id', 'user_name'])

    def test_items(self):
        users.insert().execute(user_id=1, user_name='foo')
        r = users.select().execute().fetchone()
        self.assertEqual([(x[0].lower(), x[1]) for x in r.items()], [('user_id', 1), ('user_name', 'foo')])

    def test_len(self):
        users.insert().execute(user_id=1, user_name='foo')
        r = users.select().execute().fetchone()
        self.assertEqual(len(r), 2)
        r.close()
        r = testbase.db.execute('select user_name, user_id from query_users', {}).fetchone()
        self.assertEqual(len(r), 2)
        r.close()
        r = testbase.db.execute('select user_name from query_users', {}).fetchone()
        self.assertEqual(len(r), 1)
        r.close()
    
    def test_cant_execute_join(self):
        try:
            users.join(addresses).execute()
        except exceptions.ArgumentError, e:
            assert str(e) == """Not an executeable clause: query_users JOIN query_addresses ON query_users.user_id = query_addresses.user_id"""
            
    def test_functions(self):
        x = testbase.db.func.current_date().execute().scalar()
        y = testbase.db.func.current_date().select().execute().scalar()
        z = testbase.db.func.current_date().scalar()
        assert x == y == z

    def test_conn_functions(self):
        conn = testbase.db.connect()
        try:
            x = conn.execute(func.current_date()).scalar()
            y = conn.execute(func.current_date().select()).scalar()
            z = conn.scalar(func.current_date())
        finally:
            conn.close()
        assert x == y == z
        
    def test_update_functions(self):
        """test sending functions and SQL expressions to the VALUES and SET clauses of INSERT/UPDATE instances,
        and that column-level defaults get overridden"""
        meta = BoundMetaData(testbase.db)
        t = Table('t1', meta,
            Column('id', Integer, Sequence('t1idseq', optional=True), primary_key=True),
            Column('value', Integer)
        )
        t2 = Table('t2', meta,
            Column('id', Integer, Sequence('t2idseq', optional=True), primary_key=True),
            Column('value', Integer, default="7"),
            Column('stuff', String(20), onupdate="thisisstuff")
        )
        meta.create_all()
        try:
            t.insert().execute(value=func.length("one"))
            assert t.select().execute().fetchone()['value'] == 3
            t.update().execute(value=func.length("asfda"))
            assert t.select().execute().fetchone()['value'] == 5

            r = t.insert(values=dict(value=func.length("sfsaafsda"))).execute()
            id = r.last_inserted_ids()[0]
            assert t.select(t.c.id==id).execute().fetchone()['value'] == 9
            t.update(values={t.c.value:func.length("asdf")}).execute()
            assert t.select().execute().fetchone()['value'] == 4

            t2.insert().execute()
            t2.insert().execute(value=func.length("one"))
            t2.insert().execute(value=func.length("asfda") + -19, stuff="hi")

            assert select([t2.c.value, t2.c.stuff]).execute().fetchall() == [(7,None), (3,None), (-14,"hi")]
            
            t2.update().execute(value=func.length("asdsafasd"), stuff="some stuff")
            assert select([t2.c.value, t2.c.stuff]).execute().fetchall() == [(9,"some stuff"), (9,"some stuff"), (9,"some stuff")]
            
            t2.delete().execute()
            
            t2.insert(values=dict(value=func.length("one") + 8)).execute()
            assert t2.select().execute().fetchone()['value'] == 11
            
            t2.update(values=dict(value=func.length("asfda"))).execute()
            assert select([t2.c.value, t2.c.stuff]).execute().fetchone() == (5, "thisisstuff")

            t2.update(values={t2.c.value:func.length("asfdaasdf"), t2.c.stuff:"foo"}).execute()
            print "HI", select([t2.c.value, t2.c.stuff]).execute().fetchone()
            assert select([t2.c.value, t2.c.stuff]).execute().fetchone() == (9, "foo")
            
        finally:
            meta.drop_all()
            
    @testbase.supported('postgres')
    def test_functions_with_cols(self):
        x = testbase.db.func.current_date().execute().scalar()
        y = testbase.db.func.current_date().select().execute().scalar()
        z = testbase.db.func.current_date().scalar()
        w = select(['*'], from_obj=[testbase.db.func.current_date()]).scalar()
        
        # construct a column-based FROM object out of a function, like in [ticket:172]
        s = select([column('date', type=DateTime)], from_obj=[testbase.db.func.current_date()])
        q = s.execute().fetchone()[s.c.date]
        r = s.alias('datequery').select().scalar()
        
        assert x == y == z == w == q == r
        
    def test_column_order_with_simple_query(self):
        # should return values in column definition order
        users.insert().execute(user_id=1, user_name='foo')
        r = users.select(users.c.user_id==1).execute().fetchone()
        self.assertEqual(r[0], 1)
        self.assertEqual(r[1], 'foo')
        self.assertEqual([x.lower() for x in r.keys()], ['user_id', 'user_name'])
        self.assertEqual(r.values(), [1, 'foo'])
        
    def test_column_order_with_text_query(self):
        # should return values in query order
        users.insert().execute(user_id=1, user_name='foo')
        r = testbase.db.execute('select user_name, user_id from query_users', {}).fetchone()
        self.assertEqual(r[0], 'foo')
        self.assertEqual(r[1], 1)
        self.assertEqual([x.lower() for x in r.keys()], ['user_name', 'user_id'])
        self.assertEqual(r.values(), ['foo', 1])
    
    @testbase.unsupported('oracle', 'firebird') 
    def test_column_accessor_shadow(self):
        meta = BoundMetaData(testbase.db)
        shadowed = Table('test_shadowed', meta,
                         Column('shadow_id', INT, primary_key = True),
                         Column('shadow_name', VARCHAR(20)),
                         Column('parent', VARCHAR(20)),
                         Column('row', VARCHAR(40)),
                         Column('__parent', VARCHAR(20)),
                         Column('__row', VARCHAR(20)),
        )
        shadowed.create(checkfirst=True)
        try:
            shadowed.insert().execute(shadow_id=1, shadow_name='The Shadow', parent='The Light', row='Without light there is no shadow', __parent='Hidden parent', __row='Hidden row')
            r = shadowed.select(shadowed.c.shadow_id==1).execute().fetchone()
            self.assert_(r.shadow_id == r['shadow_id'] == r[shadowed.c.shadow_id] == 1)
            self.assert_(r.shadow_name == r['shadow_name'] == r[shadowed.c.shadow_name] == 'The Shadow')
            self.assert_(r.parent == r['parent'] == r[shadowed.c.parent] == 'The Light')
            self.assert_(r.row == r['row'] == r[shadowed.c.row] == 'Without light there is no shadow')
            self.assert_(r['__parent'] == 'Hidden parent')
            self.assert_(r['__row'] == 'Hidden row')
            try:
                print r.__parent, r.__row
                self.fail('Should not allow access to private attributes')
            except AttributeError:
                pass # expected
            r.close()
        finally:
            shadowed.drop(checkfirst=True)
    
    @testbase.supported('mssql')
    def test_fetchid_trigger(self):
        meta = BoundMetaData(testbase.db)
        t1 = Table('t1', meta,
                Column('id', Integer, Sequence('fred', 100, 1), primary_key=True),
                Column('descr', String(200)))
        t2 = Table('t2', meta,
                Column('id', Integer, Sequence('fred', 200, 1), primary_key=True),
                Column('descr', String(200)))
        meta.create_all()
        con = testbase.db.connect()
        con.execute("""create trigger paj on t1 for insert as
            insert into t2 (descr) select descr from inserted""")

        try:
            tr = con.begin()
            r = con.execute(t2.insert(), descr='hello')
            self.assert_(r.last_inserted_ids() == [200])
            r = con.execute(t1.insert(), descr='hello')
            self.assert_(r.last_inserted_ids() == [100])

        finally:
            tr.commit()
            con.execute("""drop trigger paj""")
            meta.drop_all()

    @testbase.supported('mssql')
    def test_insertid_schema(self):
        meta = BoundMetaData(testbase.db)
        con = testbase.db.connect()
        con.execute('create schema paj')
        tbl = Table('test', meta, Column('id', Integer, primary_key=True), schema='paj')
        tbl.create()        
        try:
            tbl.insert().execute({'id':1})        
        finally:
            tbl.drop()
            con.execute('drop schema paj')
        

class CompoundTest(PersistTest):
    """test compound statements like UNION, INTERSECT, particularly their ability to nest on
    different databases."""
    def setUpAll(self):
        global metadata, t1, t2, t3
        metadata = BoundMetaData(testbase.db)
        t1 = Table('t1', metadata, 
            Column('col1', Integer, Sequence('t1pkseq'), primary_key=True),
            Column('col2', String(30)),
            Column('col3', String(40)),
            Column('col4', String(30))
            )
        t2 = Table('t2', metadata, 
            Column('col1', Integer, Sequence('t2pkseq'), primary_key=True),
            Column('col2', String(30)),
            Column('col3', String(40)),
            Column('col4', String(30)))
        t3 = Table('t3', metadata, 
            Column('col1', Integer, Sequence('t3pkseq'), primary_key=True),
            Column('col2', String(30)),
            Column('col3', String(40)),
            Column('col4', String(30)))
        metadata.create_all()
        
        t1.insert().execute([
            dict(col2="t1col2r1", col3="aaa", col4="aaa"),
            dict(col2="t1col2r2", col3="bbb", col4="bbb"),
            dict(col2="t1col2r3", col3="ccc", col4="ccc"),
        ])
        t2.insert().execute([
            dict(col2="t2col2r1", col3="aaa", col4="bbb"),
            dict(col2="t2col2r2", col3="bbb", col4="ccc"),
            dict(col2="t2col2r3", col3="ccc", col4="aaa"),
        ])
        t3.insert().execute([
            dict(col2="t3col2r1", col3="aaa", col4="ccc"),
            dict(col2="t3col2r2", col3="bbb", col4="aaa"),
            dict(col2="t3col2r3", col3="ccc", col4="bbb"),
        ])
        
    def tearDownAll(self):
        metadata.drop_all()
        
    def test_union(self):
        (s1, s2) = (
                    select([t1.c.col3.label('col3'), t1.c.col4.label('col4')], t1.c.col2.in_("t1col2r1", "t1col2r2")),
            select([t2.c.col3.label('col3'), t2.c.col4.label('col4')], t2.c.col2.in_("t2col2r2", "t2col2r3"))
        )        
        u = union(s1, s2, order_by=['col3', 'col4'])
        assert u.execute().fetchall() == [('aaa', 'aaa'), ('bbb', 'bbb'), ('bbb', 'ccc'), ('ccc', 'aaa')]
        assert u.alias('bar').select().execute().fetchall() == [('aaa', 'aaa'), ('bbb', 'bbb'), ('bbb', 'ccc'), ('ccc', 'aaa')]
        
    @testbase.unsupported('mysql')
    def test_intersect(self):
        i = intersect(
            select([t2.c.col3, t2.c.col4]),
            select([t2.c.col3, t2.c.col4], t2.c.col4==t3.c.col3)
        )
        assert i.execute().fetchall() == [('aaa', 'bbb'), ('bbb', 'ccc'), ('ccc', 'aaa')]
        assert i.alias('bar').select().execute().fetchall() == [('aaa', 'bbb'), ('bbb', 'ccc'), ('ccc', 'aaa')]

    @testbase.unsupported('mysql', 'oracle')
    def test_except_style1(self):
        e = except_(union(
            select([t1.c.col3, t1.c.col4]),
            select([t2.c.col3, t2.c.col4]),
            select([t3.c.col3, t3.c.col4]),
        ), select([t2.c.col3, t2.c.col4]))
        assert e.alias('bar').select().execute().fetchall() == [('aaa', 'aaa'), ('aaa', 'ccc'), ('bbb', 'aaa'), ('bbb', 'bbb'), ('ccc', 'bbb'), ('ccc', 'ccc')]

    @testbase.unsupported('mysql', 'oracle')
    def test_except_style2(self):
        e = except_(union(
            select([t1.c.col3, t1.c.col4]),
            select([t2.c.col3, t2.c.col4]),
            select([t3.c.col3, t3.c.col4]),
        ).alias('foo').select(), select([t2.c.col3, t2.c.col4]))
        assert e.execute().fetchall() == [('aaa', 'aaa'), ('aaa', 'ccc'), ('bbb', 'aaa'), ('bbb', 'bbb'), ('ccc', 'bbb'), ('ccc', 'ccc')]
        assert e.alias('bar').select().execute().fetchall() == [('aaa', 'aaa'), ('aaa', 'ccc'), ('bbb', 'aaa'), ('bbb', 'bbb'), ('ccc', 'bbb'), ('ccc', 'ccc')]

    @testbase.unsupported('mysql')
    def test_composite(self):
        u = intersect(
            select([t2.c.col3, t2.c.col4]),
            union(
                select([t1.c.col3, t1.c.col4]),
                select([t2.c.col3, t2.c.col4]),
                select([t3.c.col3, t3.c.col4]),
            ).alias('foo').select()
        )
        assert u.execute().fetchall() == [('aaa', 'bbb'), ('bbb', 'ccc'), ('ccc', 'aaa')]
        assert u.alias('foo').select().execute().fetchall() == [('aaa', 'bbb'), ('bbb', 'ccc'), ('ccc', 'aaa')]

        
if __name__ == "__main__":
    testbase.main()        
