import testenv; testenv.configure_for_tests()
import datetime
from sqlalchemy import Sequence, Column, func
from testlib import sa, testing
from testlib.sa import MetaData, Table, Integer, String, ForeignKey
from testlib.testing import eq_
from sql import _base


class DefaultTest(testing.TestBase):

    def setUpAll(self):
        global t, f, f2, ts, currenttime, metadata, default_generator

        db = testing.db
        metadata = MetaData(db)
        default_generator = {'x':50}

        def mydefault():
            default_generator['x'] += 1
            return default_generator['x']

        def myupdate_with_ctx(ctx):
            conn = ctx.connection
            return conn.execute(sa.select([sa.text('13')])).scalar()

        def mydefault_using_connection(ctx):
            conn = ctx.connection
            try:
                return conn.execute(sa.select([sa.text('12')])).scalar()
            finally:
                # ensure a "close()" on this connection does nothing,
                # since its a "branched" connection
                conn.close()

        use_function_defaults = testing.against('postgres', 'mssql', 'maxdb')
        is_oracle = testing.against('oracle')

        # select "count(1)" returns different results on different DBs also
        # correct for "current_date" compatible as column default, value
        # differences
        currenttime = func.current_date(type_=sa.Date, bind=db)
        if is_oracle:
            ts = db.scalar(sa.select([func.trunc(func.sysdate(), sa.literal_column("'DAY'"), type_=sa.Date).label('today')]))
            assert isinstance(ts, datetime.date) and not isinstance(ts, datetime.datetime)
            f = sa.select([func.length('abcdef')], bind=db).scalar()
            f2 = sa.select([func.length('abcdefghijk')], bind=db).scalar()
            # TODO: engine propigation across nested functions not working
            currenttime = func.trunc(currenttime, sa.literal_column("'DAY'"), bind=db, type_=sa.Date)
            def1 = currenttime
            def2 = func.trunc(sa.text("sysdate"), sa.literal_column("'DAY'"), type_=sa.Date)

            deftype = sa.Date
        elif use_function_defaults:
            f = sa.select([func.length('abcdef')], bind=db).scalar()
            f2 = sa.select([func.length('abcdefghijk')], bind=db).scalar()
            def1 = currenttime
            deftype = sa.Date
            if testing.against('maxdb'):
                def2 = sa.text("curdate")
            elif testing.against('mssql'):
                def2 = sa.text("getdate()")
            else:
                def2 = sa.text("current_date")
            ts = db.func.current_date().scalar()
        else:
            f = len('abcdef')
            f2 = len('abcdefghijk')
            def1 = def2 = "3"
            ts = 3
            deftype = Integer

        t = Table('default_test1', metadata,
            # python function
            Column('col1', Integer, primary_key=True,
                   default=mydefault),

            # python literal
            Column('col2', String(20),
                   default="imthedefault",
                   onupdate="im the update"),

            # preexecute expression
            Column('col3', Integer,
                   default=func.length('abcdef'),
                   onupdate=func.length('abcdefghijk')),

            # SQL-side default from sql expression
            Column('col4', deftype,
                   server_default=def1),

            # SQL-side default from literal expression
            Column('col5', deftype,
                   server_default=def2),

            # preexecute + update timestamp
            Column('col6', sa.Date,
                   default=currenttime,
                   onupdate=currenttime),

            Column('boolcol1', sa.Boolean, default=True),
            Column('boolcol2', sa.Boolean, default=False),

            # python function which uses ExecutionContext
            Column('col7', Integer,
                   default=mydefault_using_connection,
                   onupdate=myupdate_with_ctx),

            # python builtin
            Column('col8', sa.Date,
                   default=datetime.date.today,
                   onupdate=datetime.date.today),
            # combo
            Column('col9', String(20),
                   default='py',
                   server_default='ddl'))
        t.create()

    def tearDownAll(self):
        t.drop()

    def tearDown(self):
        default_generator['x'] = 50
        t.delete().execute()

    def test_bad_arg_signature(self):
        ex_msg = \
          "ColumnDefault Python function takes zero or one positional arguments"

        def fn1(x, y): pass
        def fn2(x, y, z=3): pass
        class fn3(object):
            def __init__(self, x, y):
                pass
        class FN4(object):
            def __call__(self, x, y):
                pass
        fn4 = FN4()

        for fn in fn1, fn2, fn3, fn4:
            self.assertRaisesMessage(sa.exc.ArgumentError,
                                     ex_msg,
                                     sa.ColumnDefault, fn)

    def test_arg_signature(self):
        def fn1(): pass
        def fn2(): pass
        def fn3(x=1): pass
        def fn4(x=1, y=2, z=3): pass
        fn5 = list
        class fn6(object):
            def __init__(self, x):
                pass
        class fn6(object):
            def __init__(self, x, y=3):
                pass
        class FN7(object):
            def __call__(self, x):
                pass
        fn7 = FN7()
        class FN8(object):
            def __call__(self, x, y=3):
                pass
        fn8 = FN8()

        for fn in fn1, fn2, fn3, fn4, fn5, fn6, fn7, fn8:
            c = sa.ColumnDefault(fn)

    @testing.fails_on('firebird') # 'Data type unknown'
    def test_standalone(self):
        c = testing.db.engine.contextual_connect()
        x = c.execute(t.c.col1.default)
        y = t.c.col2.default.execute()
        z = c.execute(t.c.col3.default)
        assert 50 <= x <= 57
        eq_(y, 'imthedefault')
        eq_(z, f)
        eq_(f2, 11)

    def test_py_vs_server_default_detection(self):

        def has_(name, *wanted):
            slots = ['default', 'onupdate', 'server_default', 'server_onupdate']
            col = tbl.c[name]
            for slot in wanted:
                slots.remove(slot)
                assert getattr(col, slot) is not None, getattr(col, slot)
            for slot in slots:
                assert getattr(col, slot) is None, getattr(col, slot)

        tbl = t
        has_('col1', 'default')
        has_('col2', 'default', 'onupdate')
        has_('col3', 'default', 'onupdate')
        has_('col4', 'server_default')
        has_('col5', 'server_default')
        has_('col6', 'default', 'onupdate')
        has_('boolcol1', 'default')
        has_('boolcol2', 'default')
        has_('col7', 'default', 'onupdate')
        has_('col8', 'default', 'onupdate')
        has_('col9', 'default', 'server_default')

        ColumnDefault, DefaultClause = sa.ColumnDefault, sa.DefaultClause

        t2 = Table('t2', MetaData(),
                   Column('col1', Integer, Sequence('foo')),
                   Column('col2', Integer,
                          default=Sequence('foo'),
                          server_default='y'),
                   Column('col3', Integer,
                          Sequence('foo'),
                          server_default='x'),
                   Column('col4', Integer,
                          ColumnDefault('x'),
                          DefaultClause('y')),
                   Column('col4', Integer,
                          ColumnDefault('x'),
                          DefaultClause('y'),
                          DefaultClause('y', for_update=True)),
                   Column('col5', Integer,
                          ColumnDefault('x'),
                          DefaultClause('y'),
                          onupdate='z'),
                   Column('col6', Integer,
                          ColumnDefault('x'),
                          server_default='y',
                          onupdate='z'),
                   Column('col7', Integer,
                          default='x',
                          server_default='y',
                          onupdate='z'),
                   Column('col8', Integer,
                          server_onupdate='u',
                          default='x',
                          server_default='y',
                          onupdate='z'))
        tbl = t2
        has_('col1', 'default')
        has_('col2', 'default', 'server_default')
        has_('col3', 'default', 'server_default')
        has_('col4', 'default', 'server_default', 'server_onupdate')
        has_('col5', 'default', 'server_default', 'onupdate')
        has_('col6', 'default', 'server_default', 'onupdate')
        has_('col7', 'default', 'server_default', 'onupdate')
        has_('col8', 'default', 'server_default', 'onupdate', 'server_onupdate')

    @testing.fails_on('firebird') # 'Data type unknown'
    def test_insert(self):
        r = t.insert().execute()
        assert r.lastrow_has_defaults()
        eq_(set(r.context.postfetch_cols),
            set([t.c.col3, t.c.col5, t.c.col4, t.c.col6]))

        r = t.insert(inline=True).execute()
        assert r.lastrow_has_defaults()
        eq_(set(r.context.postfetch_cols),
            set([t.c.col3, t.c.col5, t.c.col4, t.c.col6]))

        t.insert().execute()

        ctexec = sa.select([currenttime.label('now')], bind=testing.db).scalar()
        l = t.select().execute()
        today = datetime.date.today()
        eq_(l.fetchall(), [
            (x, 'imthedefault', f, ts, ts, ctexec, True, False,
             12, today, 'py')
            for x in range(51, 54)])

        t.insert().execute(col9=None)
        assert r.lastrow_has_defaults()
        eq_(set(r.context.postfetch_cols),
            set([t.c.col3, t.c.col5, t.c.col4, t.c.col6]))

        eq_(t.select(t.c.col1==54).execute().fetchall(),
            [(54, 'imthedefault', f, ts, ts, ctexec, True, False,
              12, today, None)])

    @testing.fails_on('firebird') # 'Data type unknown'
    def test_insertmany(self):
        # MySQL-Python 1.2.2 breaks functions in execute_many :(
        if (testing.against('mysql') and
            testing.db.dialect.dbapi.version_info[:3] == (1, 2, 2)):
            return

        r = t.insert().execute({}, {}, {})

        ctexec = currenttime.scalar()
        l = t.select().execute()
        today = datetime.date.today()
        eq_(l.fetchall(),
            [(51, 'imthedefault', f, ts, ts, ctexec, True, False,
              12, today, 'py'),
             (52, 'imthedefault', f, ts, ts, ctexec, True, False,
              12, today, 'py'),
             (53, 'imthedefault', f, ts, ts, ctexec, True, False,
              12, today, 'py')])

    def test_insert_values(self):
        t.insert(values={'col3':50}).execute()
        l = t.select().execute()
        eq_(50, l.fetchone()['col3'])

    @testing.fails_on('firebird') # 'Data type unknown'
    def test_updatemany(self):
        # MySQL-Python 1.2.2 breaks functions in execute_many :(
        if (testing.against('mysql') and
            testing.db.dialect.dbapi.version_info[:3] == (1, 2, 2)):
            return

        t.insert().execute({}, {}, {})

        t.update(t.c.col1==sa.bindparam('pkval')).execute(
            {'pkval':51,'col7':None, 'col8':None, 'boolcol1':False})

        t.update(t.c.col1==sa.bindparam('pkval')).execute(
            {'pkval':51,},
            {'pkval':52,},
            {'pkval':53,})

        l = t.select().execute()
        ctexec = currenttime.scalar()
        today = datetime.date.today()
        eq_(l.fetchall(),
            [(51, 'im the update', f2, ts, ts, ctexec, False, False,
              13, today, 'py'),
             (52, 'im the update', f2, ts, ts, ctexec, True, False,
              13, today, 'py'),
             (53, 'im the update', f2, ts, ts, ctexec, True, False,
              13, today, 'py')])

    @testing.fails_on('firebird') # 'Data type unknown'
    def test_update(self):
        r = t.insert().execute()
        pk = r.last_inserted_ids()[0]
        t.update(t.c.col1==pk).execute(col4=None, col5=None)
        ctexec = currenttime.scalar()
        l = t.select(t.c.col1==pk).execute()
        l = l.fetchone()
        eq_(l,
            (pk, 'im the update', f2, None, None, ctexec, True, False,
             13, datetime.date.today(), 'py'))
        eq_(11, f2)

    @testing.fails_on('firebird') # 'Data type unknown'
    def test_update_values(self):
        r = t.insert().execute()
        pk = r.last_inserted_ids()[0]
        t.update(t.c.col1==pk, values={'col3': 55}).execute()
        l = t.select(t.c.col1==pk).execute()
        l = l.fetchone()
        eq_(55, l['col3'])

    @testing.fails_on_everything_except('postgres')
    def test_passive_override(self):
        """
        Primarily for postgres, tests that when we get a primary key column
        back from reflecting a table which has a default value on it, we
        pre-execute that DefaultClause upon insert, even though DefaultClause
        says "let the database execute this", because in postgres we must have
        all the primary key values in memory before insert; otherwise we can't
        locate the just inserted row.

        """
        # TODO: move this to dialect/postgres
        try:
            meta = MetaData(testing.db)
            testing.db.execute("""
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
            eq_(l, [(1, 'user', 'lala')])
        finally:
            testing.db.execute("drop table speedy_users", None)


class PKDefaultTest(_base.TablesTest):
    __requires__ = ('subqueries',)

    def define_tables(self, metadata):
        t2 = Table('t2', metadata,
            Column('nextid', Integer))

        Table('t1', metadata,
              Column('id', Integer, primary_key=True,
                     default=sa.select([func.max(t2.c.nextid)]).as_scalar()),
              Column('data', String(30)))

    @testing.crashes('mssql', 'FIXME: unknown, verify not fails_on')
    @testing.resolve_artifact_names
    def test_basic(self):
        t2.insert().execute(nextid=1)
        r = t1.insert().execute(data='hi')
        eq_([1], r.last_inserted_ids())

        t2.insert().execute(nextid=2)
        r = t1.insert().execute(data='there')
        eq_([2], r.last_inserted_ids())


class PKIncrementTest(_base.TablesTest):
    run_define_tables = 'each'

    def define_tables(self, metadata):
        Table("aitable", metadata,
              Column('id', Integer, Sequence('ai_id_seq', optional=True),
                     primary_key=True),
              Column('int1', Integer),
              Column('str1', String(20)))

    # TODO: add coverage for increment on a secondary column in a key
    @testing.fails_on('firebird') # data type unknown
    @testing.resolve_artifact_names
    def _test_autoincrement(self, bind):
        ids = set()
        rs = bind.execute(aitable.insert(), int1=1)
        last = rs.last_inserted_ids()[0]
        self.assert_(last)
        self.assert_(last not in ids)
        ids.add(last)

        rs = bind.execute(aitable.insert(), str1='row 2')
        last = rs.last_inserted_ids()[0]
        self.assert_(last)
        self.assert_(last not in ids)
        ids.add(last)

        rs = bind.execute(aitable.insert(), int1=3, str1='row 3')
        last = rs.last_inserted_ids()[0]
        self.assert_(last)
        self.assert_(last not in ids)
        ids.add(last)

        rs = bind.execute(aitable.insert(values={'int1':func.length('four')}))
        last = rs.last_inserted_ids()[0]
        self.assert_(last)
        self.assert_(last not in ids)
        ids.add(last)

        eq_(list(bind.execute(aitable.select().order_by(aitable.c.id))),
            [(1, 1, None), (2, None, 'row 2'), (3, 3, 'row 3'), (4, 4, None)])

    @testing.resolve_artifact_names
    def test_autoincrement_autocommit(self):
        self._test_autoincrement(testing.db)

    @testing.resolve_artifact_names
    def test_autoincrement_transaction(self):
        con = testing.db.connect()
        tx = con.begin()
        try:
            try:
                self._test_autoincrement(con)
            except:
                try:
                    tx.rollback()
                except:
                    pass
                raise
            else:
                tx.commit()
        finally:
            con.close()


class AutoIncrementTest(_base.TablesTest):
    __requires__ = ('identity',)
    run_define_tables = 'each'

    def define_tables(self, metadata):
        """Each test manipulates self.metadata individually."""

    @testing.exclude('sqlite', '<', (3, 4), 'no database support')
    def test_autoincrement_single_col(self):
        single = Table('single', self.metadata,
                       Column('id', Integer, primary_key=True))
        single.create()

        r = single.insert().execute()
        id_ = r.last_inserted_ids()[0]
        assert id_ is not None
        eq_(1, sa.select([func.count(sa.text('*'))], from_obj=single).scalar())

    def test_autoincrement_fk(self):
        nodes = Table('nodes', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('parent_id', Integer, ForeignKey('nodes.id')),
            Column('data', String(30)))
        nodes.create()

        r = nodes.insert().execute(data='foo')
        id_ = r.last_inserted_ids()[0]
        nodes.insert().execute(data='bar', parent_id=id_)

    @testing.fails_on('sqlite')
    def test_non_autoincrement(self):
        # sqlite INT primary keys can be non-unique! (only for ints)
        nonai = Table("nonaitest", self.metadata,
            Column('id', Integer, autoincrement=False, primary_key=True),
            Column('data', String(20)))
        nonai.create()


        try:
            # postgres + mysql strict will fail on first row,
            # mysql in legacy mode fails on second row
            nonai.insert().execute(data='row 1')
            nonai.insert().execute(data='row 2')
            assert False
        except sa.exc.SQLError, e:
            assert True

        nonai.insert().execute(id=1, data='row 1')


class SequenceTest(testing.TestBase):
    __requires__ = ('sequences',)

    def setUpAll(self):
        global cartitems, sometable, metadata
        metadata = MetaData(testing.db)
        cartitems = Table("cartitems", metadata,
            Column("cart_id", Integer, Sequence('cart_id_seq'), primary_key=True),
            Column("description", String(40)),
            Column("createdate", sa.DateTime())
        )
        sometable = Table( 'Manager', metadata,
               Column('obj_id', Integer, Sequence('obj_id_seq'), ),
               Column('name', String(128)),
               Column('id', Integer, Sequence('Manager_id_seq', optional=True),
                      primary_key=True),
           )

        metadata.create_all()

    def testseqnonpk(self):
        """test sequences fire off as defaults on non-pk columns"""

        result = sometable.insert().execute(name="somename")
        assert 'id' in result.postfetch_cols()

        result = sometable.insert().execute(name="someother")
        assert 'id' in result.postfetch_cols()

        sometable.insert().execute(
            {'name':'name3'},
            {'name':'name4'})
        eq_(sometable.select().execute().fetchall(),
            [(1, "somename", 1),
             (2, "someother", 2),
             (3, "name3", 3),
             (4, "name4", 4)])

    def testsequence(self):
        cartitems.insert().execute(description='hi')
        cartitems.insert().execute(description='there')
        r = cartitems.insert().execute(description='lala')

        assert r.last_inserted_ids() and r.last_inserted_ids()[0] is not None
        id_ = r.last_inserted_ids()[0]

        eq_(1,
            sa.select([func.count(cartitems.c.cart_id)],
                      sa.and_(cartitems.c.description == 'lala',
                              cartitems.c.cart_id == id_)).scalar())

        cartitems.select().execute().fetchall()

    @testing.fails_on('maxdb')
    # maxdb db-api seems to double-execute NEXTVAL internally somewhere,
    # throwing off the numbers for these tests...
    def test_implicit_sequence_exec(self):
        s = Sequence("my_sequence", metadata=MetaData(testing.db))
        s.create()
        try:
            x = s.execute()
            eq_(x, 1)
        finally:
            s.drop()

    @testing.fails_on('maxdb')
    def teststandalone_explicit(self):
        s = Sequence("my_sequence")
        s.create(bind=testing.db)
        try:
            x = s.execute(testing.db)
            eq_(x, 1)
        finally:
            s.drop(testing.db)

    def test_checkfirst(self):
        s = Sequence("my_sequence")
        s.create(testing.db, checkfirst=False)
        s.create(testing.db, checkfirst=True)
        s.drop(testing.db, checkfirst=False)
        s.drop(testing.db, checkfirst=True)

    @testing.fails_on('maxdb')
    def teststandalone2(self):
        x = cartitems.c.cart_id.sequence.execute()
        self.assert_(1 <= x <= 4)

    def tearDownAll(self):
        metadata.drop_all()


if __name__ == "__main__":
    testenv.main()
