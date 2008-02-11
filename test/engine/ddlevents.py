import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from sqlalchemy.schema import DDL
import sqlalchemy
from testlib import *


class DDLEventTest(TestBase):
    class Canary(object):
        def __init__(self, schema_item, bind):
            self.state = None
            self.schema_item = schema_item
            self.bind = bind

        def before_create(self, action, schema_item, bind):
            assert self.state is None
            assert schema_item is self.schema_item
            assert bind is self.bind
            self.state = action

        def after_create(self, action, schema_item, bind):
            assert self.state in ('before-create', 'skipped')
            assert schema_item is self.schema_item
            assert bind is self.bind
            self.state = action

        def before_drop(self, action, schema_item, bind):
            assert self.state is None
            assert schema_item is self.schema_item
            assert bind is self.bind
            self.state = action

        def after_drop(self, action, schema_item, bind):
            assert self.state in ('before-drop', 'skipped')
            assert schema_item is self.schema_item
            assert bind is self.bind
            self.state = action

    def mock_engine(self):
        buffer = []
        def executor(sql, *a, **kw):
            buffer.append(sql)
        engine = create_engine(testing.db.name + '://',
                               strategy='mock', executor=executor)
        assert not hasattr(engine, 'mock')
        engine.mock = buffer
        return engine

    def setUp(self):
        self.bind = self.mock_engine()
        self.metadata = MetaData()
        self.table = Table('t', self.metadata, Column('id', Integer))

    def test_table_create_before(self):
        table, bind = self.table, self.bind
        canary = self.Canary(table, bind)
        table.ddl_listeners['before-create'].append(canary.before_create)

        table.create(bind)
        assert canary.state == 'before-create'
        table.drop(bind)
        assert canary.state == 'before-create'

    def test_table_create_after(self):
        table, bind = self.table, self.bind
        canary = self.Canary(table, bind)
        table.ddl_listeners['after-create'].append(canary.after_create)

        canary.state = 'skipped'
        table.create(bind)
        assert canary.state == 'after-create'
        table.drop(bind)
        assert canary.state == 'after-create'

    def test_table_create_both(self):
        table, bind = self.table, self.bind
        canary = self.Canary(table, bind)
        table.ddl_listeners['before-create'].append(canary.before_create)
        table.ddl_listeners['after-create'].append(canary.after_create)

        table.create(bind)
        assert canary.state == 'after-create'
        table.drop(bind)
        assert canary.state == 'after-create'

    def test_table_drop_before(self):
        table, bind = self.table, self.bind
        canary = self.Canary(table, bind)
        table.ddl_listeners['before-drop'].append(canary.before_drop)

        table.create(bind)
        assert canary.state is None
        table.drop(bind)
        assert canary.state == 'before-drop'

    def test_table_drop_after(self):
        table, bind = self.table, self.bind
        canary = self.Canary(table, bind)
        table.ddl_listeners['after-drop'].append(canary.after_drop)

        table.create(bind)
        assert canary.state is None
        canary.state = 'skipped'
        table.drop(bind)
        assert canary.state == 'after-drop'

    def test_table_drop_both(self):
        table, bind = self.table, self.bind
        canary = self.Canary(table, bind)
        table.ddl_listeners['before-drop'].append(canary.before_drop)
        table.ddl_listeners['after-drop'].append(canary.after_drop)

        table.create(bind)
        assert canary.state is None
        table.drop(bind)
        assert canary.state == 'after-drop'

    def test_table_all(self):
        table, bind = self.table, self.bind
        canary = self.Canary(table, bind)
        table.ddl_listeners['before-create'].append(canary.before_create)
        table.ddl_listeners['after-create'].append(canary.after_create)
        table.ddl_listeners['before-drop'].append(canary.before_drop)
        table.ddl_listeners['after-drop'].append(canary.after_drop)

        assert canary.state is None
        table.create(bind)
        assert canary.state == 'after-create'
        canary.state = None
        table.drop(bind)
        assert canary.state == 'after-drop'

    def test_table_create_before(self):
        metadata, bind = self.metadata, self.bind
        canary = self.Canary(metadata, bind)
        metadata.ddl_listeners['before-create'].append(canary.before_create)

        metadata.create_all(bind)
        assert canary.state == 'before-create'
        metadata.drop_all(bind)
        assert canary.state == 'before-create'

    def test_metadata_create_after(self):
        metadata, bind = self.metadata, self.bind
        canary = self.Canary(metadata, bind)
        metadata.ddl_listeners['after-create'].append(canary.after_create)

        canary.state = 'skipped'
        metadata.create_all(bind)
        assert canary.state == 'after-create'
        metadata.drop_all(bind)
        assert canary.state == 'after-create'

    def test_metadata_create_both(self):
        metadata, bind = self.metadata, self.bind
        canary = self.Canary(metadata, bind)
        metadata.ddl_listeners['before-create'].append(canary.before_create)
        metadata.ddl_listeners['after-create'].append(canary.after_create)

        metadata.create_all(bind)
        assert canary.state == 'after-create'
        metadata.drop_all(bind)
        assert canary.state == 'after-create'

    @testing.future
    def test_metadata_table_isolation(self):
        metadata, table, bind = self.metadata, self.table, self.bind

        table_canary = self.Canary(table, bind)
        table.ddl_listeners['before-create'].append(table_canary.before_create)

        metadata_canary = self.Canary(metadata, bind)
        metadata.ddl_listeners['before-create'].append(metadata_canary.before_create)

        # currently, table.create() routes through the same execution
        # path that metadata.create_all() does
        self.table.create(self.bind)
        assert metadata_canary.state == None

    def test_append_listener(self):
        metadata, table, bind = self.metadata, self.table, self.bind

        fn = lambda *a: None

        table.append_ddl_listener('before-create', fn)
        self.assertRaises(LookupError, table.append_ddl_listener, 'blah', fn)

        metadata.append_ddl_listener('before-create', fn)
        self.assertRaises(LookupError, metadata.append_ddl_listener, 'blah', fn)


class DDLExecutionTest(TestBase):
    def mock_engine(self):
        buffer = []
        def executor(sql, *a, **kw):
            buffer.append(sql)
        engine = create_engine(testing.db.name + '://',
                               strategy='mock', executor=executor)
        assert not hasattr(engine, 'mock')
        engine.mock = buffer
        return engine

    def setUp(self):
        self.engine = self.mock_engine()
        self.metadata = MetaData(self.engine)
        self.users = Table('users', self.metadata,
                           Column('user_id', Integer, primary_key=True),
                           Column('user_name', String(40)),
                           )

    def test_table_standalone(self):
        users, engine = self.users, self.engine
        DDL('mxyzptlk').execute_at('before-create', users)
        DDL('klptzyxm').execute_at('after-create', users)
        DDL('xyzzy').execute_at('before-drop', users)
        DDL('fnord').execute_at('after-drop', users)

        users.create()
        strings = [str(x) for x in engine.mock]
        assert 'mxyzptlk' in strings
        assert 'klptzyxm' in strings
        assert 'xyzzy' not in strings
        assert 'fnord' not in strings
        del engine.mock[:]
        users.drop()
        strings = [str(x) for x in engine.mock]
        assert 'mxyzptlk' not in strings
        assert 'klptzyxm' not in strings
        assert 'xyzzy' in strings
        assert 'fnord' in strings

    def test_table_by_metadata(self):
        metadata, users, engine = self.metadata, self.users, self.engine
        DDL('mxyzptlk').execute_at('before-create', users)
        DDL('klptzyxm').execute_at('after-create', users)
        DDL('xyzzy').execute_at('before-drop', users)
        DDL('fnord').execute_at('after-drop', users)

        metadata.create_all()
        strings = [str(x) for x in engine.mock]
        assert 'mxyzptlk' in strings
        assert 'klptzyxm' in strings
        assert 'xyzzy' not in strings
        assert 'fnord' not in strings
        del engine.mock[:]
        metadata.drop_all()
        strings = [str(x) for x in engine.mock]
        assert 'mxyzptlk' not in strings
        assert 'klptzyxm' not in strings
        assert 'xyzzy' in strings
        assert 'fnord' in strings

    def test_metadata(self):
        metadata, engine = self.metadata, self.engine
        DDL('mxyzptlk').execute_at('before-create', metadata)
        DDL('klptzyxm').execute_at('after-create', metadata)
        DDL('xyzzy').execute_at('before-drop', metadata)
        DDL('fnord').execute_at('after-drop', metadata)

        metadata.create_all()
        strings = [str(x) for x in engine.mock]
        assert 'mxyzptlk' in strings
        assert 'klptzyxm' in strings
        assert 'xyzzy' not in strings
        assert 'fnord' not in strings
        del engine.mock[:]
        metadata.drop_all()
        strings = [str(x) for x in engine.mock]
        assert 'mxyzptlk' not in strings
        assert 'klptzyxm' not in strings
        assert 'xyzzy' in strings
        assert 'fnord' in strings

    def test_ddl_execute(self):
        engine = create_engine('sqlite:///')
        cx = engine.connect()
        table = self.users
        ddl = DDL('SELECT 1')

        for py in ('engine.execute(ddl)',
                   'engine.execute(ddl, table)',
                   'cx.execute(ddl)',
                   'cx.execute(ddl, table)',
                   'ddl.execute(engine)',
                   'ddl.execute(engine, table)',
                   'ddl.execute(cx)',
                   'ddl.execute(cx, table)'):
            r = eval(py)
            assert list(r) == [(1,)], py

class DDLTest(TestBase):
    def mock_engine(self):
        executor = lambda *a, **kw: None
        engine = create_engine(testing.db.name + '://',
                               strategy='mock', executor=executor)
        engine.dialect.identifier_preparer = \
           sqlalchemy.sql.compiler.IdentifierPreparer(engine.dialect)
        return engine

    def test_tokens(self):
        m = MetaData()
        bind = self.mock_engine()
        sane_alone = Table('t', m, Column('id', Integer))
        sane_schema = Table('t', m, Column('id', Integer), schema='s')
        insane_alone = Table('t t', m, Column('id', Integer))
        insane_schema = Table('t t', m, Column('id', Integer), schema='s s')

        ddl = DDL('%(schema)s-%(table)s-%(fullname)s')

        self.assertEquals(ddl._expand(sane_alone, bind), '-t-t')
        self.assertEquals(ddl._expand(sane_schema, bind), '"s"-t-s.t')
        self.assertEquals(ddl._expand(insane_alone, bind), '-"t t"-"t t"')
        self.assertEquals(ddl._expand(insane_schema, bind),
                          '"s s"-"t t"-"s s"."t t"')

        # overrides are used piece-meal and verbatim.
        ddl = DDL('%(schema)s-%(table)s-%(fullname)s-%(bonus)s',
                  context={'schema':'S S', 'table': 'T T', 'bonus': 'b'})
        self.assertEquals(ddl._expand(sane_alone, bind), 'S S-T T-t-b')
        self.assertEquals(ddl._expand(sane_schema, bind), 'S S-T T-s.t-b')
        self.assertEquals(ddl._expand(insane_alone, bind), 'S S-T T-"t t"-b')
        self.assertEquals(ddl._expand(insane_schema, bind),
                          'S S-T T-"s s"."t t"-b')
    def test_filter(self):
        cx = self.mock_engine()
        cx.name = 'mock'

        tbl = Table('t', MetaData(), Column('id', Integer))

        assert DDL('')._should_execute('x', tbl, cx)
        assert DDL('', on='mock')._should_execute('x', tbl, cx)
        assert not DDL('', on='bogus')._should_execute('x', tbl, cx)
        assert DDL('', on=lambda x,y,z: True)._should_execute('x', tbl, cx)
        assert(DDL('', on=lambda x,y,z: z.engine.name != 'bogus').
               _should_execute('x', tbl, cx))

    def test_repr(self):
        assert repr(DDL('s'))
        assert repr(DDL('s', on='engine'))
        assert repr(DDL('s', on=lambda x: 1))
        assert repr(DDL('s', context={'a':1}))
        assert repr(DDL('s', on='engine', context={'a':1}))


if __name__ == "__main__":
    testenv.main()
