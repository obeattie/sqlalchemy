# coding: utf-8

"""Tests unitofwork operations."""

import testenv; testenv.configure_for_tests()
import datetime
import operator
from sqlalchemy.orm import mapper as orm_mapper

from testlib import engines, sa, testing
from testlib.sa import Table, Column, Integer, String, ForeignKey
from testlib.sa.orm import mapper, relation, create_session
from testlib.testing import eq_, ne_
from testlib.compat import set
from orm import _base, _fixtures
from engine import _base as engine_base
import pickleable

class UnitOfWorkTest(object):
    pass

class HistoryTest(_fixtures.FixtureTest):
    run_inserts = None

    def setup_classes(self):
        class User(_base.ComparableEntity):
            pass
        class Address(_base.ComparableEntity):
            pass

    @testing.resolve_artifact_names
    def test_backref(self):
        am = mapper(Address, addresses)
        m = mapper(User, users, properties=dict(
            addresses = relation(am, backref='user', lazy=False)))

        session = create_session(autocommit=False)

        u = User(name='u1')
        a = Address(email_address='u1@e')
        a.user = u
        session.add(u)

        self.assert_(u.addresses == [a])
        session.commit()
        session.clear()

        u = session.query(m).one()
        assert u.addresses[0].user == u
        session.close()


class VersioningTest(_base.MappedTest):
    def define_tables(self, metadata):
        Table('version_table', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('version_id', Integer, nullable=False),
              Column('value', String(40), nullable=False))

    def setup_classes(self):
        class Foo(_base.ComparableEntity):
            pass

    @engines.close_open_connections
    @testing.resolve_artifact_names
    def test_basic(self):
        mapper(Foo, version_table, version_id_col=version_table.c.version_id)

        s1 = create_session(autocommit=False)
        f1 = Foo(value='f1')
        f2 = Foo(value='f2')
        s1.add_all((f1, f2))
        s1.commit()

        f1.value='f1rev2'
        s1.commit()

        s2 = create_session(autocommit=False)
        f1_s = s2.query(Foo).get(f1.id)
        f1_s.value='f1rev3'
        s2.commit()

        f1.value='f1rev3mine'

        # Only dialects with a sane rowcount can detect the
        # ConcurrentModificationError
        if testing.db.dialect.supports_sane_rowcount:
            self.assertRaises(sa.orm.exc.ConcurrentModificationError, s1.commit)
            s1.rollback()
        else:
            s1.commit()

        # new in 0.5 !  dont need to close the session
        f1 = s1.query(Foo).get(f1.id)
        f2 = s1.query(Foo).get(f2.id)

        f1_s.value='f1rev4'
        s2.commit()

        s1.delete(f1)
        s1.delete(f2)

        if testing.db.dialect.supports_sane_multi_rowcount:
            self.assertRaises(sa.orm.exc.ConcurrentModificationError, s1.commit)
        else:
            s1.commit()

    @engines.close_open_connections
    @testing.resolve_artifact_names
    def test_versioncheck(self):
        """query.with_lockmode performs a 'version check' on an already loaded instance"""

        s1 = create_session(autocommit=False)

        mapper(Foo, version_table, version_id_col=version_table.c.version_id)
        f1s1 = Foo(value='f1 value')
        s1.add(f1s1)
        s1.commit()

        s2 = create_session(autocommit=False)
        f1s2 = s2.query(Foo).get(f1s1.id)
        f1s2.value='f1 new value'
        s2.commit()

        # load, version is wrong
        self.assertRaises(sa.orm.exc.ConcurrentModificationError, s1.query(Foo).with_lockmode('read').get, f1s1.id)

        # reload it
        s1.query(Foo).load(f1s1.id)
        # now assert version OK
        s1.query(Foo).with_lockmode('read').get(f1s1.id)

        # assert brand new load is OK too
        s1.close()
        s1.query(Foo).with_lockmode('read').get(f1s1.id)

    @engines.close_open_connections
    @testing.resolve_artifact_names
    def test_noversioncheck(self):
        """test query.with_lockmode works when the mapper has no version id col"""
        s1 = create_session(autocommit=False)
        mapper(Foo, version_table)
        f1s1 = Foo(value="foo", version_id=0)
        s1.add(f1s1)
        s1.commit()

        s2 = create_session(autocommit=False)
        f1s2 = s2.query(Foo).with_lockmode('read').get(f1s1.id)
        assert f1s2.id == f1s1.id
        assert f1s2.value == f1s1.value

class UnicodeTest(_base.MappedTest):
    __requires__ = ('unicode_connections',)

    def define_tables(self, metadata):
        Table('uni_t1', metadata,
            Column('id',  Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('txt', sa.Unicode(50), unique=True))
        Table('uni_t2', metadata,
            Column('id',  Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('txt', sa.Unicode(50), ForeignKey('uni_t1')))

    def setup_classes(self):
        class Test(_base.BasicEntity):
            pass
        class Test2(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def test_basic(self):
        mapper(Test, uni_t1)

        txt = u"\u0160\u0110\u0106\u010c\u017d"
        t1 = Test(id=1, txt=txt)
        self.assert_(t1.txt == txt)

        session = create_session(autocommit=False)
        session.add(t1)
        session.commit()

        self.assert_(t1.txt == txt)

    @testing.resolve_artifact_names
    def test_relation(self):
        mapper(Test, uni_t1, properties={
            't2s': relation(Test2)})
        mapper(Test2, uni_t2)

        txt = u"\u0160\u0110\u0106\u010c\u017d"
        t1 = Test(txt=txt)
        t1.t2s.append(Test2())
        t1.t2s.append(Test2())
        session = create_session(autocommit=False)
        session.add(t1)
        session.commit()
        session.close()

        session = create_session()
        t1 = session.query(Test).filter_by(id=t1.id).one()
        assert len(t1.t2s) == 2

class UnicodeSchemaTest(engine_base.AltEngineTest, _base.MappedTest):
    __requires__ = ('unicode_connections', 'unicode_ddl',)

    def create_engine(self):
        return engines.utf8_engine()

    def define_tables(self, metadata):
        t1 = Table('unitable1', metadata,
              Column(u'méil', Integer, primary_key=True, key='a'),
              Column(u'\u6e2c\u8a66', Integer, key='b'),
              Column('type',  String(20)),
              test_needs_fk=True,
              test_needs_autoincrement=True)
        t2 = Table(u'Unitéble2', metadata,
              Column(u'méil', Integer, primary_key=True, key="cc"),
              Column(u'\u6e2c\u8a66', Integer,
                     ForeignKey(u'unitable1.a'), key="d"),
              Column(u'\u6e2c\u8a66_2', Integer, key="e"),
              test_needs_fk=True,
              test_needs_autoincrement=True)

        self.tables['t1'] = t1
        self.tables['t2'] = t2

    def setUpAll(self):
        engine_base.AltEngineTest.setUpAll(self)
        _base.MappedTest.setUpAll(self)

    def tearDownAll(self):
        _base.MappedTest.tearDownAll(self)
        engine_base.AltEngineTest.tearDownAll(self)

    @testing.resolve_artifact_names
    def test_mapping(self):
        class A(_base.ComparableEntity):
            pass
        class B(_base.ComparableEntity):
            pass

        mapper(A, t1, properties={
            't2s':relation(B)})
        mapper(B, t2)

        a1 = A()
        b1 = B()
        a1.t2s.append(b1)

        session = create_session()
        session.add(a1)
        session.flush()
        session.clear()

        new_a1 = session.query(A).filter(t1.c.a == a1.a).one()
        assert new_a1.a == a1.a
        assert new_a1.t2s[0].d == b1.d
        session.clear()

        new_a1 = (session.query(A).options(sa.orm.eagerload('t2s')).
                  filter(t1.c.a == a1.a)).one()
        assert new_a1.a == a1.a
        assert new_a1.t2s[0].d == b1.d
        session.clear()

        new_a1 = session.query(A).filter(A.a == a1.a).one()
        assert new_a1.a == a1.a
        assert new_a1.t2s[0].d == b1.d
        session.clear()

    @testing.resolve_artifact_names
    def test_inheritance_mapping(self):
        class A(_base.ComparableEntity):
            pass
        class B(A):
            pass

        mapper(A, t1,
               polymorphic_on=t1.c.type,
               polymorphic_identity='a')
        mapper(B, t2,
               inherits=A,
               polymorphic_identity='b')
        a1 = A(b=5)
        b1 = B(e=7)

        session = create_session()
        session.add_all((a1, b1))
        session.flush()
        session.clear()

        eq_([A(b=5), B(e=7)], session.query(A).all())


class MutableTypesTest(_base.MappedTest):

    def define_tables(self, metadata):
        Table('mutable_t', metadata,
            Column('id', Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('data', sa.PickleType),
            Column('val', sa.Unicode(30)))

    def setup_classes(self):
        class Foo(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def setup_mappers(self):
        mapper(Foo, mutable_t)

    @testing.resolve_artifact_names
    def test_basic(self):
        """Changes are detected for types marked as MutableType."""

        f1 = Foo()
        f1.data = pickleable.Bar(4,5)

        session = create_session()
        session.add(f1)
        session.flush()
        session.clear()

        f2 = session.query(Foo).filter_by(id=f1.id).one()
        assert 'data' in sa.orm.attributes.instance_state(f2).unmodified
        eq_(f2.data, f1.data)

        f2.data.y = 19
        assert f2 in session.dirty
        assert 'data' not in sa.orm.attributes.instance_state(f2).unmodified
        session.flush()
        session.clear()

        f3 = session.query(Foo).filter_by(id=f1.id).one()
        ne_(f3.data,f1.data)
        eq_(f3.data, pickleable.Bar(4, 19))

    @testing.resolve_artifact_names
    def test_mutable_changes(self):
        """Mutable changes are detected or not detected correctly"""

        f1 = Foo()
        f1.data = pickleable.Bar(4,5)
        f1.val = u'hi'

        session = create_session(autocommit=False)
        session.add(f1)
        session.commit()

        bind = self.metadata.bind

        self.sql_count_(0, session.commit)
        f1.val = u'someothervalue'
        self.assert_sql(bind, session.commit, [
            ("UPDATE mutable_t SET val=:val "
             "WHERE mutable_t.id = :mutable_t_id",
             {'mutable_t_id': f1.id, 'val': u'someothervalue'})])

        f1.val = u'hi'
        f1.data.x = 9
        self.assert_sql(bind, session.commit, [
            ("UPDATE mutable_t SET data=:data, val=:val "
             "WHERE mutable_t.id = :mutable_t_id",
             {'mutable_t_id': f1.id, 'val': u'hi', 'data':f1.data})])

    @testing.resolve_artifact_names
    def test_nocomparison(self):
        """Changes are detected on MutableTypes lacking an __eq__ method."""

        f1 = Foo()
        f1.data = pickleable.BarWithoutCompare(4,5)
        session = create_session(autocommit=False)
        session.add(f1)
        session.commit()

        self.sql_count_(0, session.commit)
        session.close()

        session = create_session(autocommit=False)
        f2 = session.query(Foo).filter_by(id=f1.id).one()
        self.sql_count_(0, session.commit)

        f2.data.y = 19
        self.sql_count_(1, session.commit)
        session.close()

        session = create_session(autocommit=False)
        f3 = session.query(Foo).filter_by(id=f1.id).one()
        eq_((f3.data.x, f3.data.y), (4,19))
        self.sql_count_(0, session.commit)
        session.close()

    @testing.resolve_artifact_names
    def test_unicode(self):
        """Equivalent Unicode values are not flagged as changed."""

        f1 = Foo(val=u'hi')

        session = create_session(autocommit=False)
        session.add(f1)
        session.commit()
        session.clear()

        f1 = session.get(Foo, f1.id)
        f1.val = u'hi'
        self.sql_count_(0, session.commit)


class PickledDicts(_base.MappedTest):

    def define_tables(self, metadata):
        Table('mutable_t', metadata,
            Column('id', Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('data', sa.PickleType(comparator=operator.eq)))

    def setup_classes(self):
        class Foo(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def setup_mappers(self):
        mapper(Foo, mutable_t)

    @testing.resolve_artifact_names
    def test_dicts(self):
        """Dictionaries may not pickle the same way twice."""

        f1 = Foo()
        f1.data = [ {
            'personne': {'nom': u'Smith',
                         'pers_id': 1,
                         'prenom': u'john',
                         'civilite': u'Mr',
                         'int_3': False,
                         'int_2': False,
                         'int_1': u'23',
                         'VenSoir': True,
                         'str_1': u'Test',
                         'SamMidi': False,
                         'str_2': u'chien',
                         'DimMidi': False,
                         'SamSoir': True,
                         'SamAcc': False} } ]

        session = create_session(autocommit=False)
        session.add(f1)
        session.commit()

        self.sql_count_(0, session.commit)

        f1.data = [ {
            'personne': {'nom': u'Smith',
                         'pers_id': 1,
                         'prenom': u'john',
                         'civilite': u'Mr',
                         'int_3': False,
                         'int_2': False,
                         'int_1': u'23',
                         'VenSoir': True,
                         'str_1': u'Test',
                         'SamMidi': False,
                         'str_2': u'chien',
                         'DimMidi': False,
                         'SamSoir': True,
                         'SamAcc': False} } ]

        self.sql_count_(0, session.commit)

        f1.data[0]['personne']['VenSoir']= False
        self.sql_count_(1, session.commit)

        session.clear()
        f = session.query(Foo).get(f1.id)
        eq_(f.data,
            [ {
            'personne': {'nom': u'Smith',
                         'pers_id': 1,
                         'prenom': u'john',
                         'civilite': u'Mr',
                         'int_3': False,
                         'int_2': False,
                         'int_1': u'23',
                         'VenSoir': False,
                         'str_1': u'Test',
                         'SamMidi': False,
                         'str_2': u'chien',
                         'DimMidi': False,
                         'SamSoir': True,
                         'SamAcc': False} } ])


class PKTest(_base.MappedTest):

    def define_tables(self, metadata):
        Table('multipk1', metadata,
              Column('multi_id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('multi_rev', Integer, primary_key=True),
              Column('name', String(50), nullable=False),
              Column('value', String(100)))

        Table('multipk2', metadata,
              Column('pk_col_1', String(30), primary_key=True),
              Column('pk_col_2', String(30), primary_key=True),
              Column('data', String(30)))
        Table('multipk3', metadata,
              Column('pri_code', String(30), key='primary', primary_key=True),
              Column('sec_code', String(30), key='secondary', primary_key=True),
              Column('date_assigned', sa.Date, key='assigned', primary_key=True),
              Column('data', String(30)))

    def setup_classes(self):
        class Entry(_base.BasicEntity):
            pass

    # not supported on sqlite since sqlite's auto-pk generation only works with
    # single column primary keys
    @testing.fails_on('sqlite')
    @testing.resolve_artifact_names
    def test_primary_key(self):
        mapper(Entry, multipk1)

        e = Entry(name='entry1', value='this is entry 1', multi_rev=2)

        session = create_session()
        session.add(e)
        session.flush()
        session.clear()

        e2 = session.query(Entry).get((e.multi_id, 2))
        self.assert_(e is not e2)
        state = sa.orm.attributes.instance_state(e)
        state2 = sa.orm.attributes.instance_state(e2)
        eq_(state.key, state2.key)

    # this one works with sqlite since we are manually setting up pk values
    @testing.resolve_artifact_names
    def test_manual_pk(self):
        mapper(Entry, multipk2)

        e = Entry(pk_col_1='pk1', pk_col_2='pk1_related', data='im the data')

        session = create_session()
        session.add(e)
        session.flush()

    @testing.resolve_artifact_names
    def test_key_pks(self):
        mapper(Entry, multipk3)

        e = Entry(primary= 'pk1', secondary='pk2',
                   assigned=datetime.date.today(), data='some more data')

        session = create_session()
        session.add(e)
        session.flush()


class ForeignPKTest(_base.MappedTest):
    """Detection of the relationship direction on PK joins."""

    def define_tables(self, metadata):
        Table("people", metadata,
              Column('person', String(10), primary_key=True),
              Column('firstname', String(10)),
              Column('lastname', String(10)))

        Table("peoplesites", metadata,
              Column('person', String(10), ForeignKey("people.person"),
                     primary_key=True),
              Column('site', String(10)))

    def setup_classes(self):
        class Person(_base.BasicEntity):
            pass
        class PersonSite(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def test_basic(self):
        m1 = mapper(PersonSite, peoplesites)
        m2 = mapper(Person, people, properties={
            'sites' : relation(PersonSite)})

        sa.orm.compile_mappers()
        eq_(list(m2.get_property('sites').foreign_keys),
            [peoplesites.c.person])

        p = Person(person='im the key', firstname='asdf')
        ps = PersonSite(site='asdf')
        p.sites.append(ps)

        session = create_session()
        session.add(p)
        session.flush()

        p_count = people.count(people.c.person=='im the key').scalar()
        eq_(p_count, 1)
        eq_(peoplesites.count(peoplesites.c.person=='im the key').scalar(), 1)


class ClauseAttributesTest(_base.MappedTest):

    def define_tables(self, metadata):
        Table('users_t', metadata,
            Column('id', Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('name', String(30)),
            Column('counter', Integer, default=1))

    def setup_classes(self):
        class User(_base.ComparableEntity):
            pass

    @testing.resolve_artifact_names
    def setup_mappers(self):
        mapper(User, users_t)

    @testing.resolve_artifact_names
    def test_update(self):
        u = User(name='test')

        session = create_session()
        session.add(u)
        session.flush()

        eq_(u.counter, 1)
        u.counter = User.counter + 1
        session.flush()

        def go():
            assert (u.counter == 2) is True  # ensure its not a ClauseElement
        self.sql_count_(1, go)

    @testing.resolve_artifact_names
    def test_multi_update(self):
        u = User(name='test')

        session = create_session()
        session.add(u)
        session.flush()

        eq_(u.counter, 1)
        u.name = 'test2'
        u.counter = User.counter + 1
        session.flush()

        def go():
            eq_(u.name, 'test2')
            assert (u.counter == 2) is True
        self.sql_count_(1, go)

        session.clear()
        u = session.query(User).get(u.id)
        eq_(u.name, 'test2')
        eq_(u.counter,  2)

    @testing.unsupported('mssql', 'FIXME: unknown, verify not fails_on()')
    @testing.resolve_artifact_names
    def test_insert(self):
        u = User(name='test', counter=sa.select([5]))

        session = create_session()
        session.add(u)
        session.flush()

        assert (u.counter == 5) is True


class PassiveDeletesTest(_base.MappedTest):
    __requires__ = ('foreign_keys',)

    def define_tables(self, metadata):
        Table('mytable', metadata,
              Column('id', Integer, primary_key=True),
              Column('data', String(30)),
              test_needs_fk=True)

        Table('myothertable', metadata,
              Column('id', Integer, primary_key=True),
              Column('parent_id', Integer),
              Column('data', String(30)),
              sa.ForeignKeyConstraint(['parent_id'],
                                      ['mytable.id'],
                                      ondelete="CASCADE"),
              test_needs_fk=True)

    def setup_classes(self):
        class MyClass(_base.BasicEntity):
            pass
        class MyOtherClass(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def setup_mappers(self):
        mapper(MyOtherClass, myothertable)
        mapper(MyClass, mytable, properties={
            'children':relation(MyOtherClass,
                                passive_deletes=True,
                                cascade="all")})

    @testing.resolve_artifact_names
    def test_basic(self):
        session = create_session()
        mc = MyClass()
        mc.children.append(MyOtherClass())
        mc.children.append(MyOtherClass())
        mc.children.append(MyOtherClass())
        mc.children.append(MyOtherClass())

        session.add(mc)
        session.flush()
        session.clear()

        assert myothertable.count().scalar() == 4
        mc = session.query(MyClass).get(mc.id)
        session.delete(mc)
        session.flush()

        assert mytable.count().scalar() == 0
        assert myothertable.count().scalar() == 0

class ExtraPassiveDeletesTest(_base.MappedTest):
    __requires__ = ('foreign_keys',)

    def define_tables(self, metadata):
        Table('mytable', metadata,
              Column('id', Integer, primary_key=True),
              Column('data', String(30)),
              test_needs_fk=True)

        Table('myothertable', metadata,
              Column('id', Integer, primary_key=True),
              Column('parent_id', Integer),
              Column('data', String(30)),
              # no CASCADE, the same as ON DELETE RESTRICT
              sa.ForeignKeyConstraint(['parent_id'],
                                      ['mytable.id']),
              test_needs_fk=True)

    def setup_classes(self):
        class MyClass(_base.BasicEntity):
            pass
        class MyOtherClass(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def test_assertions(self):
        mapper(MyOtherClass, myothertable)
        try:
            mapper(MyClass, mytable, properties={
                'children':relation(MyOtherClass,
                                    passive_deletes='all',
                                    cascade="all")})
            assert False
        except sa.exc.ArgumentError, e:
            eq_(str(e),
                "Can't set passive_deletes='all' in conjunction with 'delete' "
                "or 'delete-orphan' cascade")

    @testing.resolve_artifact_names
    def test_extra_passive(self):
        mapper(MyOtherClass, myothertable)
        mapper(MyClass, mytable, properties={
            'children': relation(MyOtherClass,
                                 passive_deletes='all',
                                 cascade="save-update")})

        session = create_session()
        mc = MyClass()
        mc.children.append(MyOtherClass())
        mc.children.append(MyOtherClass())
        mc.children.append(MyOtherClass())
        mc.children.append(MyOtherClass())
        session.add(mc)
        session.flush()
        session.clear()

        assert myothertable.count().scalar() == 4
        mc = session.query(MyClass).get(mc.id)
        session.delete(mc)
        self.assertRaises(sa.exc.DBAPIError, session.flush)

    @testing.resolve_artifact_names
    def test_extra_passive_2(self):
        mapper(MyOtherClass, myothertable)
        mapper(MyClass, mytable, properties={
            'children': relation(MyOtherClass,
                                 passive_deletes='all',
                                 cascade="save-update")})

        session = create_session()
        mc = MyClass()
        mc.children.append(MyOtherClass())
        session.add(mc)
        session.flush()
        session.clear()

        assert myothertable.count().scalar() == 1

        mc = session.query(MyClass).get(mc.id)
        session.delete(mc)
        mc.children[0].data = 'some new data'
        self.assertRaises(sa.exc.DBAPIError, session.flush)


class DefaultTest(_base.MappedTest):
    """Exercise mappings on columns with DefaultGenerators.

    Tests that when saving objects whose table contains DefaultGenerators,
    either python-side, preexec or database-side, the newly saved instances
    receive all the default values either through a post-fetch or getting the
    pre-exec'ed defaults back from the engine.

    """

    def define_tables(self, metadata):
        use_string_defaults = testing.against('postgres', 'oracle', 'sqlite')

        if use_string_defaults:
            hohotype = String(30)
            hohoval = "im hoho"
            althohoval = "im different hoho"
        else:
            hohotype = Integer
            hohoval = 9
            althohoval = 15

        self.other_artifacts['hohoval'] = hohoval
        self.other_artifacts['althohoval'] = althohoval

        dt = Table('default_t', metadata,
            Column('id', Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('hoho', hohotype, sa.PassiveDefault(str(hohoval))),
            Column('counter', Integer, default=sa.func.length("1234567")),
            Column('foober', String(30), default="im foober",
                   onupdate="im the update"))

        st = Table('secondary_table', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(50)))

        if testing.against('postgres', 'oracle'):
            dt.append_column(
                Column('secondary_id', Integer, sa.Sequence('sec_id_seq'),
                       unique=True))
            st.append_column(
                Column('fk_val', Integer,
                       ForeignKey('default_t.secondary_id')))
        else:
            st.append_column(
                Column('hoho', hohotype, ForeignKey('default_t.hoho')))

    def setup_classes(self):
        class Hoho(_base.ComparableEntity):
            pass
        class Secondary(_base.ComparableEntity):
            pass

    @testing.resolve_artifact_names
    def test_insert(self):
        mapper(Hoho, default_t)

        h1 = Hoho(hoho=althohoval)
        h2 = Hoho(counter=12)
        h3 = Hoho(hoho=althohoval, counter=12)
        h4 = Hoho()
        h5 = Hoho(foober='im the new foober')

        session = create_session(autocommit=False)
        session.add_all((h1, h2, h3, h4, h5))
        session.commit()

        eq_(h1.hoho, althohoval)
        eq_(h3.hoho, althohoval)

        def go():
            # test deferred load of attribues, one select per instance
            self.assert_(h2.hoho == h4.hoho == h5.hoho == hohoval)
        self.sql_count_(3, go)

        def go():
            self.assert_(h1.counter == h4.counter == h5.counter == 7)
        self.sql_count_(1, go)

        def go():
            self.assert_(h3.counter == h2.counter == 12)
            self.assert_(h2.foober == h3.foober == h4.foober == 'im foober')
            self.assert_(h5.foober == 'im the new foober')
        self.sql_count_(0, go)

        session.clear()

        (h1, h2, h3, h4, h5) = session.query(Hoho).order_by(Hoho.id).all()

        eq_(h1.hoho, althohoval)
        eq_(h3.hoho, althohoval)
        self.assert_(h2.hoho == h4.hoho == h5.hoho == hohoval)
        self.assert_(h3.counter == h2.counter == 12)
        self.assert_(h1.counter ==  h4.counter == h5.counter == 7)
        self.assert_(h2.foober == h3.foober == h4.foober == 'im foober')
        eq_(h5.foober, 'im the new foober')

    @testing.resolve_artifact_names
    def test_eager_defaults(self):
        mapper(Hoho, default_t, eager_defaults=True)

        h1 = Hoho()

        session = create_session()
        session.add(h1)
        session.flush()

        self.sql_count_(0, lambda: eq_(h1.hoho, hohoval))

    @testing.resolve_artifact_names
    def test_insert_nopostfetch(self):
        # populates the PassiveDefaults explicitly so there is no
        # "post-update"
        mapper(Hoho, default_t)

        h1 = Hoho(hoho="15", counter="15")
        session = create_session()
        session.add(h1)
        session.flush()

        def go():
            eq_(h1.hoho, "15")
            eq_(h1.counter, "15")
            eq_(h1.foober, "im foober")
        self.sql_count_(0, go)

    @testing.resolve_artifact_names
    def test_update(self):
        mapper(Hoho, default_t)

        h1 = Hoho()
        session = create_session()
        session.add(h1)
        session.flush()

        eq_(h1.foober, 'im foober')
        h1.counter = 19
        session.flush()
        eq_(h1.foober, 'im the update')

    @testing.resolve_artifact_names
    def test_used_in_relation(self):
        """A server-side default can be used as the target of a foreign key"""

        mapper(Hoho, default_t, properties={
            'secondaries':relation(Secondary)})
        mapper(Secondary, secondary_table)

        h1 = Hoho()
        s1 = Secondary(data='s1')
        h1.secondaries.append(s1)

        session = create_session()
        session.add(h1)
        session.flush()
        session.clear()

        eq_(session.query(Hoho).get(h1.id),
            Hoho(hoho=hohoval,
                 secondaries=[
                   Secondary(data='s1')]))

        h1 = session.query(Hoho).get(h1.id)
        h1.secondaries.append(Secondary(data='s2'))
        session.flush()
        session.clear()

        eq_(session.query(Hoho).get(h1.id),
            Hoho(hoho=hohoval,
                 secondaries=[
                    Secondary(data='s1'),
                    Secondary(data='s2')]))


class OneToManyTest(_fixtures.FixtureTest):
    run_inserts = None

    @testing.resolve_artifact_names
    def test_one_to_many_1(self):
        """Basic save of one to many."""

        m = mapper(User, users, properties=dict(
            addresses = relation(mapper(Address, addresses), lazy=True)
        ))
        u = User(name= 'one2manytester')
        a = Address(email_address='one2many@test.org')
        u.addresses.append(a)

        a2 = Address(email_address='lala@test.org')
        u.addresses.append(a2)

        session = create_session()
        session.add(u)
        session.flush()

        user_rows = users.select(users.c.id.in_([u.id])).execute().fetchall()
        eq_(user_rows[0].values(), [u.id, 'one2manytester'])

        address_rows = addresses.select(
            addresses.c.id.in_([a.id, a2.id]),
            order_by=[addresses.c.email_address]).execute().fetchall()
        eq_(address_rows[0].values(), [a2.id, u.id, 'lala@test.org'])
        eq_(address_rows[1].values(), [a.id, u.id, 'one2many@test.org'])

        userid = u.id
        addressid = a2.id

        a2.email_address = 'somethingnew@foo.com'

        session.flush()

        address_rows = addresses.select(
            addresses.c.id == addressid).execute().fetchall()
        eq_(address_rows[0].values(),
            [addressid, userid, 'somethingnew@foo.com'])
        self.assert_(u.id == userid and a2.id == addressid)

    @testing.resolve_artifact_names
    def test_one_to_many_2(self):
        """Modifying the child items of an object."""

        m = mapper(User, users, properties=dict(
            addresses = relation(mapper(Address, addresses), lazy=True)))

        u1 = User(name='user1')
        u1.addresses = []
        a1 = Address(email_address='emailaddress1')
        u1.addresses.append(a1)

        u2 = User(name='user2')
        u2.addresses = []
        a2 = Address(email_address='emailaddress2')
        u2.addresses.append(a2)

        a3 = Address(email_address='emailaddress3')

        session = create_session()
        session.add_all((u1, u2, a3))
        session.flush()

        # modify user2 directly, append an address to user1.
        # upon commit, user2 should be updated, user1 should not
        # both address1 and address3 should be updated
        u2.name = 'user2modified'
        u1.addresses.append(a3)
        del u1.addresses[0]

        self.assert_sql(testing.db, session.flush, [
            ("UPDATE users SET name=:name "
             "WHERE users.id = :users_id",
             {'users_id': u2.id, 'name': 'user2modified'}),

            ("UPDATE addresses SET user_id=:user_id "
             "WHERE addresses.id = :email_addresses_id",
             {'user_id': None, 'addresses_id': a1.id}),

            ("UPDATE addresses SET user_id=:user_id "
             "WHERE addresses.id = :addresses_id",
             {'user_id': u1.id, 'addresses_id': a3.id})])

    @testing.resolve_artifact_names
    def test_child_move(self):
        """Moving a child from one parent to another, with a delete.

        Tests that deleting the first parent properly updates the child with
        the new parent.  This tests the 'trackparent' option in the attributes
        module.

        """
        m = mapper(User, users, properties=dict(
            addresses = relation(mapper(Address, addresses), lazy=True)))

        u1 = User(name='user1')
        u2 = User(name='user2')
        a = Address(email_address='address1')
        u1.addresses.append(a)

        session = create_session()
        session.add_all((u1, u2))
        session.flush()

        del u1.addresses[0]
        u2.addresses.append(a)
        session.delete(u1)

        session.flush()
        session.clear()

        u2 = session.get(User, u2.id)
        eq_(len(u2.addresses), 1)

    @testing.resolve_artifact_names
    def test_child_move_2(self):
        m = mapper(User, users, properties=dict(
            addresses = relation(mapper(Address, addresses), lazy=True)))

        u1 = User(name='user1')
        u2 = User(name='user2')
        a = Address(email_address='address1')
        u1.addresses.append(a)

        session = create_session()
        session.add_all((u1, u2))
        session.flush()

        del u1.addresses[0]
        u2.addresses.append(a)

        session.flush()
        session.clear()

        u2 = session.get(User, u2.id)
        eq_(len(u2.addresses), 1)

    @testing.resolve_artifact_names
    def test_o2m_delete_parent(self):
        m = mapper(User, users, properties=dict(
            address = relation(mapper(Address, addresses),
                               lazy=True,
                               uselist=False)))

        u = User(name='one2onetester')
        a = Address(email_address='myonlyaddress@foo.com')
        u.address = a

        session = create_session()
        session.add(u)
        session.flush()

        session.delete(u)
        session.flush()

        assert a.id is not None
        assert a.user_id is None
        assert sa.orm.attributes.instance_state(a).key in session.identity_map
        assert sa.orm.attributes.instance_state(u).key not in session.identity_map

    @testing.resolve_artifact_names
    def test_one_to_one(self):
        m = mapper(User, users, properties=dict(
            address = relation(mapper(Address, addresses),
                               lazy=True,
                               uselist=False)))

        u = User(name='one2onetester')
        u.address = Address(email_address='myonlyaddress@foo.com')

        session = create_session()
        session.add(u)
        session.flush()

        u.name = 'imnew'
        session.flush()

        u.address.email_address = 'imnew@foo.com'
        session.flush()

    @testing.resolve_artifact_names
    def test_bidirectional(self):
        m1 = mapper(User, users)
        m2 = mapper(Address, addresses, properties=dict(
            user = relation(m1, lazy=False, backref='addresses')))


        u = User(name='test')
        a = Address(email_address='testaddress', user=u)

        session = create_session()
        session.add(u)
        session.flush()
        session.delete(u)
        session.flush()

    @testing.resolve_artifact_names
    def test_double_relation(self):
        m2 = mapper(Address, addresses)
        m = mapper(User, users, properties={
            'boston_addresses' : relation(m2, primaryjoin=
                        sa.and_(users.c.id==addresses.c.user_id,
                                addresses.c.email_address.like('%boston%'))),
            'newyork_addresses' : relation(m2, primaryjoin=
                        sa.and_(users.c.id==addresses.c.user_id,
                                addresses.c.email_address.like('%newyork%')))})

        u = User(name='u1')
        a = Address(email_address='foo@boston.com')
        b = Address(email_address='bar@newyork.com')
        u.boston_addresses.append(a)
        u.newyork_addresses.append(b)

        session = create_session()
        session.add(u)
        session.flush()

class SaveTest(_fixtures.FixtureTest):
    run_inserts = None

    @testing.resolve_artifact_names
    def test_basic(self):
        m = mapper(User, users)

        # save two users
        u = User(name='savetester')
        u2 = User(name='savetester2')

        session = create_session()
        session.add_all((u, u2))
        session.flush()

        # assert the first one retreives the same from the identity map
        nu = session.get(m, u.id)
        assert u is nu

        # clear out the identity map, so next get forces a SELECT
        session.clear()

        # check it again, identity should be different but ids the same
        nu = session.get(m, u.id)
        assert u is not nu and u.id == nu.id and nu.name == 'savetester'

        # change first users name and save
        session = create_session()
        session.update(u)
        u.name = 'modifiedname'
        assert u in session.dirty
        session.flush()

        # select both
        userlist = session.query(User).filter(
            users.c.id.in_([u.id, u2.id])).order_by([users.c.name]).all()

        eq_(u.id, userlist[0].id)
        eq_(userlist[0].name, 'modifiedname')
        eq_(u2.id, userlist[1].id)
        eq_(userlist[1].name, 'savetester2')

    @testing.resolve_artifact_names
    def test_synonym(self):
        class SUser(_base.BasicEntity):
            def _get_name(self):
                return "User:" + self.name
            def _set_name(self, name):
                self.name = name + ":User"
            syn_name = property(_get_name, _set_name)

        mapper(SUser, users, properties={
            'syn_name': sa.orm.synonym('name')
        })

        u = SUser(syn_name="some name")
        eq_(u.syn_name, 'User:some name:User')

        session = create_session()
        session.add(u)
        session.flush()
        session.clear()

        u = session.query(SUser).first()
        eq_(u.syn_name, 'User:some name:User')

    @testing.resolve_artifact_names
    def test_lazyattr_commit(self):
        """Lazily loaded relations.

        When a lazy-loaded list is unloaded, and a commit occurs, that the
        'passive' call on that list does not blow away its value

        """
        mapper(User, users, properties = {
            'addresses': relation(mapper(Address, addresses))})

        u = User(name='u1')
        u.addresses.append(Address(email_address='u1@e1'))
        u.addresses.append(Address(email_address='u1@e2'))
        u.addresses.append(Address(email_address='u1@e3'))
        u.addresses.append(Address(email_address='u1@e4'))

        session = create_session()
        session.add(u)
        session.flush()
        session.clear()

        u = session.query(User).one()
        u.name = 'newname'
        session.flush()
        eq_(len(u.addresses), 4)

    @testing.resolve_artifact_names
    def test_inherits(self):
        m1 = mapper(User, users)

        class AddressUser(User):
            """a user object that also has the users mailing address."""
            pass

        # define a mapper for AddressUser that inherits the User.mapper, and
        # joins on the id column
        mapper(AddressUser, addresses, inherits=m1)

        au = AddressUser(name='u', email_address='u@e')

        session = create_session()
        session.add(au)
        session.flush()
        session.clear()

        rt = session.query(AddressUser).one()
        eq_(au.user_id, rt.user_id)
        eq_(rt.id, rt.id)

    @testing.resolve_artifact_names
    def test_deferred(self):
        """Deferred column operations"""

        mapper(Order, orders, properties={
            'description': sa.orm.deferred(orders.c.description)})

        # dont set deferred attribute, commit session
        o = Order(id=42)
        session = create_session(autocommit=False)
        session.add(o)
        session.commit()

        # assert that changes get picked up
        o.description = 'foo'
        session.commit()

        eq_(list(session.execute(orders.select(), mapper=Order)),
            [(42, None, None, 'foo', None)])
        session.clear()

        # assert that a set operation doesn't trigger a load operation
        o = session.query(Order).filter(Order.description == 'foo').one()
        def go():
            o.description = 'hoho'
        self.sql_count_(0, go)
        session.flush()

        eq_(list(session.execute(orders.select(), mapper=Order)),
            [(42, None, None, 'hoho', None)])

        session.clear()

        # test assigning None to an unloaded deferred also works
        o = session.query(Order).filter(Order.description == 'hoho').one()
        o.description = None
        session.flush()
        eq_(list(session.execute(orders.select(), mapper=Order)),
            [(42, None, None, None, None)])
        session.close()

    # why no support on oracle ?  because oracle doesn't save
    # "blank" strings; it saves a single space character.
    @testing.fails_on('oracle')
    @testing.resolve_artifact_names
    def test_dont_update_blanks(self):
        mapper(User, users)

        u = User(name='')
        session = create_session()
        session.add(u)
        session.flush()
        session.clear()

        u = session.query(User).get(u.id)
        u.name = ''
        self.sql_count_(0, session.flush)

    @testing.resolve_artifact_names
    def test_multi_table_selectable(self):
        """Mapped selectables that span tables.

        Also tests redefinition of the keynames for the column properties.

        """
        usersaddresses = sa.join(users, addresses,
                                 users.c.id == addresses.c.user_id)

        m = mapper(User, usersaddresses,
            properties=dict(
                email = addresses.c.email_address,
                foo_id = [users.c.id, addresses.c.user_id]))

        u = User(name='multitester', email='multi@test.org')
        session = create_session()
        session.add(u)
        session.flush()
        session.clear()

        id = m.primary_key_from_instance(u)

        u = session.get(User, id)
        assert u.name == 'multitester'

        user_rows = users.select(users.c.id.in_([u.foo_id])).execute().fetchall()
        eq_(user_rows[0].values(), [u.foo_id, 'multitester'])
        address_rows = addresses.select(addresses.c.id.in_([u.id])).execute().fetchall()
        eq_(address_rows[0].values(), [u.id, u.foo_id, 'multi@test.org'])

        u.email = 'lala@hey.com'
        u.name = 'imnew'
        session.flush()

        user_rows = users.select(users.c.id.in_([u.foo_id])).execute().fetchall()
        eq_(user_rows[0].values(), [u.foo_id, 'imnew'])
        address_rows = addresses.select(addresses.c.id.in_([u.id])).execute().fetchall()
        eq_(address_rows[0].values(), [u.id, u.foo_id, 'lala@hey.com'])

        session.clear()
        u = session.get(User, id)
        assert u.name == 'imnew'

    @testing.resolve_artifact_names
    def test_history_get(self):
        """The history lazy-fetches data when it wasn't otherwise loaded."""
        mapper(User, users, properties={
            'addresses':relation(Address, cascade="all, delete-orphan")})
        mapper(Address, addresses)

        u = User(name='u1')
        u.addresses.append(Address(email_address='u1@e1'))
        u.addresses.append(Address(email_address='u1@e2'))
        session = create_session()
        session.add(u)
        session.flush()
        session.clear()

        u = session.query(User).get(u.id)
        session.delete(u)
        session.flush()
        assert users.count().scalar() == 0
        assert addresses.count().scalar() == 0

    @testing.resolve_artifact_names
    def test_batch_mode(self):
        """The 'batch=False' flag on mapper()"""

        class TestExtension(sa.orm.MapperExtension):
            def before_insert(self, mapper, connection, instance):
                self.current_instance = instance
            def after_insert(self, mapper, connection, instance):
                assert instance is self.current_instance

        mapper(User, users, extension=TestExtension(), batch=False)
        u1 = User(name='user1')
        u2 = User(name='user2')

        session = create_session()
        session.add_all((u1, u2))
        session.flush()
        session.clear()

        sa.orm.clear_mappers()

        m = mapper(User, users, extension=TestExtension())
        u1 = User(name='user1')
        u2 = User(name='user2')
        try:
            session.flush()
            assert False
        except AssertionError:
            assert True


class ManyToOneTest(_fixtures.FixtureTest):

    @testing.resolve_artifact_names
    def test_m2o_one_to_one(self):
        # TODO: put assertion in here !!!
        m = mapper(Address, addresses, properties=dict(
            user = relation(mapper(User, users), lazy=True, uselist=False)))

        session = create_session()

        data = [
            {'name': 'thesub' ,  'email_address': 'bar@foo.com'},
            {'name': 'assdkfj' , 'email_address': 'thesdf@asdf.com'},
            {'name': 'n4knd' ,   'email_address': 'asf3@bar.org'},
            {'name': 'v88f4' ,   'email_address': 'adsd5@llala.net'},
            {'name': 'asdf8d' ,  'email_address': 'theater@foo.com'}
        ]
        objects = []
        for elem in data:
            a = Address()
            a.email_address = elem['email_address']
            a.user = User()
            a.user.name = elem['name']
            objects.append(a)
            session.add(a)

        session.flush()

        objects[2].email_address = 'imnew@foo.bar'
        objects[3].user = User()
        objects[3].user.name = 'imnewlyadded'
        self.assert_sql(testing.db,
                        session.flush,
                        [
            ("INSERT INTO users (name) VALUES (:name)",
             {'name': 'imnewlyadded'} ),

            {"UPDATE addresses SET email_address=:email_address "
             "WHERE addresses.id = :addresses_id":
             lambda ctx: {'email_address': 'imnew@foo.bar',
                          'addresses_id': objects[2].id},
             "UPDATE addresses SET user_id=:user_id "
             "WHERE addresses.id = :addresses_id":
             lambda ctx: {'user_id': objects[3].user.id,
                          'addresses_id': objects[3].id}},
                        ],
                        with_sequences=[
            ("INSERT INTO users (id, name) VALUES (:id, :name)",
             lambda ctx:{'name': 'imnewlyadded',
                         'id':ctx.last_inserted_ids()[0]}),
            {"UPDATE addresses SET email_address=:email_address "
             "WHERE addresses.id = :addresses_id":
             lambda ctx: {'email_address': 'imnew@foo.bar',
                          'addresses_id': objects[2].id},
             ("UPDATE addresses SET user_id=:user_id "
              "WHERE addresses.id = :addresses_id"):
             lambda ctx: {'user_id': objects[3].user.id,
                          'addresses_id': objects[3].id}}])

        l = sa.select([users, addresses],
                      sa.and_(users.c.id==addresses.c.user_id,
                              addresses.c.id==a.id)).execute()
        eq_(l.fetchone().values(),
            [a.user.id, 'asdf8d', a.id, a.user_id, 'theater@foo.com'])

    @testing.resolve_artifact_names
    def test_many_to_one_1(self):
        m = mapper(Address, addresses, properties=dict(
            user = relation(mapper(User, users), lazy=True)))

        a1 = Address(email_address='emailaddress1')
        u1 = User(name='user1')
        a1.user = u1

        session = create_session()
        session.add(a1)
        session.flush()
        session.clear()

        a1 = session.query(Address).get(a1.id)
        u1 = session.query(User).get(u1.id)
        assert a1.user is u1

        a1.user = None
        session.flush()
        session.clear()
        a1 = session.query(Address).get(a1.id)
        u1 = session.query(User).get(u1.id)
        assert a1.user is None

    @testing.resolve_artifact_names
    def test_many_to_one_2(self):
        m = mapper(Address, addresses, properties=dict(
            user = relation(mapper(User, users), lazy=True)))

        a1 = Address(email_address='emailaddress1')
        a2 = Address(email_address='emailaddress2')
        u1 = User(name='user1')
        a1.user = u1

        session = create_session()
        session.add_all((a1, a2))
        session.flush()
        session.clear()

        a1 = session.query(Address).get(a1.id)
        a2 = session.query(Address).get(a2.id)
        u1 = session.query(User).get(u1.id)
        assert a1.user is u1

        a1.user = None
        a2.user = u1
        session.flush()
        session.clear()

        a1 = session.query(Address).get(a1.id)
        a2 = session.query(Address).get(a2.id)
        u1 = session.query(User).get(u1.id)
        assert a1.user is None
        assert a2.user is u1

    @testing.resolve_artifact_names
    def test_many_to_one_3(self):
        m = mapper(Address, addresses, properties=dict(
            user = relation(mapper(User, users), lazy=True)))

        a1 = Address(email_address='emailaddress1')
        u1 = User(name='user1')
        u2 = User(name='user2')
        a1.user = u1

        session = create_session()
        session.add_all((a1, u1, u2))
        session.flush()
        session.clear()

        a1 = session.query(Address).get(a1.id)
        u1 = session.query(User).get(u1.id)
        u2 = session.query(User).get(u2.id)
        assert a1.user is u1

        a1.user = u2
        session.flush()
        session.clear()
        a1 = session.query(Address).get(a1.id)
        u1 = session.query(User).get(u1.id)
        u2 = session.query(User).get(u2.id)
        assert a1.user is u2

    @testing.resolve_artifact_names
    def test_bidirectional_no_load(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref='user', lazy=None)})
        mapper(Address, addresses)

        # try it on unsaved objects
        u1 = User(name='u1')
        a1 = Address(email_address='e1')
        a1.user = u1

        session = create_session()
        session.add(u1)
        session.flush()
        session.clear()

        a1 = session.query(Address).get(a1.id)

        a1.user = None
        session.flush()
        session.clear()
        assert session.query(Address).get(a1.id).user is None
        assert session.query(User).get(u1.id).addresses == []


class ManyToManyTest(_fixtures.FixtureTest):
    run_inserts = None

    @testing.resolve_artifact_names
    def test_many_to_many(self):
        mapper(Keyword, keywords)

        m = mapper(Item, items, properties=dict(
                keywords=relation(Keyword,
                                  item_keywords,
                                  lazy=False,
                                  order_by=keywords.c.name)))

        data = [Item,
            {'description': 'mm_item1',
             'keywords' : (Keyword, [{'name': 'big'},
                                     {'name': 'green'},
                                     {'name': 'purple'},
                                     {'name': 'round'}])},
            {'description': 'mm_item2',
             'keywords' : (Keyword, [{'name':'blue'},
                                     {'name':'imnew'},
                                     {'name':'round'},
                                     {'name':'small'}])},
            {'description': 'mm_item3',
             'keywords' : (Keyword, [])},
            {'description': 'mm_item4',
             'keywords' : (Keyword, [{'name':'big'},
                                    {'name':'blue'},])},
            {'description': 'mm_item5',
             'keywords' : (Keyword, [{'name':'big'},
                                     {'name':'exacting'},
                                     {'name':'green'}])},
            {'description': 'mm_item6',
             'keywords' : (Keyword, [{'name':'red'},
                                     {'name':'round'},
                                     {'name':'small'}])}]

        _fixtures.run_inserts_for(keywords)
        session = create_session()

        objects = []
        _keywords = dict([(k.name, k) for k in session.query(Keyword)])

        for elem in data[1:]:
            item = Item(description=elem['description'])
            objects.append(item)

            for spec in elem['keywords'][1]:
                keyword_name = spec['name']
                try:
                    kw = _keywords[keyword_name]
                except KeyError:
                    _keywords[keyword_name] = kw = Keyword(name=keyword_name)
                item.keywords.append(kw)

        session.add_all(objects)
        session.flush()

        l = (session.query(Item).
             filter(Item.description.in_([e['description']
                                          for e in data[1:]])).
             order_by(Item.description).all())
        self.assert_result(l, *data)

        objects[4].description = 'item4updated'
        k = Keyword()
        k.name = 'yellow'
        objects[5].keywords.append(k)
        self.assert_sql(testing.db, session.flush, [
            {"UPDATE items SET description=:description "
             "WHERE items.id = :items_id":
             {'description': 'item4updated',
              'items_id': objects[4].id},
             "INSERT INTO keywords (name) "
             "VALUES (:name)":
             {'name': 'yellow'}},
            ("INSERT INTO item_keywords (item_id, keyword_id) "
             "VALUES (:item_id, :keyword_id)",
             lambda ctx: [{'item_id': objects[5].id,
                           'keyword_id': k.id}])],
                        with_sequences = [
              {"UPDATE items SET description=:description "
               "WHERE items.id = :items_id":
               {'description': 'item4updated',
                'items_id': objects[4].id},
               "INSERT INTO keywords (id, name) "
               "VALUES (:id, :name)":
               lambda ctx: {'name': 'yellow',
                            'id':ctx.last_inserted_ids()[0]}},
              ("INSERT INTO item_keywords (item_id, keyword_id) "
               "VALUES (:item_id, :keyword_id)",
               lambda ctx: [{'item_id': objects[5].id,
                             'keyword_id': k.id}])])

        objects[2].keywords.append(k)
        dkid = objects[5].keywords[1].id
        del objects[5].keywords[1]
        self.assert_sql(testing.db, session.flush, [
            ("DELETE FROM item_keywords "
             "WHERE item_keywords.item_id = :item_id AND "
             "item_keywords.keyword_id = :keyword_id",
             [{'item_id': objects[5].id, 'keyword_id': dkid}]),
            ("INSERT INTO item_keywords (item_id, keyword_id) "
             "VALUES (:item_id, :keyword_id)",
             lambda ctx: [{'item_id': objects[2].id, 'keyword_id': k.id}]
             )])

        session.delete(objects[3])
        session.flush()

    @testing.resolve_artifact_names
    def test_many_to_many_remove(self):
        """Setting a collection to empty deletes many-to-many rows.

        Tests that setting a list-based attribute to '[]' properly affects the
        history and allows the many-to-many rows to be deleted

        """
        mapper(Keyword, keywords)
        mapper(Item, items, properties=dict(
            keywords = relation(Keyword, item_keywords, lazy=False),
            ))

        i = Item(description='i1')
        k1 = Keyword(name='k1')
        k2 = Keyword(name='k2')
        i.keywords.append(k1)
        i.keywords.append(k2)

        session = create_session()
        session.add(i)
        session.flush()

        assert item_keywords.count().scalar() == 2
        i.keywords = []
        session.flush()
        assert item_keywords.count().scalar() == 0

    @testing.resolve_artifact_names
    def test_scalar(self):
        """sa.dependency won't delete an m2m relation referencing None."""

        mapper(Keyword, keywords)

        mapper(Item, items, properties=dict(
            keyword=relation(Keyword, secondary=item_keywords, uselist=False)))

        i = Item(description='x')
        session = create_session()
        session.add(i)
        session.flush()
        session.delete(i)
        session.flush()

    @testing.resolve_artifact_names
    def test_many_to_many_update(self):
        """Assorted history operations on a many to many"""
        mapper(Keyword, keywords)
        mapper(Item, items, properties=dict(
            keywords=relation(Keyword,
                              secondary=item_keywords,
                              lazy=False,
                              order_by=keywords.c.name)))

        k1 = Keyword(name='keyword 1')
        k2 = Keyword(name='keyword 2')
        k3 = Keyword(name='keyword 3')

        item = Item(description='item 1')
        item.keywords.extend([k1, k2, k3])

        session = create_session()
        session.add(item)
        session.flush()

        item.keywords = []
        item.keywords.append(k1)
        item.keywords.append(k2)
        session.flush()

        session.clear()
        item = session.query(Item).get(item.id)
        assert item.keywords == [k1, k2]

    @testing.resolve_artifact_names
    def test_association(self):
        """Basic test of an association object"""

        class IKAssociation(_base.ComparableEntity):
            pass

        mapper(Keyword, keywords)

        # note that we are breaking a rule here, making a second
        # mapper(Keyword, keywords) the reorganization of mapper construction
        # affected this, but was fixed again

        mapper(IKAssociation, item_keywords,
               primary_key=[item_keywords.c.item_id, item_keywords.c.keyword_id],
               properties=dict(
                 keyword=relation(mapper(Keyword, keywords, non_primary=True),
                                  lazy=False,
                                  uselist=False,
                                  order_by=keywords.c.name)))

        mapper(Item, items, properties=dict(
            keywords=relation(IKAssociation, lazy=False)))

        _fixtures.run_inserts_for(keywords)
        session = create_session()

        def fixture():
            _kw = dict([(k.name, k) for k in session.query(Keyword)])
            for n in ('big', 'green', 'purple', 'round', 'huge',
                      'violet', 'yellow', 'blue'):
                if n not in _kw:
                    _kw[n] = Keyword(name=n)

            def assocs(*names):
                return [IKAssociation(keyword=kw)
                        for kw in [_kw[n] for n in names]]

            return [
                Item(description='a_item1',
                     keywords=assocs('big', 'green', 'purple', 'round')),
                Item(description='a_item2',
                     keywords=assocs('huge', 'violet', 'yellow')),
                Item(description='a_item3',
                     keywords=assocs('big', 'blue'))]

        session.add_all(fixture())
        session.flush()
        eq_(fixture(), session.query(Item).order_by(Item.description).all())


class SaveTest2(_fixtures.FixtureTest):
    run_inserts = None

    @testing.resolve_artifact_names
    def test_m2o_nonmatch(self):
        mapper(User, users)
        mapper(Address, addresses, properties=dict(
            user = relation(User, lazy=True, uselist=False)))

        session = create_session()

        def fixture():
            return [
                Address(email_address='a1', user=User(name='u1')),
                Address(email_address='a2', user=User(name='u2'))]

        session.add_all(fixture())

        self.assert_sql(testing.db, session.flush, [
            ("INSERT INTO users (name) VALUES (:name)",
             {'name': 'u1'}),
            ("INSERT INTO users (name) VALUES (:name)",
             {'name': 'u2'}),
            ("INSERT INTO addresses (user_id, email_address) "
             "VALUES (:user_id, :email_address)",
             {'user_id': 1, 'email_address': 'a1'}),
            ("INSERT INTO addresses (user_id, email_address) "
             "VALUES (:user_id, :email_address)",
             {'user_id': 2, 'email_address': 'a2'})],
            with_sequences = [
            ("INSERT INTO users (id, name) "
             "VALUES (:id, :name)",
             lambda ctx: {'name': 'u1', 'id':ctx.last_inserted_ids()[0]}),
            ("INSERT INTO users (id, name) "
             "VALUES (:id, :name)",
             lambda ctx: {'name': 'u2', 'id':ctx.last_inserted_ids()[0]}),
            ("INSERT INTO addresses (id, user_id, email_address) "
             "VALUES (:id, :user_id, :email_address)",
             lambda ctx:{'user_id': 1, 'email_address': 'a1',
                         'id':ctx.last_inserted_ids()[0]}),
            ("INSERT INTO addresses (id, user_id, email_address) "
             "VALUES (:id, :user_id, :email_address)",
             lambda ctx:{'user_id': 2, 'email_address': 'a2',
                         'id':ctx.last_inserted_ids()[0]})])


class SaveTest3(_base.MappedTest):
    def define_tables(self, metadata):
        Table('items', metadata,
              Column('item_id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('item_name', String(50)))

        Table('keywords', metadata,
              Column('keyword_id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(50)))

        Table('assoc', metadata,
              Column('item_id', Integer, ForeignKey("items")),
              Column('keyword_id', Integer, ForeignKey("keywords")),
              Column('foo', sa.Boolean, default=True))

    def setup_classes(self):
        class Keyword(_base.BasicEntity):
            pass
        class Item(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def test_manytomany_xtracol_delete(self):
        """A many-to-many on a table that has an extra column can properly delete rows from the table without referencing the extra column"""

        mapper(Keyword, keywords)
        mapper(Item, items, properties=dict(
                keywords = relation(Keyword, secondary=assoc, lazy=False),))

        i = Item()
        k1 = Keyword()
        k2 = Keyword()
        i.keywords.append(k1)
        i.keywords.append(k2)

        session = create_session()
        session.add(i)
        session.flush()

        assert assoc.count().scalar() == 2
        i.keywords = []
        print i.keywords
        session.flush()
        assert assoc.count().scalar() == 0

class BooleanColTest(_base.MappedTest):
    def define_tables(self, metadata):
        Table('t1_t', metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(30)),
            Column('value', sa.Boolean))

    @testing.resolve_artifact_names
    def test_boolean(self):
        # use the regular mapper
        class T(_base.ComparableEntity):
            pass
        orm_mapper(T, t1_t, order_by=t1_t.c.id)

        sess = create_session()
        t1 = T(value=True, name="t1")
        t2 = T(value=False, name="t2")
        t3 = T(value=True, name="t3")
        sess.save(t1)
        sess.save(t2)
        sess.save(t3)

        sess.flush()

        for clear in (False, True):
            if clear:
                sess.clear()
            eq_(sess.query(T).all(), [T(value=True, name="t1"), T(value=False, name="t2"), T(value=True, name="t3")])
            if clear:
                sess.clear()
            eq_(sess.query(T).filter(T.value==True).all(), [T(value=True, name="t1"),T(value=True, name="t3")])
            if clear:
                sess.clear()
            eq_(sess.query(T).filter(T.value==False).all(), [T(value=False, name="t2")])

        t2 = sess.query(T).get(t2.id)
        t2.value = True
        sess.flush()
        eq_(sess.query(T).filter(T.value==True).all(), [T(value=True, name="t1"), T(value=True, name="t2"), T(value=True, name="t3")])
        t2.value = False
        sess.flush()
        eq_(sess.query(T).filter(T.value==True).all(), [T(value=True, name="t1"),T(value=True, name="t3")])


class RowSwitchTest(_base.MappedTest):
    def define_tables(self, metadata):
        # parent
        Table('t1', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30), nullable=False))

        # onetomany
        Table('t2', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30), nullable=False),
            Column('t1id', Integer, ForeignKey('t1.id'),nullable=False))

        # associated
        Table('t3', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30), nullable=False))

        #manytomany
        Table('t1t3', metadata,
            Column('t1id', Integer, ForeignKey('t1.id'),nullable=False),
            Column('t3id', Integer, ForeignKey('t3.id'),nullable=False))

    def setup_classes(self):
        class T1(_base.ComparableEntity):
            pass

        class T2(_base.ComparableEntity):
            pass

        class T3(_base.ComparableEntity):
            pass

    @testing.resolve_artifact_names
    def test_onetomany(self):
        mapper(T1, t1, properties={
            't2s':relation(T2, cascade="all, delete-orphan")
        })
        mapper(T2, t2)

        sess = create_session()

        o1 = T1(data='some t1', id=1)
        o1.t2s.append(T2(data='some t2', id=1))
        o1.t2s.append(T2(data='some other t2', id=2))

        sess.save(o1)
        sess.flush()

        assert list(sess.execute(t1.select(), mapper=T1)) == [(1, 'some t1')]
        assert list(sess.execute(t2.select(), mapper=T1)) == [(1, 'some t2', 1), (2, 'some other t2', 1)]

        o2 = T1(data='some other t1', id=o1.id, t2s=[
            T2(data='third t2', id=3),
            T2(data='fourth t2', id=4),
            ])
        sess.delete(o1)
        sess.save(o2)
        sess.flush()

        assert list(sess.execute(t1.select(), mapper=T1)) == [(1, 'some other t1')]
        assert list(sess.execute(t2.select(), mapper=T1)) == [(3, 'third t2', 1), (4, 'fourth t2', 1)]

    @testing.resolve_artifact_names
    def test_manytomany(self):
        mapper(T1, t1, properties={
            't3s':relation(T3, secondary=t1t3, cascade="all, delete-orphan")
        })
        mapper(T3, t3)

        sess = create_session()

        o1 = T1(data='some t1', id=1)
        o1.t3s.append(T3(data='some t3', id=1))
        o1.t3s.append(T3(data='some other t3', id=2))

        sess.save(o1)
        sess.flush()

        assert list(sess.execute(t1.select(), mapper=T1)) == [(1, 'some t1')]
        assert testing.rowset(sess.execute(t1t3.select(), mapper=T1)) == set([(1,1), (1, 2)])
        assert list(sess.execute(t3.select(), mapper=T1)) == [(1, 'some t3'), (2, 'some other t3')]

        o2 = T1(data='some other t1', id=1, t3s=[
            T3(data='third t3', id=3),
            T3(data='fourth t3', id=4),
            ])
        sess.delete(o1)
        sess.save(o2)
        sess.flush()

        assert list(sess.execute(t1.select(), mapper=T1)) == [(1, 'some other t1')]
        assert list(sess.execute(t3.select(), mapper=T1)) == [(3, 'third t3'), (4, 'fourth t3')]

    @testing.resolve_artifact_names
    def test_manytoone(self):

        mapper(T2, t2, properties={
            't1':relation(T1)
        })
        mapper(T1, t1)

        sess = create_session()

        o1 = T2(data='some t2', id=1)
        o1.t1 = T1(data='some t1', id=1)

        sess.save(o1)
        sess.flush()

        assert list(sess.execute(t1.select(), mapper=T1)) == [(1, 'some t1')]
        assert list(sess.execute(t2.select(), mapper=T1)) == [(1, 'some t2', 1)]

        o2 = T2(data='some other t2', id=1, t1=T1(data='some other t1', id=2))
        sess.delete(o1)
        sess.delete(o1.t1)
        sess.save(o2)
        sess.flush()

        assert list(sess.execute(t1.select(), mapper=T1)) == [(2, 'some other t1')]
        assert list(sess.execute(t2.select(), mapper=T1)) == [(1, 'some other t2', 2)]

class TransactionTest(_base.MappedTest):
    __requires__ = ('deferrable_constraints',)

    __whitelist__ = ('sqlite',)
    # sqlite doesn't have deferrable constraints, but it allows them to
    # be specified.  it'll raise immediately post-INSERT, instead of at
    # COMMIT. either way, this test should pass.

    def define_tables(self, metadata):
        t1 = Table('t1', metadata,
            Column('id', Integer, primary_key=True))

        t2 = Table('t2', metadata,
            Column('id', Integer, primary_key=True),
            Column('t1_id', Integer,
                   ForeignKey('t1.id', deferrable=True, initially='deferred')
                   ))
    def setup_classes(self):
        class T1(_base.ComparableEntity):
            pass

        class T2(_base.ComparableEntity):
            pass

    @testing.resolve_artifact_names
    def setup_mappers(self):
        orm_mapper(T1, t1)
        orm_mapper(T2, t2)

    @testing.resolve_artifact_names
    def test_close_transaction_on_commit_fail(self):
        session = create_session(autocommit=True)

        # with a deferred constraint, this fails at COMMIT time instead
        # of at INSERT time.
        session.add(T2(t1_id=123))

        try:
            session.flush()
            assert False
        except:
            # Flush needs to rollback also when commit fails
            assert session.transaction is None

        # todo: on 8.3 at least, the failed commit seems to close the cursor?
        # needs investigation.  leaving in the DDL above now to help verify
        # that the new deferrable support on FK isn't involved in this issue.
        if testing.against('postgres'):
            t1.bind.engine.dispose()

if __name__ == "__main__":
    testenv.main()
