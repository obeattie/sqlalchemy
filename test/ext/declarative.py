import testenv; testenv.configure_for_tests()

from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base, declared_synonym, \
                                       synonym_for, comparable_using
from sqlalchemy import exceptions
from testlib.fixtures import Base as Fixture
from testlib import *


class DeclarativeTest(TestBase, AssertsExecutionResults):
    def setUp(self):
        global Base
        Base = declarative_base(testing.db)

    def tearDown(self):
        Base.metadata.drop_all()

    def test_basic(self):
        class User(Base, Fixture):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            addresses = relation("Address", backref="user")

        class Address(Base, Fixture):
            __tablename__ = 'addresses'

            id = Column(Integer, primary_key=True)
            email = Column(String(50), key='_email')
            user_id = Column('user_id', Integer, ForeignKey('users.id'),
                             key='_user_id')

        Base.metadata.create_all()

        assert Address.__table__.c['id'].name == 'id'
        assert Address.__table__.c['_email'].name == 'email'
        assert Address.__table__.c['_user_id'].name == 'user_id'

        u1 = User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
        ])
        sess = create_session()
        sess.save(u1)
        sess.flush()
        sess.clear()

        self.assertEquals(sess.query(User).all(), [User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
        ])])

        a1 = sess.query(Address).filter(Address.email=='two').one()
        self.assertEquals(a1, Address(email='two'))
        self.assertEquals(a1.user, User(name='u1'))

    def test_nice_dependency_error(self):
        class User(Base):
            __tablename__ = 'users'
            id = Column('id', Integer, primary_key=True)
            addresses = relation("Address")
        
        def go():
            class Address(Base):
                __tablename__ = 'addresses'

                id = Column(Integer, primary_key=True)
                foo = column_property(User.id==5)
        self.assertRaises(exceptions.InvalidRequestError, go)
        
    def test_add_prop(self):
        class User(Base, Fixture):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
        User.name = Column('name', String(50))
        User.addresses = relation("Address", backref="user")

        class Address(Base, Fixture):
            __tablename__ = 'addresses'

            id = Column(Integer, primary_key=True)
        Address.email = Column(String(50), key='_email')
        Address.user_id = Column('user_id', Integer, ForeignKey('users.id'),
                             key='_user_id')

        Base.metadata.create_all()

        assert Address.__table__.c['id'].name == 'id'
        assert Address.__table__.c['_email'].name == 'email'
        assert Address.__table__.c['_user_id'].name == 'user_id'

        u1 = User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
        ])
        sess = create_session()
        sess.save(u1)
        sess.flush()
        sess.clear()

        self.assertEquals(sess.query(User).all(), [User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
        ])])

        a1 = sess.query(Address).filter(Address.email=='two').one()
        self.assertEquals(a1, Address(email='two'))
        self.assertEquals(a1.user, User(name='u1'))


    @testing.emits_warning('Ignoring declarative-like tuple value of '
                           'attribute id')
    def test_oops(self):
        def define():
            class User(Base, Fixture):
                __tablename__ = 'users'

                id = Column('id', Integer, primary_key=True),
                name = Column('name', String(50))
            assert False
        self.assertRaisesMessage(
            exceptions.ArgumentError,
            "Mapper Mapper|User|users could not assemble any primary key",
            define)

    def test_expression(self):
        class User(Base, Fixture):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            addresses = relation("Address", backref="user")

        class Address(Base, Fixture):
            __tablename__ = 'addresses'

            id = Column('id', Integer, primary_key=True)
            email = Column('email', String(50))
            user_id = Column('user_id', Integer, ForeignKey('users.id'))

        User.address_count = column_property(select([func.count(Address.id)]).where(Address.user_id==User.id).as_scalar())

        Base.metadata.create_all()

        u1 = User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
        ])
        sess = create_session()
        sess.save(u1)
        sess.flush()
        sess.clear()

        self.assertEquals(sess.query(User).all(), [User(name='u1', address_count=2, addresses=[
            Address(email='one'),
            Address(email='two'),
        ])])

    def test_column(self):
        class User(Base, Fixture):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))

        User.a = Column('a', String(10))
        User.b = Column(String(10))

        Base.metadata.create_all()

        u1 = User(name='u1', a='a', b='b')
        assert u1.a == 'a'
        assert User.a.get_history(u1) == (['a'], [], [])
        sess = create_session()
        sess.save(u1)
        sess.flush()
        sess.clear()

        self.assertEquals(sess.query(User).all(),
                          [User(name='u1', a='a', b='b')])

    def test_column_properties(self):
        
        class Address(Base, Fixture):
            __tablename__ = 'addresses'
            id = Column(Integer, primary_key=True)
            email = Column(String(50))
            user_id = Column(Integer, ForeignKey('users.id'))
            
        class User(Base, Fixture):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            adr_count = column_property(select([func.count(Address.id)], Address.user_id==id).as_scalar())
            addresses = relation(Address)
        
        Base.metadata.create_all()
        
        u1 = User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
        ])
        sess = create_session()
        sess.save(u1)
        sess.flush()
        sess.clear()

        self.assertEquals(sess.query(User).all(), [User(name='u1', adr_count=2, addresses=[
            Address(email='one'),
            Address(email='two'),
        ])])

    def test_column_properties_2(self):

        class Address(Base, Fixture):
            __tablename__ = 'addresses'
            id = Column(Integer, primary_key=True)
            email = Column(String(50))
            user_id = Column(Integer, ForeignKey('users.id'))

        class User(Base, Fixture):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            # this is not "valid" but we want to test that Address.id doesnt get stuck into user's table
            adr_count = Address.id
            
        self.assertEquals(set(User.__table__.c.keys()), set(['id', 'name']))
        self.assertEquals(set(Address.__table__.c.keys()), set(['id', 'email', 'user_id']))
        
    def test_deferred(self):
        class User(Base, Fixture):
            __tablename__ = 'users'

            id = Column(Integer, primary_key=True)
            name = deferred(Column(String(50)))
            
        Base.metadata.create_all()
        sess = create_session()
        sess.save(User(name='u1'))
        sess.flush()
        sess.clear()
        
        u1 = sess.query(User).filter(User.name=='u1').one()
        assert 'name' not in u1.__dict__
        def go():
            assert u1.name == 'u1'
        self.assert_sql_count(testing.db, go, 1)
        
    def test_synonym_inline(self):
        class User(Base, Fixture):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            _name = Column('name', String(50))
            def _set_name(self, name):
                self._name = "SOMENAME " + name
            def _get_name(self):
                return self._name
            name = synonym('_name', descriptor=property(_get_name, _set_name))

        Base.metadata.create_all()

        sess = create_session()
        u1 = User(name='someuser')
        assert u1.name == "SOMENAME someuser", u1.name
        sess.save(u1)
        sess.flush()
        self.assertEquals(sess.query(User).filter(User.name=="SOMENAME someuser").one(), u1)

    @testing.uses_deprecated('Call to deprecated function declared_synonym')
    def test_decl_synonym_inline(self):
        class User(Base, Fixture):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            _name = Column('name', String(50))
            def _set_name(self, name):
                self._name = "SOMENAME " + name
            def _get_name(self):
                return self._name
            name = declared_synonym(property(_get_name, _set_name), '_name')

        Base.metadata.create_all()

        sess = create_session()
        u1 = User(name='someuser')
        assert u1.name == "SOMENAME someuser", u1.name
        sess.save(u1)
        sess.flush()
        self.assertEquals(sess.query(User).filter(User.name=="SOMENAME someuser").one(), u1)

    def test_synonym_added(self):
        class User(Base, Fixture):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            _name = Column('name', String(50))
            def _set_name(self, name):
                self._name = "SOMENAME " + name
            def _get_name(self):
                return self._name
            name = property(_get_name, _set_name)
        User.name = synonym('_name', descriptor=User.name)

        Base.metadata.create_all()

        sess = create_session()
        u1 = User(name='someuser')
        assert u1.name == "SOMENAME someuser", u1.name
        sess.save(u1)
        sess.flush()
        self.assertEquals(sess.query(User).filter(User.name=="SOMENAME someuser").one(), u1)

    @testing.uses_deprecated('Call to deprecated function declared_synonym')
    def test_decl_synonym_added(self):
        class User(Base, Fixture):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            _name = Column('name', String(50))
            def _set_name(self, name):
                self._name = "SOMENAME " + name
            def _get_name(self):
                return self._name
            name = property(_get_name, _set_name)
        User.name = declared_synonym(User.name, '_name')

        Base.metadata.create_all()

        sess = create_session()
        u1 = User(name='someuser')
        assert u1.name == "SOMENAME someuser", u1.name
        sess.save(u1)
        sess.flush()
        self.assertEquals(sess.query(User).filter(User.name=="SOMENAME someuser").one(), u1)

    def test_joined_inheritance(self):
        class Company(Base, Fixture):
            __tablename__ = 'companies'
            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            employees = relation("Person")

        class Person(Base, Fixture):
            __tablename__ = 'people'
            id = Column('id', Integer, primary_key=True)
            company_id = Column('company_id', Integer, ForeignKey('companies.id'))
            name = Column('name', String(50))
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on':discriminator}

        class Engineer(Person):
            __tablename__ = 'engineers'
            __mapper_args__ = {'polymorphic_identity':'engineer'}
            id = Column('id', Integer, ForeignKey('people.id'), primary_key=True)
            primary_language = Column('primary_language', String(50))

        class Manager(Person):
            __tablename__ = 'managers'
            __mapper_args__ = {'polymorphic_identity':'manager'}
            id = Column('id', Integer, ForeignKey('people.id'), primary_key=True)
            golf_swing = Column('golf_swing', String(50))

        Base.metadata.create_all()

        sess = create_session()
        c1 = Company(name="MegaCorp, Inc.", employees=[
            Engineer(name="dilbert", primary_language="java"),
            Engineer(name="wally", primary_language="c++"),
            Manager(name="dogbert", golf_swing="fore!")
        ])

        c2 = Company(name="Elbonia, Inc.", employees=[
            Engineer(name="vlad", primary_language="cobol")
        ])

        sess.save(c1)
        sess.save(c2)
        sess.flush()
        sess.clear()

        self.assertEquals(sess.query(Company).filter(Company.employees.of_type(Engineer).any(Engineer.primary_language=='cobol')).first(), c2)

    def test_relation_reference(self):
        class Address(Base, Fixture):
            __tablename__ = 'addresses'

            id = Column('id', Integer, primary_key=True)
            email = Column('email', String(50))
            user_id = Column('user_id', Integer, ForeignKey('users.id'))

        class User(Base, Fixture):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            addresses = relation("Address", backref="user",
                                 primaryjoin=id==Address.user_id)

        User.address_count = column_property(select([func.count(Address.id)]).where(Address.user_id==User.id).as_scalar())

        Base.metadata.create_all()

        u1 = User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
        ])
        sess = create_session()
        sess.save(u1)
        sess.flush()
        sess.clear()

        self.assertEquals(sess.query(User).all(), [User(name='u1', address_count=2, addresses=[
            Address(email='one'),
            Address(email='two'),
        ])])

    def test_single_inheritance(self):
        class Company(Base, Fixture):
            __tablename__ = 'companies'
            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            employees = relation("Person")

        class Person(Base, Fixture):
            __tablename__ = 'people'
            id = Column('id', Integer, primary_key=True)
            company_id = Column('company_id', Integer, ForeignKey('companies.id'))
            name = Column('name', String(50))
            discriminator = Column('type', String(50))
            primary_language = Column('primary_language', String(50))
            golf_swing = Column('golf_swing', String(50))
            __mapper_args__ = {'polymorphic_on':discriminator}

        class Engineer(Person):
            __mapper_args__ = {'polymorphic_identity':'engineer'}

        class Manager(Person):
            __mapper_args__ = {'polymorphic_identity':'manager'}

        Base.metadata.create_all()

        sess = create_session()
        c1 = Company(name="MegaCorp, Inc.", employees=[
            Engineer(name="dilbert", primary_language="java"),
            Engineer(name="wally", primary_language="c++"),
            Manager(name="dogbert", golf_swing="fore!")
        ])

        c2 = Company(name="Elbonia, Inc.", employees=[
            Engineer(name="vlad", primary_language="cobol")
        ])

        sess.save(c1)
        sess.save(c2)
        sess.flush()
        sess.clear()

        self.assertEquals(sess.query(Person).filter(Engineer.primary_language=='cobol').first(), Engineer(name='vlad'))
        self.assertEquals(sess.query(Company).filter(Company.employees.of_type(Engineer).any(Engineer.primary_language=='cobol')).first(), c2)

    def test_with_explicit_autoloaded(self):
        meta = MetaData(testing.db)
        t1 = Table('t1', meta, Column('id', String(50), primary_key=True), Column('data', String(50)))
        meta.create_all()
        try:
            class MyObj(Base):
                __table__ = Table('t1', Base.metadata, autoload=True)

            sess = create_session()
            m = MyObj(id="someid", data="somedata")
            sess.save(m)
            sess.flush()

            assert t1.select().execute().fetchall() == [('someid', 'somedata')]
        finally:
            meta.drop_all()


class DeclarativeReflectionTest(TestBase):
    def setUpAll(self):
        global reflection_metadata
        reflection_metadata = MetaData(testing.db)

        Table('users', reflection_metadata,
              Column('id', Integer, primary_key=True),
              Column('name', String(50)),
              test_needs_fk=True)
        Table('addresses', reflection_metadata,
              Column('id', Integer, primary_key=True),
              Column('email', String(50)),
              Column('user_id', Integer, ForeignKey('users.id')),
              test_needs_fk=True)
        Table('imhandles', reflection_metadata,
              Column('id', Integer, primary_key=True),
              Column('user_id', Integer),
              Column('network', String(50)),
              Column('handle', String(50)),
              test_needs_fk=True)

        reflection_metadata.create_all()

    def setUp(self):
        global Base
        Base = declarative_base(testing.db)

    def tearDown(self):
        for t in reflection_metadata.table_iterator():
            t.delete().execute()

    def tearDownAll(self):
        reflection_metadata.drop_all()

    def test_basic(self):
        meta = MetaData(testing.db)

        class User(Base, Fixture):
            __tablename__ = 'users'
            __autoload__ = True
            addresses = relation("Address", backref="user")

        class Address(Base, Fixture):
            __tablename__ = 'addresses'
            __autoload__ = True

        u1 = User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
            ])
        sess = create_session()
        sess.save(u1)
        sess.flush()
        sess.clear()

        self.assertEquals(sess.query(User).all(), [User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
            ])])

        a1 = sess.query(Address).filter(Address.email=='two').one()
        self.assertEquals(a1, Address(email='two'))
        self.assertEquals(a1.user, User(name='u1'))

    def test_rekey(self):
        meta = MetaData(testing.db)

        class User(Base, Fixture):
            __tablename__ = 'users'
            __autoload__ = True
            nom = Column('name', String(50), key='nom')
            addresses = relation("Address", backref="user")

        class Address(Base, Fixture):
            __tablename__ = 'addresses'
            __autoload__ = True

        u1 = User(nom='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
            ])
        sess = create_session()
        sess.save(u1)
        sess.flush()
        sess.clear()

        self.assertEquals(sess.query(User).all(), [User(nom='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
            ])])

        a1 = sess.query(Address).filter(Address.email=='two').one()
        self.assertEquals(a1, Address(email='two'))
        self.assertEquals(a1.user, User(nom='u1'))

        self.assertRaises(TypeError, User, name='u3')

    def test_supplied_fk(self):
        meta = MetaData(testing.db)

        class IMHandle(Base, Fixture):
            __tablename__ = 'imhandles'
            __autoload__ = True

            user_id = Column('user_id', Integer,
                             ForeignKey('users.id'))
        class User(Base, Fixture):
            __tablename__ = 'users'
            __autoload__ = True
            handles = relation("IMHandle", backref="user")

        u1 = User(name='u1', handles=[
            IMHandle(network='blabber', handle='foo'),
            IMHandle(network='lol', handle='zomg')
            ])
        sess = create_session()
        sess.save(u1)
        sess.flush()
        sess.clear()

        self.assertEquals(sess.query(User).all(), [User(name='u1', handles=[
            IMHandle(network='blabber', handle='foo'),
            IMHandle(network='lol', handle='zomg')
            ])])

        a1 = sess.query(IMHandle).filter(IMHandle.handle=='zomg').one()
        self.assertEquals(a1, IMHandle(network='lol', handle='zomg'))
        self.assertEquals(a1.user, User(name='u1'))

    def test_synonym_for(self):
        class User(Base, Fixture):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))

            @synonym_for('name')
            @property
            def namesyn(self):
                return self.name

        Base.metadata.create_all()

        sess = create_session()
        u1 = User(name='someuser')
        assert u1.name == "someuser", u1.name
        assert u1.namesyn == 'someuser', u1.namesyn
        sess.save(u1)
        sess.flush()

        rt = sess.query(User).filter(User.namesyn=='someuser').one()
        self.assertEquals(rt, u1)

    def test_comparable_using(self):
        class NameComparator(PropComparator):
            @property
            def upperself(self):
                cls = self.prop.parent.class_
                col = getattr(cls, 'name')
                return func.upper(col)

            def operate(self, op, other, **kw):
                return op(self.upperself, other, **kw)

        class User(Base, Fixture):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))

            @comparable_using(NameComparator)
            @property
            def uc_name(self):
                return self.name is not None and self.name.upper() or None

        Base.metadata.create_all()

        sess = create_session()
        u1 = User(name='someuser')
        assert u1.name == "someuser", u1.name
        assert u1.uc_name == 'SOMEUSER', u1.uc_name
        sess.save(u1)
        sess.flush()
        sess.clear()

        rt = sess.query(User).filter(User.uc_name=='SOMEUSER').one()
        self.assertEquals(rt, u1)
        sess.clear()

        rt = sess.query(User).filter(User.uc_name.startswith('SOMEUSE')).one()
        self.assertEquals(rt, u1)

if __name__ == '__main__':
    testing.main()
