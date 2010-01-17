from sqlalchemy.test.testing import eq_
import pickle
import sqlalchemy as sa
from sqlalchemy.test import testing
from sqlalchemy.test.testing import assert_raises_message
from sqlalchemy import Integer, String, ForeignKey, exc
from sqlalchemy.test.schema import Table, Column
from sqlalchemy.orm import mapper, relation, create_session, \
                            sessionmaker, attributes, interfaces,\
                            clear_mappers, exc as orm_exc
from test.orm import _base, _fixtures


User, EmailUser = None, None

class PickleTest(_fixtures.FixtureTest):
    run_inserts = None
    
    @testing.resolve_artifact_names
    def test_transient(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref="user")
        })
        mapper(Address, addresses)

        sess = create_session()
        u1 = User(name='ed')
        u1.addresses.append(Address(email_address='ed@bar.com'))

        u2 = pickle.loads(pickle.dumps(u1))
        sess.add(u2)
        sess.flush()

        sess.expunge_all()

        eq_(u1, sess.query(User).get(u2.id))

    @testing.resolve_artifact_names
    def test_no_mappers(self):
        
        umapper = mapper(User, users)
        u1 = User(name='ed')
        u1_pickled = pickle.dumps(u1, -1)

        clear_mappers()

        assert_raises_message(
            orm_exc.UnmappedInstanceError,
            "Cannot deserialize object of type <class 'test.orm._fixtures.User'> - no mapper()",
            pickle.loads, u1_pickled)
        
    @testing.resolve_artifact_names
    def test_serialize_path(self):
        umapper = mapper(User, users, properties={
            'addresses':relation(Address, backref="user")
        })
        amapper = mapper(Address, addresses)
        
        # this is a "relation" path with mapper, key, mapper, key
        p1 = (umapper, 'addresses', amapper, 'email_address')
        eq_(
            interfaces.deserialize_path(interfaces.serialize_path(p1)),
            p1
        )
        
        # this is a "mapper" path with mapper, key, mapper, no key
        # at the end.
        p2 = (umapper, 'addresses', amapper, )
        eq_(
            interfaces.deserialize_path(interfaces.serialize_path(p2)),
            p2
        )
        
    @testing.resolve_artifact_names
    def test_class_deferred_cols(self):
        mapper(User, users, properties={
            'name':sa.orm.deferred(users.c.name),
            'addresses':relation(Address, backref="user")
        })
        mapper(Address, addresses, properties={
            'email_address':sa.orm.deferred(addresses.c.email_address)
        })
        sess = create_session()
        u1 = User(name='ed')
        u1.addresses.append(Address(email_address='ed@bar.com'))
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        u1 = sess.query(User).get(u1.id)
        assert 'name' not in u1.__dict__
        assert 'addresses' not in u1.__dict__

        u2 = pickle.loads(pickle.dumps(u1))
        sess2 = create_session()
        sess2.add(u2)
        eq_(u2.name, 'ed')
        eq_(u2, User(name='ed', addresses=[Address(email_address='ed@bar.com')]))

        u2 = pickle.loads(pickle.dumps(u1))
        sess2 = create_session()
        u2 = sess2.merge(u2, load=False)
        eq_(u2.name, 'ed')
        eq_(u2, User(name='ed', addresses=[Address(email_address='ed@bar.com')]))

    @testing.resolve_artifact_names
    def test_instance_deferred_cols(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref="user")
        })
        mapper(Address, addresses)

        sess = create_session()
        u1 = User(name='ed')
        u1.addresses.append(Address(email_address='ed@bar.com'))
        sess.add(u1)
        sess.flush()
        sess.expunge_all()

        u1 = sess.query(User).\
                options(sa.orm.defer('name'), 
                        sa.orm.defer('addresses.email_address')).\
                        get(u1.id)
        assert 'name' not in u1.__dict__
        assert 'addresses' not in u1.__dict__

        u2 = pickle.loads(pickle.dumps(u1))
        sess2 = create_session()
        sess2.add(u2)
        eq_(u2.name, 'ed')
        assert 'addresses' not in u2.__dict__
        ad = u2.addresses[0]
        assert 'email_address' not in ad.__dict__
        eq_(ad.email_address, 'ed@bar.com')
        eq_(u2, User(name='ed', addresses=[Address(email_address='ed@bar.com')]))

        u2 = pickle.loads(pickle.dumps(u1))
        sess2 = create_session()
        u2 = sess2.merge(u2, load=False)
        eq_(u2.name, 'ed')
        assert 'addresses' not in u2.__dict__
        ad = u2.addresses[0]
        
        # mapper options now transmit over merge(),
        # new as of 0.6, so email_address is deferred.
        assert 'email_address' not in ad.__dict__  
        
        eq_(ad.email_address, 'ed@bar.com')
        eq_(u2, User(name='ed', addresses=[Address(email_address='ed@bar.com')]))

    @testing.resolve_artifact_names
    def test_pickle_protocols(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref="user")
        })
        mapper(Address, addresses)

        sess = sessionmaker()()
        u1 = User(name='ed')
        u1.addresses.append(Address(email_address='ed@bar.com'))
        sess.add(u1)
        sess.commit()

        u1 = sess.query(User).first()
        u1.addresses
        for protocol in -1, 0, 1, 2:
            u2 = pickle.loads(pickle.dumps(u1, protocol))
            eq_(u1, u2)
        
    @testing.resolve_artifact_names
    def test_options_with_descriptors(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref="user")
        })
        mapper(Address, addresses)
        sess = create_session()
        u1 = User(name='ed')
        u1.addresses.append(Address(email_address='ed@bar.com'))
        sess.add(u1)
        sess.flush()
        sess.expunge_all()

        for opt in [
            sa.orm.eagerload(User.addresses),
            sa.orm.eagerload("addresses"),
            sa.orm.defer("name"),
            sa.orm.defer(User.name),
            sa.orm.eagerload("addresses", User.addresses),
        ]:
            opt2 = pickle.loads(pickle.dumps(opt))
            eq_(opt.key, opt2.key)
        
        u1 = sess.query(User).options(opt).first()
        
        u2 = pickle.loads(pickle.dumps(u1))
        
        
class PolymorphicDeferredTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('name', String(30)),
            Column('type', String(30)))
        Table('email_users', metadata,
            Column('id', Integer, ForeignKey('users.id'), primary_key=True),
            Column('email_address', String(30)))

    @classmethod
    def setup_classes(cls):
        global User, EmailUser
        class User(_base.BasicEntity):
            pass

        class EmailUser(User):
            pass

    @classmethod
    def teardown_class(cls):
        global User, EmailUser
        User, EmailUser = None, None
        super(PolymorphicDeferredTest, cls).teardown_class()

    @testing.resolve_artifact_names
    def test_polymorphic_deferred(self):
        mapper(User, users, polymorphic_identity='user', polymorphic_on=users.c.type)
        mapper(EmailUser, email_users, inherits=User, polymorphic_identity='emailuser')

        eu = EmailUser(name="user1", email_address='foo@bar.com')
        sess = create_session()
        sess.add(eu)
        sess.flush()
        sess.expunge_all()

        eu = sess.query(User).first()
        eu2 = pickle.loads(pickle.dumps(eu))
        sess2 = create_session()
        sess2.add(eu2)
        assert 'email_address' not in eu2.__dict__
        eq_(eu2.email_address, 'foo@bar.com')

class CustomSetupTeardownTest(_fixtures.FixtureTest):
    @testing.resolve_artifact_names
    def test_rebuild_state(self):
        """not much of a 'test', but illustrate how to 
        remove instance-level state before pickling.
        
        """
        mapper(User, users)

        u1 = User()
        attributes.manager_of_class(User).teardown_instance(u1)
        assert not u1.__dict__
        u2 = pickle.loads(pickle.dumps(u1))
        attributes.manager_of_class(User).setup_instance(u2)
        assert attributes.instance_state(u2)
    
