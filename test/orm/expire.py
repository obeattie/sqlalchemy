"""test attribute/instance expiration, deferral of attributes, etc."""

import testbase
from sqlalchemy import *
from sqlalchemy import exceptions
from sqlalchemy.orm import *
from testlib import *
from testlib.fixtures import *

class ExpireTest(FixtureTest):
    keep_mappers = False
    refresh_data = True
    
    def test_expire(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref='user'),
            })
        mapper(Address, addresses)
            
        sess = create_session()
        u = sess.query(User).get(7)
        assert len(u.addresses) == 1
        u.name = 'foo'
        del u.addresses[0]
        sess.expire(u)
        
        assert 'name' not in u.__dict__
        
        def go():
            assert u.name == 'jack'
        self.assert_sql_count(testbase.db, go, 1)
        assert 'name' in u.__dict__

        # we're changing the database here, so if this test fails in the middle,
        # it'll screw up the other tests which are hardcoded to 7/'jack'
        u.name = 'foo'
        sess.flush()
        # change the value in the DB
        users.update(users.c.id==7, values=dict(name='jack')).execute()
        sess.expire(u)
        # object isnt refreshed yet, using dict to bypass trigger
        assert u.__dict__.get('name') != 'jack'
        # reload all
        sess.query(User).all()
        # test that it refreshed
        assert u.__dict__['name'] == 'jack'

        # object should be back to normal now,
        # this should *not* produce a SELECT statement (not tested here though....)
        assert u.name == 'jack'
    
    def test_expire_doesntload_on_set(self):
        mapper(User, users)
        
        sess = create_session()
        u = sess.query(User).get(7)
        
        sess.expire(u, attribute_names=['name'])
        def go():
            u.name = 'somenewname'
        self.assert_sql_count(testbase.db, go, 0)
        sess.flush()
        sess.clear()
        assert sess.query(User).get(7).name == 'somenewname'
        
    def test_expire_committed(self):
        """test that the committed state of the attribute receives the most recent DB data"""
        mapper(Order, orders)
            
        sess = create_session()
        o = sess.query(Order).get(3)
        sess.expire(o)

        orders.update(id=3).execute(description='order 3 modified')
        assert o.isopen == 1
        assert o._state.committed_state['description'] == 'order 3 modified'
        def go():
            sess.flush()
        self.assert_sql_count(testbase.db, go, 0)
        
    def test_expire_cascade(self):
        mapper(User, users, properties={
            'addresses':relation(Address, cascade="all, refresh-expire")
        })
        mapper(Address, addresses)
        s = create_session()
        u = s.get(User, 8)
        assert u.addresses[0].email_address == 'ed@wood.com'

        u.addresses[0].email_address = 'someotheraddress'
        s.expire(u)
        u.name
        print u._state.dict
        assert u.addresses[0].email_address == 'ed@wood.com'

    def test_expired_lazy(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref='user'),
            })
        mapper(Address, addresses)

        sess = create_session()
        u = sess.query(User).get(7)

        sess.expire(u)
        assert 'name' not in u.__dict__
        assert 'addresses' not in u.__dict__

        def go():
            assert u.addresses[0].email_address == 'jack@bean.com'
            assert u.name == 'jack'
        # two loads 
        self.assert_sql_count(testbase.db, go, 2)
        assert 'name' in u.__dict__
        assert 'addresses' in u.__dict__

    def test_expired_eager(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref='user', lazy=False),
            })
        mapper(Address, addresses)

        sess = create_session()
        u = sess.query(User).get(7)

        sess.expire(u)
        assert 'name' not in u.__dict__
        assert 'addresses' not in u.__dict__

        def go():
            assert u.addresses[0].email_address == 'jack@bean.com'
            assert u.name == 'jack'
        # one load
        self.assert_sql_count(testbase.db, go, 1)
        assert 'name' in u.__dict__
        assert 'addresses' in u.__dict__

    def test_partial_expire(self):
        mapper(Order, orders)

        sess = create_session()
        o = sess.query(Order).get(3)
        
        sess.expire(o, attribute_names=['description'])
        assert 'id' in o.__dict__
        assert 'description' not in o.__dict__
        assert o._state.committed_state['isopen'] == 1
        
        orders.update(orders.c.id==3).execute(description='order 3 modified')
        
        def go():
            assert o.description == 'order 3 modified'
        self.assert_sql_count(testbase.db, go, 1)
        assert o._state.committed_state['description'] == 'order 3 modified'
        
        o.isopen = 5
        sess.expire(o, attribute_names=['description'])
        assert 'id' in o.__dict__
        assert 'description' not in o.__dict__
        assert o.__dict__['isopen'] == 5
        assert o._state.committed_state['isopen'] == 1
        
        def go():
            assert o.description == 'order 3 modified'
        self.assert_sql_count(testbase.db, go, 1)
        assert o.__dict__['isopen'] == 5
        assert o._state.committed_state['description'] == 'order 3 modified'
        assert o._state.committed_state['isopen'] == 1


    def test_partial_expire_lazy(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref='user'),
            })
        mapper(Address, addresses)

        sess = create_session()
        u = sess.query(User).get(8)
        
        sess.expire(u, ['name', 'addresses'])
        assert 'name' not in u.__dict__
        assert 'addresses' not in u.__dict__
        
        # hit the lazy loader.  just does the lazy load,
        # doesnt do the overall refresh
        def go():
            assert u.addresses[0].email_address=='ed@wood.com'
        self.assert_sql_count(testbase.db, go, 1)
        
        assert 'name' not in u.__dict__
        
        # check that mods to expired lazy-load attributes 
        # only do the lazy load
        sess.expire(u, ['name', 'addresses'])
        def go():
            u.addresses = [Address(email_address='foo@bar.com')]
        self.assert_sql_count(testbase.db, go, 1)
        
        sess.flush()
        
        def go():
            assert u.addresses[0].email_address=='foo@bar.com'
            assert len(u.addresses) == 1
        self.assert_sql_count(testbase.db, go, 0)
        
        def go():
            assert u.name == 'ed'
        # this loads due to the name attribute
        self.assert_sql_count(testbase.db, go, 1)

    def test_partial_expire_eager(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref='user', lazy=False),
            })
        mapper(Address, addresses)

        sess = create_session()
        u = sess.query(User).get(8)

        sess.expire(u, ['name', 'addresses'])
        assert 'name' not in u.__dict__
        assert 'addresses' not in u.__dict__

        def go():
            assert u.addresses[0].email_address=='ed@wood.com'
        self.assert_sql_count(testbase.db, go, 1)

        # check that mods to expired eager-load attributes 
        # do the refresh
        sess.expire(u, ['name', 'addresses'])
        def go():
            u.addresses = [Address(email_address='foo@bar.com')]
        self.assert_sql_count(testbase.db, go, 1)
        sess.flush()
        
        # this should trigger the whole load
        def go():
            assert u.addresses[0].email_address=='foo@bar.com'
            assert len(u.addresses) == 1
        self.assert_sql_count(testbase.db, go, 1)
        
        def go():
            assert u.name == 'ed'
        # and this was already loaded
        self.assert_sql_count(testbase.db, go, 0)


class RefreshTest(FixtureTest):
    keep_mappers = False
    refresh_data = True

    def test_refresh(self):
        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses), backref='user')
        })
        s = create_session()
        u = s.get(User, 7)
        u.name = 'foo'
        a = Address()
        assert object_session(a) is None
        u.addresses.append(a)
        assert a.email_address is None
        assert id(a) in [id(x) for x in u.addresses]

        s.refresh(u)

        # its refreshed, so not dirty
        assert u not in s.dirty

        # username is back to the DB
        assert u.name == 'jack'
        
        assert id(a) not in [id(x) for x in u.addresses]

        u.name = 'foo'
        u.addresses.append(a)
        # now its dirty
        assert u in s.dirty
        assert u.name == 'foo'
        assert id(a) in [id(x) for x in u.addresses]
        s.expire(u)

        # get the attribute, it refreshes
        assert u.name == 'jack'
        assert id(a) not in [id(x) for x in u.addresses]

    def test_refresh_with_lazy(self):
        """test that when a lazy loader is set as a trigger on an object's attribute 
        (at the attribute level, not the class level), a refresh() operation doesnt 
        fire the lazy loader or create any problems"""
        
        s = create_session()
        mapper(User, users, properties={'addresses':relation(mapper(Address, addresses))})
        q = s.query(User).options(lazyload('addresses'))
        u = q.filter(users.c.id==8).first()
        def go():
            s.refresh(u)
        self.assert_sql_count(testbase.db, go, 1)


    def test_refresh_with_eager(self):
        """test that a refresh/expire operation loads rows properly and sends correct "isnew" state to eager loaders"""
        
        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses), lazy=False)
        })
        
        s = create_session()
        u = s.get(User, 8)
        assert len(u.addresses) == 3
        s.refresh(u)
        assert len(u.addresses) == 3

        s = create_session()
        u = s.get(User, 8)
        assert len(u.addresses) == 3
        s.expire(u)
        assert len(u.addresses) == 3

    @testing.fails_on('maxdb')
    def test_refresh2(self):
        """test a hang condition that was occuring on expire/refresh"""

        s = create_session()
        m1 = mapper(Address, addresses)

        m2 = mapper(User, users, properties = dict(addresses=relation(Address,private=True,lazy=False)) )
        u=User()
        u.name='Justin'
        a = Address()
        a.address_id=17  # to work around the hardcoded IDs in this test suite....
        u.addresses.append(a)
        s.flush()
        s.clear()
        u = s.query(User).first()

        #ok so far
        s.expire(u)        #hangs when
        print u.name #this line runs

        s.refresh(u) #hangs





if __name__ == '__main__':
    testbase.main()
