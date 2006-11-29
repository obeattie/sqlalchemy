from testbase import AssertMixin
import testbase
import unittest, sys, datetime

import tables
from tables import *

db = testbase.db
from sqlalchemy import *


class SessionTest(AssertMixin):
    def setUpAll(self):
        tables.create()
        tables.data()
    def tearDownAll(self):
        tables.drop()
    def tearDown(self):
        tables.delete()
        clear_mappers()
    def setUp(self):
        pass

    def test_close(self):
        """test that flush() doenst close a connection the session didnt open"""
        c = testbase.db.connect()
        class User(object):pass
        mapper(User, users)
        s = create_session(bind_to=c)
        s.save(User())
        s.flush()
        c.execute("select * from users")
        u = User()
        s.save(u)
        s.user_name = 'some user'
        s.flush()
        u = User()
        s.save(u)
        s.user_name = 'some other user'
        s.flush()

    def test_close_two(self):
        c = testbase.db.connect()
        try:
            class User(object):pass
            mapper(User, users)
            s = create_session(bind_to=c)
            tran = s.create_transaction()
            s.save(User())
            s.flush()
            c.execute("select * from users")
            u = User()
            s.save(u)
            s.user_name = 'some user'
            s.flush()
            u = User()
            s.save(u)
            s.user_name = 'some other user'
            s.flush()
            assert s.transaction is tran
            tran.close()
        finally:
            c.close()
            
    def test_update(self):
        """test that the update() method functions and doesnet blow away changes"""
        tables.delete()
        s = create_session()
        class User(object):pass
        mapper(User, users)
        
        # save user
        s.save(User())
        s.flush()
        user = s.query(User).selectone()
        s.expunge(user)
        assert user not in s
        
        # modify outside of session, assert changes remain/get saved
        user.user_name = "fred"
        s.update(user)
        assert user in s
        assert user in s.dirty
        s.flush()
        s.clear()
        user = s.query(User).selectone()
        assert user.user_name == 'fred'
        
        # insure its not dirty if no changes occur
        s.clear()
        assert user not in s
        s.update(user)
        assert user in s
        assert user not in s.dirty
        
        
class OrphanDeletionTest(AssertMixin):

    def setUpAll(self):
        tables.create()
        tables.data()
    def tearDownAll(self):
        tables.drop()
    def tearDown(self):
        clear_mappers()
    def setUp(self):
        pass

    def test_orphan(self):
        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relation(Address, cascade="all,delete-orphan", backref="user")
        ))
        s = create_session()
        a = Address()
        s.save(a)
        try:
            s.flush()
        except exceptions.FlushError, e:
            pass
        assert a.address_id is None, "Error: address should not be persistent"
        
    def test_delete_new_object(self):
        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relation(Address, cascade="all,delete-orphan", backref="user")
        ))
        s = create_session()

        u = User()
        s.save(u)
        a = Address()
        assert a not in s.new
        u.addresses.append(a)
        u.addresses.remove(a)
        s.delete(u)
        try:
            s.flush() # (erroneously) causes "a" to be persisted
            assert False
        except exceptions.FlushError:
            assert True
        assert u.user_id is None, "Error: user should not be persistent"
        assert a.address_id is None, "Error: address should not be persistent"


class CascadingOrphanDeletionTest(AssertMixin):
    def setUpAll(self):
        global meta, orders, items, attributes
        meta = BoundMetaData(db)

        orders = Table('orders', meta,
            Column('id', Integer, Sequence('order_id_seq'), primary_key = True),
            Column('name', VARCHAR(50)),

        )
        items = Table('items', meta,
            Column('id', Integer, Sequence('item_id_seq'), primary_key = True),
            Column('order_id', Integer, ForeignKey(orders.c.id), nullable=False),
            Column('name', VARCHAR(50)),

        )
        attributes = Table('attributes', meta,
            Column('id', Integer, Sequence('attribute_id_seq'), primary_key = True),
            Column('item_id', Integer, ForeignKey(items.c.id), nullable=False),
            Column('name', VARCHAR(50)),

        )

    def setUp(self):
        meta.create_all()
    def tearDown(self):
        meta.drop_all()

    def testdeletechildwithchild(self):
        class Order(object): pass
        class Item(object): pass
        class Attribute(object): pass

        attrMapper = mapper(Attribute, attributes)
        itemMapper = mapper(Item, items, properties=dict(
            attributes=relation(attrMapper, cascade="all,delete-orphan", backref="item")
        ))
        orderMapper = mapper(Order, orders, properties=dict(
            items=relation(itemMapper, cascade="all,delete-orphan", backref="order")
        ))

        s = create_session( )
        order = Order()
        s.save(order)

        item = Item()
        attr = Attribute()
        item.attributes.append(attr)

        order.items.append(item)
        order.items.remove(item) # item is an orphan, but attr is not so flush() tries to save attr
        try:
            s.flush()
            assert False
        except exceptions.FlushError, e:
            print e
            assert True

        assert item.id is None
        assert attr.id is None

class DoubleOrphanTest(testbase.AssertMixin):
    def setUpAll(self):
        global metadata, address_table, businesses, homes
        metadata = BoundMetaData(testbase.db)
        address_table = Table('addresses', metadata,
            Column('address_id', Integer, primary_key=True),
            Column('street', String(30)),
        )

        homes = Table('homes', metadata,
            Column('home_id', Integer, primary_key=True),
            Column('description', String(30)),
            Column('address_id', Integer, ForeignKey('addresses.address_id'), nullable=False),
        )

        businesses = Table('businesses', metadata,
            Column('business_id', Integer, primary_key=True, key="id"),
            Column('description', String(30), key="description"),
            Column('address_id', Integer, ForeignKey('addresses.address_id'), nullable=False),
        )
        metadata.create_all()
    def tearDown(self):
        clear_mappers()
    def tearDownAll(self):
        metadata.drop_all()
    def test_non_orphan(self):
        class Address(object):pass
        class Home(object):pass
        class Business(object):pass
        mapper(Address, address_table)
        mapper(Home, homes, properties={'address':relation(Address, cascade="all,delete-orphan")})
        mapper(Business, businesses, properties={'address':relation(Address, cascade="all,delete-orphan")})
        
        session = create_session()
        a1 = Address()
        a2 = Address()
        h1 = Home()
        b1 = Business()
        h1.address = a1
        b1.address = a2
        [session.save(x) for x in [h1,b1]]
        session.flush()
    def test_orphan(self):
        class Address(object):pass
        class Home(object):pass
        class Business(object):pass
        mapper(Address, address_table)
        mapper(Home, homes, properties={'address':relation(Address, cascade="all,delete-orphan")})
        mapper(Business, businesses, properties={'address':relation(Address, cascade="all,delete-orphan")})
        
        session = create_session()
        a1 = Address()
        session.save(a1)
        try:
            session.flush()
            assert False
        except exceptions.FlushError, e:
            assert True
        
if __name__ == "__main__":    
    testbase.main()
