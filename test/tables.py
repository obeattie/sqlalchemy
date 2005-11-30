
from sqlalchemy import *
import os
import testbase

__all__ = ['db', 'users', 'addresses', 'orders', 'orderitems', 'keywords', 'itemkeywords',
            'User', 'Address', 'Order', 'Item', 'Keyword'
        ]

ECHO = testbase.echo
db = testbase.db


users = Table('users', db,
    Column('user_id', Integer, Sequence('user_id_seq', optional=True), primary_key = True),
    Column('user_name', String(40)),
)

addresses = Table('email_addresses', db,
    Column('address_id', Integer, Sequence('address_id_seq', optional=True), primary_key = True),
    Column('user_id', Integer, ForeignKey(users.c.user_id)),
    Column('email_address', String(40)),
)

orders = Table('orders', db,
    Column('order_id', Integer, primary_key = True),
    Column('user_id', Integer, ForeignKey(users.c.user_id)),
    Column('description', String(50)),
    Column('isopen', Integer)
)

orderitems = Table('items', db,
    Column('item_id', INT, primary_key = True),
    Column('order_id', INT, ForeignKey("orders")),
    Column('item_name', VARCHAR(50))
)

keywords = Table('keywords', db,
    Column('keyword_id', Integer, primary_key = True),
    Column('name', VARCHAR(50))
)

itemkeywords = Table('itemkeywords', db,
    Column('item_id', INT, ForeignKey("items")),
    Column('keyword_id', INT, ForeignKey("keywords"))
)

def create():
    users.create()
    addresses.create()
    orders.create()
    orderitems.create()
    keywords.create()
    itemkeywords.create()
    
def drop():
    itemkeywords.drop()
    keywords.drop()
    orderitems.drop()
    orders.drop()
    addresses.drop()
    users.drop()
    
def delete():
    itemkeywords.delete().execute()
    keywords.delete().execute()
    orderitems.delete().execute()
    orders.delete().execute()
    addresses.delete().execute()
    users.delete().execute()
    db.commit()
    
def data():
    delete()
    
    # with SQLITE, the OID column of a table defaults to the primary key, if it has one.
    # so to database-neutrally get rows back in "insert order" based on OID, we
    # have to also put the primary keys in order for the purpose of these tests
    users.insert().execute(
        dict(user_id = 7, user_name = 'jack'),
        dict(user_id = 8, user_name = 'ed'),
        dict(user_id = 9, user_name = 'fred')
    )
    addresses.insert().execute(
        dict(address_id = 1, user_id = 7, email_address = "jack@bean.com"),
        dict(address_id = 2, user_id = 8, email_address = "ed@wood.com"),
        dict(address_id = 3, user_id = 8, email_address = "ed@lala.com")
    )
    orders.insert().execute(
        dict(order_id = 1, user_id = 7, description = 'order 1', isopen=0),
        dict(order_id = 2, user_id = 9, description = 'order 2', isopen=0),
        dict(order_id = 3, user_id = 7, description = 'order 3', isopen=1),
        dict(order_id = 4, user_id = 9, description = 'order 4', isopen=1),
        dict(order_id = 5, user_id = 7, description = 'order 5', isopen=0)
    )
    orderitems.insert().execute(
        dict(item_id=1, order_id=2, item_name='item 1'),
        dict(item_id=2, order_id=2, item_name='item 2'),
        dict(item_id=3, order_id=3, item_name='item 3'),
        dict(item_id=4, order_id=3, item_name='item 4'),
        dict(item_id=5, order_id=3, item_name='item 5'),
    )
    keywords.insert().execute(
        dict(keyword_id=1, name='blue'),
        dict(keyword_id=2, name='red'),
        dict(keyword_id=3, name='green'),
        dict(keyword_id=4, name='big'),
        dict(keyword_id=5, name='small'),
        dict(keyword_id=6, name='round'),
        dict(keyword_id=7, name='square')
    )
    
    # this many-to-many table has the keywords inserted
    # in primary key order, to appease the unit tests.
    # this is because postgres, oracle, and sqlite all support 
    # true insert-order row id, but of course our pal MySQL does not,
    # so the best it can do is order by, well something, so there you go.
    itemkeywords.insert().execute(
        dict(keyword_id=2, item_id=1),
        dict(keyword_id=2, item_id=2),
        dict(keyword_id=4, item_id=1),
        dict(keyword_id=6, item_id=1),
        dict(keyword_id=5, item_id=2),
        dict(keyword_id=3, item_id=3),
        dict(keyword_id=4, item_id=3),
        dict(keyword_id=7, item_id=2),
        dict(keyword_id=6, item_id=3)
    )

    db.commit()
    
class User(object):
    def __init__(self):
        self.user_id = None
    def __repr__(self):
        return (
"""
objid: %d
User ID: %s
User Name: %s
email address ?: %s
Addresses: %s
Orders: %s
Open Orders %s
Closed Orderss %s
------------------
""" % tuple([id(self), self.user_id, repr(self.user_name), repr(getattr(self, 'email_address', None))] + [repr(getattr(self, attr, None)) for attr in ('addresses', 'orders', 'open_orders', 'closed_orders')])
)

class Address(object):
    def __repr__(self):
        return "Address: " + repr(getattr(self, 'address_id', None)) + " " + repr(getattr(self, 'user_id', None)) + " " + repr(self.email_address)

class Order(object):
    def __init__(self):
        self.isopen=0
    def __repr__(self):
        return "Order: " + repr(self.description) + " " + repr(self.isopen) + " " + repr(getattr(self, 'items', None))

class Item(object):
    def __repr__(self):
        return "Item: " + repr(self.item_name) + " " +repr(getattr(self, 'keywords', None))
    
class Keyword(object):
    def __repr__(self):
        return "Keyword: %s/%s" % (repr(getattr(self, 'keyword_id', None)),repr(self.name))



#db.echo = True
