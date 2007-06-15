from sqlalchemy import *
from testbase import Table, Column

class Base(object):
    def __init__(self, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])
            
    def __ne__(self, other):
        return not self.__eq__(other)
        
    def __eq__(self, other):
        """'passively' compare this object to another.
        
        only look at attributes that are present on the source object.
        
        """
        # use __dict__ to avoid instrumented properties
        for attr in self.__dict__.keys():
            if attr[0] == '_':
                continue
            value = getattr(self, attr)
            if hasattr(value, '__iter__') and not isinstance(value, basestring):
                if len(value) == 0:
                    continue
                for (us, them) in zip(value, getattr(other, attr)):
                    if us != them:
                        return False
                else:
                    continue
            else:
                if value is not None:
                    if value != getattr(other, attr):
                        return False
        else:
            return True

class User(Base):pass
class Order(Base):pass
class Item(Base):pass
class Keyword(Base):pass
class Address(Base):pass

metadata = MetaData()

users = Table('users', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(30), nullable=False))

orders = Table('orders', metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', None, ForeignKey('users.id')),
    Column('address_id', None, ForeignKey('addresses.id')),
    Column('description', String(30)),
    Column('isopen', Integer)
    )

addresses = Table('addresses', metadata, 
    Column('id', Integer, primary_key=True),
    Column('user_id', None, ForeignKey('users.id')),
    Column('email_address', String(50), nullable=False))

items = Table('items', metadata, 
    Column('id', Integer, primary_key=True),
    Column('description', String(30), nullable=False)
    )

order_items = Table('order_items', metadata,
    Column('item_id', None, ForeignKey('items.id')),
    Column('order_id', None, ForeignKey('orders.id')))

item_keywords = Table('item_keywords', metadata, 
    Column('item_id', None, ForeignKey('items.id')),
    Column('keyword_id', None, ForeignKey('keywords.id')))

keywords = Table('keywords', metadata, 
    Column('id', Integer, primary_key=True),
    Column('name', String(30), nullable=False)
    )

def install_fixture_data():
    users.insert().execute(
        dict(id = 7, name = 'jack'),
        dict(id = 8, name = 'ed'),
        dict(id = 9, name = 'fred'),
        dict(id = 10, name = 'chuck'),

    )
    addresses.insert().execute(
        dict(id = 1, user_id = 7, email_address = "jack@bean.com"),
        dict(id = 2, user_id = 8, email_address = "ed@wood.com"),
        dict(id = 3, user_id = 8, email_address = "ed@bettyboop.com"),
        dict(id = 4, user_id = 8, email_address = "ed@lala.com"),
        dict(id = 5, user_id = 9, email_address = "fred@fred.com"),
    )
    orders.insert().execute(
        dict(id = 1, user_id = 7, description = 'order 1', isopen=0, address_id=1),
        dict(id = 2, user_id = 9, description = 'order 2', isopen=0, address_id=4),
        dict(id = 3, user_id = 7, description = 'order 3', isopen=1, address_id=1),
        dict(id = 4, user_id = 9, description = 'order 4', isopen=1, address_id=4),
        dict(id = 5, user_id = 7, description = 'order 5', isopen=0, address_id=1)
    )
    items.insert().execute(
        dict(id=1, description='item 1'),
        dict(id=2, description='item 2'),
        dict(id=3, description='item 3'),
        dict(id=4, description='item 4'),
        dict(id=5, description='item 5'),
    )
    order_items.insert().execute(
        dict(item_id=1, order_id=1),
        dict(item_id=2, order_id=1),
        dict(item_id=3, order_id=1),

        dict(item_id=1, order_id=2),
        dict(item_id=2, order_id=2),
        dict(item_id=3, order_id=2),

        dict(item_id=3, order_id=3),
        dict(item_id=4, order_id=3),
        dict(item_id=5, order_id=3),

        dict(item_id=1, order_id=4),
        dict(item_id=5, order_id=4),

        dict(item_id=5, order_id=5),
    )
    keywords.insert().execute(
        dict(id=1, name='blue'),
        dict(id=2, name='red'),
        dict(id=3, name='green'),
        dict(id=4, name='big'),
        dict(id=5, name='small'),
        dict(id=6, name='round'),
        dict(id=7, name='square')
    )

    # this many-to-many table has the keywords inserted
    # in primary key order, to appease the unit tests.
    # this is because postgres, oracle, and sqlite all support 
    # true insert-order row id, but of course our pal MySQL does not,
    # so the best it can do is order by, well something, so there you go.
    item_keywords.insert().execute(
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
