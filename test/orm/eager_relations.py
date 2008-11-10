"""basic tests of eager loaded attributes"""

import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from sqlalchemy.orm import *
from testlib import *
from testlib.fixtures import *
from query import QueryTest

class EagerTest(FixtureTest):
    keep_mappers = False
    keep_data = True

    def test_basic(self):
        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses), lazy=False)
        })
        sess = create_session()
        q = sess.query(User)

        assert [User(id=7, addresses=[Address(id=1, email_address='jack@bean.com')])] == q.filter(User.id==7).all()
        assert fixtures.user_address_result == q.all()

    def test_no_orphan(self):
        """test that an eagerly loaded child object is not marked as an orphan"""

        mapper(User, users, properties={
            'addresses':relation(Address, cascade="all,delete-orphan", lazy=False)
        })
        mapper(Address, addresses)

        sess = create_session()
        user = sess.query(User).get(7)
        assert getattr(User, 'addresses').hasparent(user.addresses[0], optimistic=True)
        assert not class_mapper(Address)._is_orphan(user.addresses[0])

    def test_orderby(self):
        mapper(User, users, properties = {
            'addresses':relation(mapper(Address, addresses), lazy=False, order_by=addresses.c.email_address),
        })
        q = create_session().query(User)
        assert [
            User(id=7, addresses=[
                Address(id=1)
            ]),
            User(id=8, addresses=[
                Address(id=3, email_address='ed@bettyboop.com'),
                Address(id=4, email_address='ed@lala.com'),
                Address(id=2, email_address='ed@wood.com')
            ]),
            User(id=9, addresses=[
                Address(id=5)
            ]),
            User(id=10, addresses=[])
        ] == q.all()

    def test_orderby_multi(self):
        mapper(User, users, properties = {
            'addresses':relation(mapper(Address, addresses), lazy=False, order_by=[addresses.c.email_address, addresses.c.id]),
        })
        q = create_session().query(User)
        assert [
            User(id=7, addresses=[
                Address(id=1)
            ]),
            User(id=8, addresses=[
                Address(id=3, email_address='ed@bettyboop.com'),
                Address(id=4, email_address='ed@lala.com'),
                Address(id=2, email_address='ed@wood.com')
            ]),
            User(id=9, addresses=[
                Address(id=5)
            ]),
            User(id=10, addresses=[])
        ] == q.all()

    def test_orderby_related(self):
        """tests that a regular mapper select on a single table can order by a relation to a second table"""

        mapper(Address, addresses)

        mapper(User, users, properties = dict(
            addresses = relation(Address, lazy=False),
        ))

        q = create_session().query(User)
        l = q.filter(User.id==Address.user_id).order_by(Address.email_address).all()

        assert [
            User(id=8, addresses=[
                Address(id=2, email_address='ed@wood.com'),
                Address(id=3, email_address='ed@bettyboop.com'),
                Address(id=4, email_address='ed@lala.com'),
            ]),
            User(id=9, addresses=[
                Address(id=5)
            ]),
            User(id=7, addresses=[
                Address(id=1)
            ]),
        ] == l

    def test_orderby_desc(self):
        mapper(Address, addresses)

        mapper(User, users, properties = dict(
            addresses = relation(Address, lazy=False, order_by=[desc(addresses.c.email_address)]),
        ))
        sess = create_session()
        assert [
            User(id=7, addresses=[
                Address(id=1)
            ]),
            User(id=8, addresses=[
                Address(id=2, email_address='ed@wood.com'),
                Address(id=4, email_address='ed@lala.com'),
                Address(id=3, email_address='ed@bettyboop.com'),
            ]),
            User(id=9, addresses=[
                Address(id=5)
            ]),
            User(id=10, addresses=[])
        ] == sess.query(User).all()

    def test_deferred_fk_col(self):
        mapper(Address, addresses, properties={
            'user_id':deferred(addresses.c.user_id),
            'user':relation(User, lazy=False)
        })
        mapper(User, users)

        assert [Address(id=1, user=User(id=7)), Address(id=4, user=User(id=8)), Address(id=5, user=User(id=9))] == create_session().query(Address).filter(Address.id.in_([1, 4, 5])).all()

        assert [Address(id=1, user=User(id=7)), Address(id=4, user=User(id=8)), Address(id=5, user=User(id=9))] == create_session().query(Address).filter(Address.id.in_([1, 4, 5])).limit(3).all()

        sess = create_session()
        a = sess.query(Address).get(1)
        def go():
            assert a.user_id==7
        # assert that the eager loader added 'user_id' to the row
        # and deferred loading of that col was disabled
        self.assert_sql_count(testing.db, go, 0)

        # do the mapping in reverse
        # (we would have just used an "addresses" backref but the test fixtures then require the whole
        # backref to be set up, lazy loaders trigger, etc.)
        clear_mappers()

        mapper(Address, addresses, properties={
            'user_id':deferred(addresses.c.user_id),
        })
        mapper(User, users, properties={'addresses':relation(Address, lazy=False)})

        assert [User(id=7, addresses=[Address(id=1)])] == create_session().query(User).filter(User.id==7).all()

        assert [User(id=7, addresses=[Address(id=1)])] == create_session().query(User).limit(1).filter(User.id==7).all()

        sess = create_session()
        u = sess.query(User).get(7)
        def go():
            assert u.addresses[0].user_id==7
        # assert that the eager loader didn't have to affect 'user_id' here
        # and that its still deferred
        self.assert_sql_count(testing.db, go, 1)

        clear_mappers()

        mapper(User, users, properties={'addresses':relation(Address, lazy=False)})
        mapper(Address, addresses, properties={
            'user_id':deferred(addresses.c.user_id),
            'dingalings':relation(Dingaling, lazy=False)
        })
        mapper(Dingaling, dingalings, properties={
            'address_id':deferred(dingalings.c.address_id)
        })
        sess = create_session()
        def go():
            u = sess.query(User).limit(1).get(8)
            assert User(id=8, addresses=[Address(id=2, dingalings=[Dingaling(id=1)]), Address(id=3), Address(id=4)]) == u
        self.assert_sql_count(testing.db, go, 1)

    def test_many_to_many(self):

        mapper(Keyword, keywords)
        mapper(Item, items, properties = dict(
                keywords = relation(Keyword, secondary=item_keywords, lazy=False, order_by=keywords.c.id),
        ))

        q = create_session().query(Item)
        def go():
            assert fixtures.item_keyword_result == q.all()
        self.assert_sql_count(testing.db, go, 1)

        def go():
            assert fixtures.item_keyword_result[0:2] == q.join('keywords').filter(keywords.c.name == 'red').all()
        self.assert_sql_count(testing.db, go, 1)

        def go():
            assert fixtures.item_keyword_result[0:2] == q.join('keywords', aliased=True).filter(keywords.c.name == 'red').all()
        self.assert_sql_count(testing.db, go, 1)


    def test_eager_option(self):
        mapper(Keyword, keywords)
        mapper(Item, items, properties = dict(
                keywords = relation(Keyword, secondary=item_keywords, lazy=True, order_by=keywords.c.id),
        ))

        q = create_session().query(Item)

        def go():
            assert fixtures.item_keyword_result[0:2] == q.options(eagerload('keywords')).join('keywords').filter(keywords.c.name == 'red').all()

        self.assert_sql_count(testing.db, go, 1)

    def test_cyclical(self):
        """test that a circular eager relationship breaks the cycle with a lazy loader"""

        mapper(Address, addresses)
        mapper(User, users, properties = dict(
            addresses = relation(Address, lazy=False, backref=backref('user', lazy=False))
        ))
        assert class_mapper(User).get_property('addresses').lazy is False
        assert class_mapper(Address).get_property('user').lazy is False

        sess = create_session()
        assert fixtures.user_address_result == sess.query(User).all()

    def test_double(self):
        """tests eager loading with two relations simulatneously, from the same table, using aliases.  """
        openorders = alias(orders, 'openorders')
        closedorders = alias(orders, 'closedorders')

        mapper(Address, addresses)

        mapper(User, users, properties = dict(
            addresses = relation(Address, lazy=False),
            open_orders = relation(mapper(Order, openorders, entity_name='open'), primaryjoin = and_(openorders.c.isopen == 1, users.c.id==openorders.c.user_id), lazy=False),
            closed_orders = relation(mapper(Order, closedorders,entity_name='closed'), primaryjoin = and_(closedorders.c.isopen == 0, users.c.id==closedorders.c.user_id), lazy=False)
        ))
        q = create_session().query(User)

        def go():
            assert [
                User(
                    id=7,
                    addresses=[Address(id=1)],
                    open_orders = [Order(id=3)],
                    closed_orders = [Order(id=1), Order(id=5)]
                ),
                User(
                    id=8,
                    addresses=[Address(id=2), Address(id=3), Address(id=4)],
                    open_orders = [],
                    closed_orders = []
                ),
                User(
                    id=9,
                    addresses=[Address(id=5)],
                    open_orders = [Order(id=4)],
                    closed_orders = [Order(id=2)]
                ),
                User(id=10)

            ] == q.all()
        self.assert_sql_count(testing.db, go, 1)

    def test_double_same_mappers(self):
        """tests eager loading with two relations simulatneously, from the same table, using aliases.  """

        mapper(Address, addresses)
        mapper(Order, orders, properties={
            'items':relation(Item, secondary=order_items, lazy=False, order_by=items.c.id),
        })
        mapper(Item, items)
        mapper(User, users, properties = dict(
            addresses = relation(Address, lazy=False),
            open_orders = relation(Order, primaryjoin = and_(orders.c.isopen == 1, users.c.id==orders.c.user_id), lazy=False),
            closed_orders = relation(Order, primaryjoin = and_(orders.c.isopen == 0, users.c.id==orders.c.user_id), lazy=False)
        ))
        q = create_session().query(User)

        def go():
            assert [
                User(
                    id=7,
                    addresses=[Address(id=1)],
                    open_orders = [Order(id=3, items=[Item(id=3), Item(id=4), Item(id=5)])],
                    closed_orders = [Order(id=1, items=[Item(id=1), Item(id=2), Item(id=3)]), Order(id=5, items=[Item(id=5)])]
                ),
                User(
                    id=8,
                    addresses=[Address(id=2), Address(id=3), Address(id=4)],
                    open_orders = [],
                    closed_orders = []
                ),
                User(
                    id=9,
                    addresses=[Address(id=5)],
                    open_orders = [Order(id=4, items=[Item(id=1), Item(id=5)])],
                    closed_orders = [Order(id=2, items=[Item(id=1), Item(id=2), Item(id=3)])]
                ),
                User(id=10)

            ] == q.all()
        self.assert_sql_count(testing.db, go, 1)

    def test_no_false_hits(self):
        """test that eager loaders don't interpret main table columns as part of their eager load."""

        mapper(User, users, properties={
            'addresses':relation(Address, lazy=False),
            'orders':relation(Order, lazy=False)
        })
        mapper(Address, addresses)
        mapper(Order, orders)

        allusers = create_session().query(User).all()

        # using a textual select, the columns will be 'id' and 'name'.
        # the eager loaders have aliases which should not hit on those columns, they should
        # be required to locate only their aliased/fully table qualified column name.
        noeagers = create_session().query(User).from_statement("select * from users").all()
        assert 'orders' not in noeagers[0].__dict__
        assert 'addresses' not in noeagers[0].__dict__

    def test_limit(self):
        """test limit operations combined with lazy-load relationships."""

        mapper(Item, items)
        mapper(Order, orders, properties={
            'items':relation(Item, secondary=order_items, lazy=False, order_by=items.c.id)
        })
        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses), lazy=False, order_by=addresses.c.id),
            'orders':relation(Order, lazy=True)
        })

        sess = create_session()
        q = sess.query(User)

        if testing.against('mysql'):
            l = q.limit(2).all()
            assert fixtures.user_all_result[:2] == l
        else:
            l = q.limit(2).offset(1).order_by(User.id).all()
            print fixtures.user_all_result[1:3]
            print l
            assert fixtures.user_all_result[1:3] == l
    test_limit = testing.fails_on('maxdb')(test_limit)

    def test_distinct(self):
        # this is an involved 3x union of the users table to get a lot of rows.
        # then see if the "distinct" works its way out.  you actually get the same
        # result with or without the distinct, just via less or more rows.
        u2 = users.alias('u2')
        s = union_all(u2.select(use_labels=True), u2.select(use_labels=True), u2.select(use_labels=True)).alias('u')

        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses), lazy=False),
        })

        sess = create_session()
        q = sess.query(User)

        def go():
            l = q.filter(s.c.u2_id==User.c.id).distinct().all()
            assert fixtures.user_address_result == l
        self.assert_sql_count(testing.db, go, 1)

    def test_limit_2(self):
        mapper(Keyword, keywords)
        mapper(Item, items, properties = dict(
                keywords = relation(Keyword, secondary=item_keywords, lazy=False, order_by=[keywords.c.id]),
            ))

        sess = create_session()
        q = sess.query(Item)
        l = q.filter((Item.c.description=='item 2') | (Item.c.description=='item 5') | (Item.c.description=='item 3')).\
            order_by(Item.id).limit(2).all()

        assert fixtures.item_keyword_result[1:3] == l
    test_limit_2 = testing.fails_on('maxdb')(test_limit_2)

    def test_limit_3(self):
        """test that the ORDER BY is propagated from the inner select to the outer select, when using the
        'wrapped' select statement resulting from the combination of eager loading and limit/offset clauses."""

        mapper(Item, items)
        mapper(Order, orders, properties = dict(
                items = relation(Item, secondary=order_items, lazy=False)
        ))

        mapper(Address, addresses)
        mapper(User, users, properties = dict(
            addresses = relation(Address, lazy=False),
            orders = relation(Order, lazy=False),
        ))
        sess = create_session()

        q = sess.query(User)

        if not testing.against('maxdb', 'mssql'):
            l = q.join('orders').order_by(Order.user_id.desc()).limit(2).offset(1)
            assert [
                User(id=9,
                    orders=[Order(id=2), Order(id=4)],
                    addresses=[Address(id=5)]
                ),
                User(id=7,
                    orders=[Order(id=1), Order(id=3), Order(id=5)],
                    addresses=[Address(id=1)]
                )
            ] == l.all()

        l = q.join('addresses').order_by(Address.email_address.desc()).limit(1).offset(0)
        assert [
            User(id=7,
                orders=[Order(id=1), Order(id=3), Order(id=5)],
                addresses=[Address(id=1)]
            )
        ] == l.all()
    test_limit_3 = testing.fails_on('maxdb')(test_limit_3)

    def test_limit_4(self):
        # tests the LIMIT/OFFSET aliasing on a mapper against a select.   original issue from ticket #904
        sel = select([users, addresses.c.email_address], users.c.id==addresses.c.user_id).alias('useralias')
        mapper(User, sel, properties={
            'orders':relation(Order, primaryjoin=sel.c.id==orders.c.user_id, lazy=False)
        })
        mapper(Order, orders)

        sess = create_session()
        self.assertEquals(sess.query(User).first(),
            User(name=u'jack',orders=[
                Order(address_id=1,description=u'order 1',isopen=0,user_id=7,id=1),
                Order(address_id=1,description=u'order 3',isopen=1,user_id=7,id=3),
                Order(address_id=None,description=u'order 5',isopen=0,user_id=7,id=5)],
            email_address=u'jack@bean.com',id=7)
        )

    def test_one_to_many_scalar(self):
        mapper(User, users, properties = dict(
            address = relation(mapper(Address, addresses), lazy=False, uselist=False)
        ))
        q = create_session().query(User)

        def go():
            l = q.filter(users.c.id == 7).all()
            assert [User(id=7, address=Address(id=1))] == l
        self.assert_sql_count(testing.db, go, 1)

    def test_many_to_one(self):
        mapper(Address, addresses, properties = dict(
            user = relation(mapper(User, users), lazy=False)
        ))
        sess = create_session()
        q = sess.query(Address)

        def go():
            a = q.filter(addresses.c.id==1).one()
            assert a.user is not None
            u1 = sess.query(User).get(7)
            assert a.user is u1
        self.assert_sql_count(testing.db, go, 1)
    test_many_to_one = testing.fails_on('maxdb')(test_many_to_one)


    def test_one_and_many(self):
        """tests eager load for a parent object with a child object that
        contains a many-to-many relationship to a third object."""

        mapper(User, users, properties={
            'orders':relation(Order, lazy=False)
        })
        mapper(Item, items)
        mapper(Order, orders, properties = dict(
                items = relation(Item, secondary=order_items, lazy=False, order_by=items.c.id)
            ))

        q = create_session().query(User)

        l = q.filter("users.id in (7, 8, 9)")

        def go():
            assert fixtures.user_order_result[0:3] == l.all()
        self.assert_sql_count(testing.db, go, 1)

    def test_double_with_aggregate(self):

        max_orders_by_user = select([func.max(orders.c.id).label('order_id')], group_by=[orders.c.user_id]).alias('max_orders_by_user')

        max_orders = orders.select(orders.c.id==max_orders_by_user.c.order_id).alias('max_orders')

        mapper(Order, orders)
        mapper(User, users, properties={
               'orders':relation(Order, backref='user', lazy=False),
               'max_order':relation(mapper(Order, max_orders, non_primary=True), lazy=False, uselist=False)
               })
        q = create_session().query(User)

        def go():
            assert [
                User(id=7, orders=[
                        Order(id=1),
                        Order(id=3),
                        Order(id=5),
                    ],
                    max_order=Order(id=5)
                ),
                User(id=8, orders=[]),
                User(id=9, orders=[Order(id=2),Order(id=4)],
                    max_order=Order(id=4)
                ),
                User(id=10),
            ] == q.all()
        self.assert_sql_count(testing.db, go, 1)

    def test_wide(self):
        mapper(Order, orders, properties={'items':relation(Item, secondary=order_items, lazy=False, order_by=items.c.id)})
        mapper(Item, items)
        mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = False),
            orders = relation(Order, lazy = False),
        ))
        q = create_session().query(User)
        l = q.all()
        assert fixtures.user_all_result == q.all()

    def test_against_select(self):
        """test eager loading of a mapper which is against a select"""

        s = select([orders], orders.c.isopen==1).alias('openorders')

        mapper(Order, s, properties={
            'user':relation(User, lazy=False)
        })
        mapper(User, users)
        mapper(Item, items)

        q = create_session().query(Order)
        assert [
            Order(id=3, user=User(id=7)),
            Order(id=4, user=User(id=9))
        ] == q.all()

        q = q.select_from(s.join(order_items).join(items)).filter(~Item.id.in_([1, 2, 5]))
        assert [
            Order(id=3, user=User(id=7)),
        ] == q.all()

    def test_aliasing(self):
        """test that eager loading uses aliases to insulate the eager load from regular criterion against those tables."""

        mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy=False)
        ))
        q = create_session().query(User)
        l = q.filter(addresses.c.email_address == 'ed@lala.com').filter(Address.user_id==User.id)
        assert fixtures.user_address_result[1:2] == l.all()

class AddEntityTest(FixtureTest):
    keep_mappers = False
    keep_data = True

    def _assert_result(self):
        return [
            (
                User(id=7,
                    addresses=[Address(id=1)]
                ),
                Order(id=1,
                    items=[Item(id=1), Item(id=2), Item(id=3)]
                ),
            ),
            (
                User(id=7,
                    addresses=[Address(id=1)]
                ),
                Order(id=3,
                    items=[Item(id=3), Item(id=4), Item(id=5)]
                ),
            ),
            (
                User(id=7,
                    addresses=[Address(id=1)]
                ),
                Order(id=5,
                    items=[Item(id=5)]
                ),
            ),
            (
                 User(id=9,
                    addresses=[Address(id=5)]
                ),
                 Order(id=2,
                    items=[Item(id=1), Item(id=2), Item(id=3)]
                ),
             ),
             (
                  User(id=9,
                    addresses=[Address(id=5)]
                ),
                  Order(id=4,
                    items=[Item(id=1), Item(id=5)]
                ),
              )
        ]

    def test_basic(self):
        mapper(User, users, properties={
            'addresses':relation(Address, lazy=False),
            'orders':relation(Order)
        })
        mapper(Address, addresses)
        mapper(Order, orders, properties={
            'items':relation(Item, secondary=order_items, lazy=False, order_by=items.c.id)
        })
        mapper(Item, items)


        sess = create_session()
        def go():
            ret = sess.query(User).add_entity(Order).join('orders', aliased=True).order_by(User.id).order_by(Order.id).all()
            self.assertEquals(ret, self._assert_result())
        self.assert_sql_count(testing.db, go, 1)

    def test_options(self):
        mapper(User, users, properties={
            'addresses':relation(Address),
            'orders':relation(Order)
        })
        mapper(Address, addresses)
        mapper(Order, orders, properties={
            'items':relation(Item, secondary=order_items, order_by=items.c.id)
        })
        mapper(Item, items)

        sess = create_session()

        def go():
            ret = sess.query(User).options(eagerload('addresses')).add_entity(Order).join('orders', aliased=True).order_by(User.id).order_by(Order.id).all()
            self.assertEquals(ret, self._assert_result())
        self.assert_sql_count(testing.db, go, 6)

        sess.clear()
        def go():
            ret = sess.query(User).options(eagerload('addresses')).add_entity(Order).options(eagerload('items', Order)).join('orders', aliased=True).order_by(User.id).order_by(Order.id).all()
            self.assertEquals(ret, self._assert_result())
        self.assert_sql_count(testing.db, go, 1)

class OrderBySecondaryTest(ORMTest):
    def define_tables(self, metadata):
        global a, b, m2m
        m2m = Table('mtom', metadata, 
            Column('id', Integer, primary_key=True),
            Column('aid', Integer, ForeignKey('a.id')),
            Column('bid', Integer, ForeignKey('b.id')),
            )
            
        a = Table('a', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(50)),
            )
        b = Table('b', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(50)),
            )

    def insert_data(self):
        a.insert().execute([
            {'id':1, 'data':'a1'},
            {'id':2, 'data':'a2'}
        ])
        
        b.insert().execute([
            {'id':1, 'data':'b1'},
            {'id':2, 'data':'b2'},
            {'id':3, 'data':'b3'},
            {'id':4, 'data':'b4'},
        ])
        
        m2m.insert().execute([
            {'id':2, 'aid':1, 'bid':1},
            {'id':4, 'aid':2, 'bid':4},
            {'id':1, 'aid':1, 'bid':3},
            {'id':6, 'aid':2, 'bid':2},
            {'id':3, 'aid':1, 'bid':2},
            {'id':5, 'aid':2, 'bid':3},
        ])
    
    def test_ordering(self):
        class A(Base):pass
        class B(Base):pass
        
        mapper(A, a, properties={
            'bs':relation(B, secondary=m2m, lazy=False, order_by=m2m.c.id)
        })
        mapper(B, b)
        
        sess = create_session()
        self.assertEquals(sess.query(A).all(), [A(data='a1', bs=[B(data='b3'), B(data='b1'), B(data='b2')]), A(bs=[B(data='b4'), B(data='b3'), B(data='b2')])])
        
        
class SelfReferentialEagerTest(ORMTest):
    def define_tables(self, metadata):
        global nodes
        nodes = Table('nodes', metadata,
            Column('id', Integer, Sequence('node_id_seq', optional=True), primary_key=True),
            Column('parent_id', Integer, ForeignKey('nodes.id')),
            Column('data', String(30)))

    def test_basic(self):
        class Node(Base):
            def append(self, node):
                self.children.append(node)

        mapper(Node, nodes, properties={
            'children':relation(Node, lazy=False, join_depth=3)
        })
        sess = create_session()
        n1 = Node(data='n1')
        n1.append(Node(data='n11'))
        n1.append(Node(data='n12'))
        n1.append(Node(data='n13'))
        n1.children[1].append(Node(data='n121'))
        n1.children[1].append(Node(data='n122'))
        n1.children[1].append(Node(data='n123'))
        sess.save(n1)
        sess.flush()
        sess.clear()
        def go():
            d = sess.query(Node).filter_by(data='n1').first()
            assert Node(data='n1', children=[
                Node(data='n11'),
                Node(data='n12', children=[
                    Node(data='n121'),
                    Node(data='n122'),
                    Node(data='n123')
                ]),
                Node(data='n13')
            ]) == d
        self.assert_sql_count(testing.db, go, 1)
    test_basic = testing.fails_on('maxdb')(test_basic)


    def test_lazy_fallback_doesnt_affect_eager(self):
        class Node(Base):
            def append(self, node):
                self.children.append(node)

        mapper(Node, nodes, properties={
            'children':relation(Node, lazy=False, join_depth=1)
        })
        sess = create_session()
        n1 = Node(data='n1')
        n1.append(Node(data='n11'))
        n1.append(Node(data='n12'))
        n1.append(Node(data='n13'))
        n1.children[1].append(Node(data='n121'))
        n1.children[1].append(Node(data='n122'))
        n1.children[1].append(Node(data='n123'))
        sess.save(n1)
        sess.flush()
        sess.clear()

        # eager load with join depth 1.  when eager load of 'n1'
        # hits the children of 'n12', no columns are present, eager loader
        # degrades to lazy loader; fine.  but then, 'n12' is *also* in the
        # first level of columns since we're loading the whole table.
        # when those rows arrive, now we *can* eager load its children and an
        # eager collection should be initialized.  essentially the 'n12' instance
        # is present in not just two different rows but two distinct sets of columns
        # in this result set.
        def go():
            allnodes = sess.query(Node).order_by(Node.data).all()
            n12 = allnodes[2]
            assert n12.data == 'n12'
            print "N12 IS", id(n12)
            print [c.data for c in n12.children]
            assert [
                Node(data='n121'),
                Node(data='n122'),
                Node(data='n123')
            ] == list(n12.children)
        self.assert_sql_count(testing.db, go, 1)

    def test_with_deferred(self):
        class Node(Base):
            def append(self, node):
                self.children.append(node)

        mapper(Node, nodes, properties={
            'children':relation(Node, lazy=False, join_depth=3),
            'data':deferred(nodes.c.data)
        })
        sess = create_session()
        n1 = Node(data='n1')
        n1.append(Node(data='n11'))
        n1.append(Node(data='n12'))
        sess.save(n1)
        sess.flush()
        sess.clear()

        def go():
            assert Node(data='n1', children=[Node(data='n11'), Node(data='n12')]) == sess.query(Node).first()
        self.assert_sql_count(testing.db, go, 4)

        sess.clear()

        def go():
            assert Node(data='n1', children=[Node(data='n11'), Node(data='n12')]) == sess.query(Node).options(undefer('data')).first()
        self.assert_sql_count(testing.db, go, 3)

        sess.clear()

        def go():
            assert Node(data='n1', children=[Node(data='n11'), Node(data='n12')]) == sess.query(Node).options(undefer('data'), undefer('children.data')).first()
        self.assert_sql_count(testing.db, go, 1)



    def test_options(self):
        class Node(Base):
            def append(self, node):
                self.children.append(node)

        mapper(Node, nodes, properties={
            'children':relation(Node, lazy=True)
        })
        sess = create_session()
        n1 = Node(data='n1')
        n1.append(Node(data='n11'))
        n1.append(Node(data='n12'))
        n1.append(Node(data='n13'))
        n1.children[1].append(Node(data='n121'))
        n1.children[1].append(Node(data='n122'))
        n1.children[1].append(Node(data='n123'))
        sess.save(n1)
        sess.flush()
        sess.clear()
        def go():
            d = sess.query(Node).filter_by(data='n1').options(eagerload('children.children')).first()
            assert Node(data='n1', children=[
                Node(data='n11'),
                Node(data='n12', children=[
                    Node(data='n121'),
                    Node(data='n122'),
                    Node(data='n123')
                ]),
                Node(data='n13')
            ]) == d
        self.assert_sql_count(testing.db, go, 2)

        def go():
            d = sess.query(Node).filter_by(data='n1').options(eagerload('children.children')).first()

        # test that the query isn't wrapping the initial query for eager loading.
        # testing only sqlite for now since the query text is slightly different on other
        # dialects
        if testing.against('sqlite'):
            self.assert_sql(testing.db, go, [
                (
                    "SELECT nodes.id AS nodes_id, nodes.parent_id AS nodes_parent_id, nodes.data AS nodes_data FROM nodes WHERE nodes.data = :data_1 ORDER BY nodes.oid  LIMIT 1 OFFSET 0",
                    {'data_1': 'n1'}
                ),
            ])

    def test_no_depth(self):
        class Node(Base):
            def append(self, node):
                self.children.append(node)

        mapper(Node, nodes, properties={
            'children':relation(Node, lazy=False)
        })
        sess = create_session()
        n1 = Node(data='n1')
        n1.append(Node(data='n11'))
        n1.append(Node(data='n12'))
        n1.append(Node(data='n13'))
        n1.children[1].append(Node(data='n121'))
        n1.children[1].append(Node(data='n122'))
        n1.children[1].append(Node(data='n123'))
        sess.save(n1)
        sess.flush()
        sess.clear()
        def go():
            d = sess.query(Node).filter_by(data='n1').first()
            assert Node(data='n1', children=[
                Node(data='n11'),
                Node(data='n12', children=[
                    Node(data='n121'),
                    Node(data='n122'),
                    Node(data='n123')
                ]),
                Node(data='n13')
            ]) == d
        self.assert_sql_count(testing.db, go, 3)
    test_no_depth = testing.fails_on('maxdb')(test_no_depth)

class SelfReferentialM2MEagerTest(ORMTest):
    def define_tables(self, metadata):
        global widget, widget_rel

        widget = Table('widget', metadata,
            Column('id', Integer, primary_key=True),
            Column('name', Unicode(40), nullable=False, unique=True),
        )

        widget_rel = Table('widget_rel', metadata,
            Column('parent_id', Integer, ForeignKey('widget.id')),
            Column('child_id', Integer, ForeignKey('widget.id')),
            UniqueConstraint('parent_id', 'child_id'),
        )
    def test_basic(self):
        class Widget(Base):
            pass

        mapper(Widget, widget, properties={
            'children': relation(Widget, secondary=widget_rel,
                primaryjoin=widget_rel.c.parent_id==widget.c.id,
                secondaryjoin=widget_rel.c.child_id==widget.c.id,
                lazy=False, join_depth=1,
            )
        })

        sess = create_session()
        w1 = Widget(name=u'w1')
        w2 = Widget(name=u'w2')
        w1.children.append(w2)
        sess.save(w1)
        sess.flush()
        sess.clear()

#        l = sess.query(Widget).filter(Widget.name=='w1').all()
#        print l
        assert [Widget(name='w1', children=[Widget(name='w2')])] == sess.query(Widget).filter(Widget.name==u'w1').all()

class CyclicalInheritingEagerTest(ORMTest):
    def define_tables(self, metadata):
        global t1, t2
        t1 = Table('t1', metadata,
            Column('c1', Integer, primary_key=True),
            Column('c2', String(30)),
            Column('type', String(30))
            )

        t2 = Table('t2', metadata,
            Column('c1', Integer, primary_key=True),
            Column('c2', String(30)),
            Column('type', String(30)),
            Column('t1.id', Integer, ForeignKey('t1.c1')))

    def test_basic(self):
        class T(object):
            pass

        class SubT(T):
            pass

        class T2(object):
            pass

        class SubT2(T2):
            pass

        mapper(T, t1, polymorphic_on=t1.c.type, polymorphic_identity='t1')
        mapper(SubT, None, inherits=T, polymorphic_identity='subt1', properties={
            't2s':relation(SubT2, lazy=False, backref=backref('subt', lazy=False))
        })
        mapper(T2, t2, polymorphic_on=t2.c.type, polymorphic_identity='t2')
        mapper(SubT2, None, inherits=T2, polymorphic_identity='subt2')

        # testing a particular endless loop condition in eager join setup
        create_session().query(SubT).all()

class SubqueryTest(ORMTest):
    def define_tables(self, metadata):
        global users_table, tags_table
        
        users_table = Table('users', metadata, 
            Column('id', Integer, primary_key=True),
            Column('name', String(16))
        )

        tags_table = Table('tags', metadata,
            Column('id', Integer, primary_key=True),
            Column('user_id', Integer, ForeignKey("users.id")),
            Column('score1', Float),
            Column('score2', Float),
        )

    def test_label_anonymizing(self):
        """test that eager loading works with subqueries with labels, 
        even if an explicit labelname which conflicts with a label on the parent.
        
        There's not much reason a column_property() would ever need to have a label
        of a specific name (and they don't even need labels these days), 
        unless you'd like the name to line up with a name
        that you may be using for a straight textual statement used for loading
        instances of that type.
        
        """
        class User(Base):
            def prop_score(self):
                return sum([tag.prop_score for tag in self.tags])
            prop_score = property(prop_score)

        class Tag(Base):
            def prop_score(self):
                return self.score1 * self.score2
            prop_score = property(prop_score)
        
        for labeled, labelname in [(True, 'score'), (True, None), (False, None)]:
            clear_mappers()
            
            tag_score = (tags_table.c.score1 * tags_table.c.score2)
            user_score = select([func.sum(tags_table.c.score1 *
                                          tags_table.c.score2)],
                                tags_table.c.user_id == users_table.c.id)
            
            if labeled:
                tag_score = tag_score.label(labelname)
                user_score = user_score.label(labelname)
            else:
                user_score = user_score.as_scalar()
            
            mapper(Tag, tags_table, properties={
                'query_score': column_property(tag_score),
            })


            mapper(User, users_table, properties={
                'tags': relation(Tag, backref='user', lazy=False), 
                'query_score': column_property(user_score),
            })

            session = create_session()
            session.save(User(name='joe', tags=[Tag(score1=5.0, score2=3.0), Tag(score1=55.0, score2=1.0)]))
            session.save(User(name='bar', tags=[Tag(score1=5.0, score2=4.0), Tag(score1=50.0, score2=1.0), Tag(score1=15.0, score2=2.0)]))
            session.flush()
            session.clear()

            def go():
                for user in session.query(User).all():
                    self.assertEquals(user.query_score, user.prop_score)
            self.assert_sql_count(testing.db, go, 1)


            # fails for non labeled (fixed in 0.5):
            if labeled:
                def go():
                    u = session.query(User).filter_by(name='joe').one()
                    self.assertEquals(u.query_score, u.prop_score)
                self.assert_sql_count(testing.db, go, 1)
            else:
                u = session.query(User).filter_by(name='joe').one()
                self.assertEquals(u.query_score, u.prop_score)
            
            for t in (tags_table, users_table):
                t.delete().execute()
            
if __name__ == '__main__':
    testenv.main()
