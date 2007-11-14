"""tests general mapper operations with an emphasis on selecting/loading"""

import testbase
from sqlalchemy import *
from sqlalchemy import exceptions, sql
from sqlalchemy.orm import *
from sqlalchemy.ext.sessioncontext import SessionContext, SessionContextExt
from testlib import *
from testlib.tables import *
import testlib.tables as tables


class MapperSuperTest(AssertMixin):
    def setUpAll(self):
        tables.create()
        tables.data()
    def tearDownAll(self):
        tables.drop()
    def tearDown(self):
        clear_mappers()
    def setUp(self):
        pass

class MapperTest(MapperSuperTest):

    def test_propconflict(self):
        """test that a backref created against an existing mapper with a property name
        conflict raises a decent error message"""
        mapper(Address, addresses)
        mapper(User, users,
            properties={
            'addresses':relation(Address, backref='email_address')
        })
        try:
            class_mapper(Address)
            class_mapper(User)
            assert False
        except exceptions.ArgumentError:
            pass

    def test_prop_accessor(self):
        mapper(User, users)
        try:
            class_mapper(User).properties
            assert False
        except NotImplementedError, uoe:
            assert str(uoe) == "Public collection of MapperProperty objects is provided by the get_property() and iterate_properties accessors."

    def test_badcascade(self):
        mapper(Address, addresses)
        try:
            mapper(User, users, properties={'addresses':relation(Address, cascade="fake, all, delete-orphan")})
            assert False
        except exceptions.ArgumentError, e:
            assert str(e) == "Invalid cascade option 'fake'"

    def test_columnprefix(self):
        mapper(User, users, column_prefix='_', properties={
            'user_name':synonym('_user_name')
        })

        s = create_session()
        u = s.get(User, 7)
        assert u._user_name=='jack'
    	assert u._user_id ==7
        assert not hasattr(u, 'user_name')
        u2 = s.query(User).filter_by(user_name='jack').one()
        assert u is u2

    def test_refresh(self):
        mapper(User, users, properties={'addresses':relation(mapper(Address, addresses), backref='user')})
        s = create_session()
        u = s.get(User, 7)
        u.user_name = 'foo'
        a = Address()
        assert object_session(a) is None
        u.addresses.append(a)

        self.assert_(a in u.addresses)

        s.refresh(u)

        # its refreshed, so not dirty
        self.assert_(u not in s.dirty)

        # username is back to the DB
        self.assert_(u.user_name == 'jack')

        self.assert_(a not in u.addresses)

        u.user_name = 'foo'
        u.addresses.append(a)
        # now its dirty
        self.assert_(u in s.dirty)
        self.assert_(u.user_name == 'foo')
        self.assert_(a in u.addresses)
        s.expire(u)

        # get the attribute, it refreshes
        self.assert_(u.user_name == 'jack')
        self.assert_(a not in u.addresses)

    def test_compileonsession(self):
        m = mapper(User, users)
        session = create_session()
        session.connection(m)

    def test_expirecascade(self):
        mapper(User, users, properties={'addresses':relation(mapper(Address, addresses), cascade="all, refresh-expire")})
        s = create_session()
        u = s.get(User, 8)
        u.addresses[0].email_address = 'someotheraddress'
        s.expire(u)
        assert u.addresses[0].email_address == 'ed@wood.com'

    def test_refreshwitheager(self):
        """test that a refresh/expire operation loads rows properly and sends correct "isnew" state to eager loaders"""
        mapper(User, users, properties={'addresses':relation(mapper(Address, addresses), lazy=False)})
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

    def test_incompletecolumns(self):
        """test loading from a select which does not contain all columns"""
        mapper(Address, addresses)
        s = create_session()
        a = s.query(Address).from_statement(select([addresses.c.address_id, addresses.c.user_id])).first()
        assert a.user_id == 7
        assert a.address_id == 1
        assert a.email_address is None

    def test_badconstructor(self):
        """test that if the construction of a mapped class fails, the instnace does not get placed in the session"""
        class Foo(object):
            def __init__(self, one, two):
                pass
        mapper(Foo, users)
        sess = create_session()
        try:
            Foo('one', _sa_session=sess)
            assert False
        except:
            assert len(list(sess)) == 0
        try:
            Foo('one')
            assert False
        except TypeError, e:
            pass

    def test_constructorexceptions(self):
        """test that exceptions raised in the mapped class are not masked by sa decorations"""
        ex = AssertionError('oops')
        sess = create_session()

        class Foo(object):
            def __init__(self):
                raise ex
        mapper(Foo, users)

        try:
            Foo()
            assert False
        except Exception, e:
            assert e is ex

        clear_mappers()
        mapper(Foo, users, extension=SessionContextExt(SessionContext()))
        def bad_expunge(foo):
            raise Exception("this exception should be stated as a warning")

        import warnings
        warnings.filterwarnings("always", r".*this exception should be stated as a warning")

        sess.expunge = bad_expunge
        try:
            Foo(_sa_session=sess)
            assert False
        except Exception, e:
            assert e is ex

    def test_refresh_lazy(self):
        """test that when a lazy loader is set as a trigger on an object's attribute (at the attribute level, not the class level), a refresh() operation doesnt fire the lazy loader or create any problems"""
        s = create_session()
        mapper(User, users, properties={'addresses':relation(mapper(Address, addresses))})
        q2 = s.query(User).options(lazyload('addresses'))
        u = q2.selectfirst(users.c.user_id==8)
        def go():
            s.refresh(u)
        self.assert_sql_count(testbase.db, go, 1)

    def test_expire(self):
        """test the expire function"""
        s = create_session()
        mapper(User, users, properties={'addresses':relation(mapper(Address, addresses), lazy=False)})
        u = s.get(User, 7)
        assert(len(u.addresses) == 1)
        u.user_name = 'foo'
        del u.addresses[0]
        s.expire(u)
        # test plain expire
        self.assert_(u.user_name =='jack')
        self.assert_(len(u.addresses) == 1)

        # we're changing the database here, so if this test fails in the middle,
        # it'll screw up the other tests which are hardcoded to 7/'jack'
        u.user_name = 'foo'
        s.flush()
        # change the value in the DB
        users.update(users.c.user_id==7, values=dict(user_name='jack')).execute()
        s.expire(u)
        # object isnt refreshed yet, using dict to bypass trigger
        self.assert_(u.__dict__.get('user_name') != 'jack')
        # do a select
        s.query(User).select()
        # test that it refreshed
        self.assert_(u.__dict__['user_name'] == 'jack')

        # object should be back to normal now,
        # this should *not* produce a SELECT statement (not tested here though....)
        self.assert_(u.user_name =='jack')

    @testing.fails_on('maxdb')
    def test_refresh2(self):
        """test a hang condition that was occuring on expire/refresh"""

        s = create_session()
        m1 = mapper(Address, addresses)

        m2 = mapper(User, users, properties = dict(addresses=relation(Address,private=True,lazy=False)) )
        u=User()
        u.user_name='Justin'
        a = Address()
        a.address_id=17  # to work around the hardcoded IDs in this test suite....
        u.addresses.append(a)
        s.flush()
        s.clear()
        u = s.query(User).selectfirst()
        print u.user_name

        #ok so far
        s.expire(u)        #hangs when
        print u.user_name #this line runs

        s.refresh(u) #hangs

    def test_props(self):
        m = mapper(User, users, properties = {
            'addresses' : relation(mapper(Address, addresses))
        }).compile()
        self.assert_(User.addresses.property is m.get_property('addresses'))

    def test_compileonprop(self):
        mapper(User, users, properties = {
            'addresses' : relation(mapper(Address, addresses))
        })
        User.addresses.any(Address.email_address=='foo@bar.com')
        clear_mappers()

        mapper(User, users, properties = {
            'addresses' : relation(mapper(Address, addresses))
        })
        assert (User.user_id==3).compare(users.c.user_id==3)

        clear_mappers()

        class Foo(User):pass
        mapper(User, users)
        mapper(Foo, addresses, inherits=User)
        assert getattr(Foo().__class__, 'user_name').impl is not None

    def test_addproperty(self):
        m = mapper(User, users)
        mapper(Address, addresses)
        m.add_property('user_name', deferred(users.c.user_name))
        m.add_property('name', synonym('user_name'))
        m.add_property('addresses', relation(Address))

        sess = create_session(transactional=True)
        assert sess.query(User).get(7)

        u = sess.query(User).filter_by(name='jack').one()

        def go():
            self.assert_result([u], User, user_address_result[0])
            assert u.user_name == 'jack'

        self.assert_sql_count(testbase.db, go, 2)

        u3 = User()
        u3.user_name = 'some user'
        sess.save(u3)
        sess.flush()
        sess.rollback()

    def test_propfilters(self):
        t = Table('person', MetaData(),
                  Column('id', Integer, primary_key=True),
                  Column('type', String),
                  Column('name', String),
                  Column('employee_number', Integer),
                  Column('boss_id', Integer, ForeignKey('person.id')),
                  Column('vendor_id', Integer))

        class Person(object): pass
        class Vendor(Person): pass
        class Employee(Person): pass
        class Manager(Employee): pass
        class Hoho(object): pass
        class Lala(object): pass

        p_m = mapper(Person, t, polymorphic_on=t.c.type,
                     include_properties=('id', 'type', 'name'))
        e_m = mapper(Employee, inherits=p_m, polymorphic_identity='employee',
          properties={
            'boss': relation(Manager, backref='peon')
          },
          exclude_properties=('vendor_id',))

        m_m = mapper(Manager, inherits=e_m, polymorphic_identity='manager',
                     include_properties=())

        v_m = mapper(Vendor, inherits=p_m, polymorphic_identity='vendor',
                     exclude_properties=('boss_id', 'employee_number'))
        h_m = mapper(Hoho, t, include_properties=('id', 'type', 'name'))
        l_m = mapper(Lala, t, exclude_properties=('vendor_id', 'boss_id'),
                     column_prefix="p_")

        p_m.compile()
        #compile_mappers()

        def assert_props(cls, want):
            have = set([n for n in dir(cls) if not n.startswith('_')])
            want = set(want)
            want.add('c')
            self.assert_(have == want, repr(have) + " " + repr(want))

        assert_props(Person, ['id', 'name', 'type'])
        assert_props(Employee, ['boss', 'boss_id', 'employee_number',
                                'id', 'name', 'type'])
        assert_props(Manager, ['boss', 'boss_id', 'employee_number', 'peon',
                               'id', 'name', 'type'])
        assert_props(Vendor, ['vendor_id', 'id', 'name', 'type'])
        assert_props(Hoho, ['id', 'name', 'type'])
        assert_props(Lala, ['p_employee_number', 'p_id', 'p_name', 'p_type'])

    def test_recursiveselectby(self):
        """test that no endless loop occurs when traversing for select_by"""
        m = mapper(User, users, properties={
            'orders':relation(mapper(Order, orders), backref='user'),
            'addresses':relation(mapper(Address, addresses), backref='user'),
        })
        q = create_session().query(m)
        q.select_by(email_address='foo')

    def test_mappingtojoin(self):
        """test mapping to a join"""
        usersaddresses = sql.join(users, addresses, users.c.user_id == addresses.c.user_id)
        m = mapper(User, usersaddresses, primary_key=[users.c.user_id])
        q = create_session().query(m)
        l = q.select()
        self.assert_result(l, User, *user_result[0:2])

    def test_mappingtojoinnopk(self):
        metadata = MetaData()
        account_ids_table = Table('account_ids', metadata,
                Column('account_id', Integer, primary_key=True),
                Column('username', String(20)))
        account_stuff_table = Table('account_stuff', metadata,
                Column('account_id', Integer, ForeignKey('account_ids.account_id')),
                Column('credit', Numeric))
        class A(object):pass
        m = mapper(A, account_ids_table.join(account_stuff_table))
        m.compile()
        assert m._has_pks(account_ids_table)
        assert not m._has_pks(account_stuff_table)
        metadata.create_all(testbase.db)
        try:
            sess = create_session(bind=testbase.db)
            a = A()
            sess.save(a)
            sess.flush()
            assert testbase.db.execute(account_ids_table.count()).scalar() == 1
            assert testbase.db.execute(account_stuff_table.count()).scalar() == 0
        finally:
            metadata.drop_all(testbase.db)

    def test_mappingtoouterjoin(self):
        """test mapping to an outer join, with a composite primary key that allows nulls"""
        result = [
        {'user_id' : 7, 'address_id' : 1},
        {'user_id' : 8, 'address_id' : 2},
        {'user_id' : 8, 'address_id' : 3},
        {'user_id' : 8, 'address_id' : 4},
        {'user_id' : 9, 'address_id':None}
        ]

        j = join(users, addresses, isouter=True)
        m = mapper(User, j, allow_null_pks=True, primary_key=[users.c.user_id, addresses.c.address_id])
        q = create_session().query(m)
        l = q.select()
        self.assert_result(l, User, *result)


    def test_customjoin(self):
        """test that the from_obj parameter to query.select() can be used
        to totally replace the FROM parameters of the generated query."""

        m = mapper(User, users, properties={
            'orders':relation(mapper(Order, orders, properties={
                'items':relation(mapper(Item, orderitems))
            }))
        })

        q = create_session().query(m)
        l = q.select((orderitems.c.item_name=='item 4'), from_obj=[users.join(orders).join(orderitems)])
        self.assert_result(l, User, user_result[0])

    def test_orderby(self):
        """test ordering at the mapper and query level"""
        # TODO: make a unit test out of these various combinations
#        m = mapper(User, users, order_by=desc(users.c.user_name))
        mapper(User, users, order_by=None)
#        mapper(User, users)

#        l = create_session().query(User).select(order_by=[desc(users.c.user_name), asc(users.c.user_id)])
        l = create_session().query(User).select()
#        l = create_session().query(User).select(order_by=[])
#        l = create_session().query(User).select(order_by=None)


    @testing.unsupported('firebird')
    def test_function(self):
        """test mapping to a SELECT statement that has functions in it."""
        s = select([users, (users.c.user_id * 2).label('concat'), func.count(addresses.c.address_id).label('count')],
        users.c.user_id==addresses.c.user_id, group_by=[c for c in users.c]).alias('myselect')
        mapper(User, s)
        sess = create_session()
        l = sess.query(User).select()
        for u in l:
            print "User", u.user_id, u.user_name, u.concat, u.count
        assert l[0].concat == l[0].user_id * 2 == 14
        assert l[1].concat == l[1].user_id * 2 == 16

    @testing.unsupported('firebird')
    def test_count(self):
        """test the count function on Query.

        (why doesnt this work on firebird?)"""
        mapper(User, users)
        q = create_session().query(User)
        self.assert_(q.count()==3)
        self.assert_(q.count(users.c.user_id.in_([8,9]))==2)
        self.assert_(q.count_by(user_name='fred')==1)

    def test_manytomany_count(self):
        mapper(Item, orderitems, properties = dict(
                keywords = relation(mapper(Keyword, keywords), itemkeywords, lazy = True),
            ))
        q = create_session().query(Item)
        assert q.join('keywords').distinct().count(Keyword.c.name=="red") == 2

    def test_override(self):
        # assert that overriding a column raises an error
        try:
            m = mapper(User, users, properties = {
                    'user_name' : relation(mapper(Address, addresses)),
                }).compile()
            self.assert_(False, "should have raised ArgumentError")
        except exceptions.ArgumentError, e:
            self.assert_(True)

        clear_mappers()
        # assert that allow_column_override cancels the error
        m = mapper(User, users, properties = {
                'user_name' : relation(mapper(Address, addresses))
            }, allow_column_override=True)

        clear_mappers()
        # assert that the column being named else where also cancels the error
        m = mapper(User, users, properties = {
                'user_name' : relation(mapper(Address, addresses)),
                'foo' : users.c.user_name,
            })

    def test_synonym(self):
        sess = create_session()
        mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = True),
            uname = synonym('user_name', proxy=True),
            adlist = synonym('addresses', proxy=True),
            adname = synonym('addresses')
        ))

        assert hasattr(User, 'adlist')
        assert not hasattr(User, 'adname')

        u = sess.query(User).get_by(uname='jack')
        self.assert_result(u.adlist, Address, *(user_address_result[0]['addresses'][1]))

        assert hasattr(u, 'adlist')
        assert not hasattr(u, 'adname')

        addr = sess.query(Address).get_by(address_id=user_address_result[0]['addresses'][1][0]['address_id'])
        u = sess.query(User).get_by(adname=addr)
        u2 = sess.query(User).get_by(adlist=addr)
        assert u is u2

        assert u not in sess.dirty
        u.uname = "some user name"
        assert u.uname == "some user name"
        assert u.user_name == "some user name"
        assert u in sess.dirty

    @testing.fails_on('maxdb')
    def test_synonymoptions(self):
        sess = create_session()
        mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = True),
            adlist = synonym('addresses', proxy=True)
        ))

        def go():
            u = sess.query(User).options(eagerload('adlist')).get_by(user_name='jack')
            self.assert_result(u.adlist, Address, *(user_address_result[0]['addresses'][1]))
        self.assert_sql_count(testbase.db, go, 1)

    def test_extensionoptions(self):
        sess  = create_session()
        class ext1(MapperExtension):
            def populate_instance(self, mapper, selectcontext, row, instance, **flags):
                """test options at the Mapper._instance level"""
                instance.TEST = "hello world"
                return EXT_CONTINUE
        mapper(User, users, extension=ext1(), properties={
            'addresses':relation(mapper(Address, addresses), lazy=False)
        })
        class testext(MapperExtension):
            def select_by(self, *args, **kwargs):
                """test options at the Query level"""
                return "HI"
            def populate_instance(self, mapper, selectcontext, row, instance, **flags):
                """test options at the Mapper._instance level"""
                instance.TEST_2 = "also hello world"
                return EXT_CONTINUE
        l = sess.query(User).options(extension(testext())).select_by(x=5)
        assert l == "HI"
        l = sess.query(User).options(extension(testext())).get(7)
        assert l.user_id == 7
        assert l.TEST == "hello world"
        assert l.TEST_2 == "also hello world"
        assert not hasattr(l.addresses[0], 'TEST')
        assert not hasattr(l.addresses[0], 'TEST2')

    def test_eageroptions(self):
        """tests that a lazy relation can be upgraded to an eager relation via the options method"""
        sess = create_session()
        mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = True)
        ))
        l = sess.query(User).options(eagerload('addresses')).select()

        def go():
            self.assert_result(l, User, *user_address_result)
        self.assert_sql_count(testbase.db, go, 0)

    @testing.fails_on('maxdb')
    def test_eageroptionswithlimit(self):
        sess = create_session()
        mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = True)
        ))
        u = sess.query(User).options(eagerload('addresses')).get_by(user_id=8)

        def go():
            assert u.user_id == 8
            assert len(u.addresses) == 3
        self.assert_sql_count(testbase.db, go, 0)

        sess.clear()

        # test that eager loading doesnt modify parent mapper
        def go():
            u = sess.query(User).get_by(user_id=8)
            assert u.user_id == 8
            assert len(u.addresses) == 3
        assert "tbl_row_count" not in self.capture_sql(testbase.db, go)

    @testing.fails_on('maxdb')
    def test_lazyoptionswithlimit(self):
        sess = create_session()
        mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = False)
        ))
        u = sess.query(User).options(lazyload('addresses')).get_by(user_id=8)

        def go():
            assert u.user_id == 8
            assert len(u.addresses) == 3
        self.assert_sql_count(testbase.db, go, 1)

    def test_eagerdegrade(self):
        """tests that an eager relation automatically degrades to a lazy relation if eager columns are not available"""
        sess = create_session()
        usermapper = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = False)
        )).compile()

        # first test straight eager load, 1 statement
        def go():
            l = sess.query(usermapper).select()
            self.assert_result(l, User, *user_address_result)
        self.assert_sql_count(testbase.db, go, 1)

        sess.clear()

        # then select just from users.  run it into instances.
        # then assert the data, which will launch 3 more lazy loads
        # (previous users in session fell out of scope and were removed from session's identity map)
        def go():
            r = users.select().execute()
            l = usermapper.instances(r, sess)
            self.assert_result(l, User, *user_address_result)
        self.assert_sql_count(testbase.db, go, 4)

        clear_mappers()

        sess.clear()

        # test with a deeper set of eager loads.  when we first load the three
        # users, they will have no addresses or orders.  the number of lazy loads when
        # traversing the whole thing will be three for the addresses and three for the
        # orders.
        # (previous users in session fell out of scope and were removed from session's identity map)
        usermapper = mapper(User, users,
            properties = {
                'addresses':relation(mapper(Address, addresses), lazy=False),
                'orders': relation(mapper(Order, orders, properties = {
                    'items' : relation(mapper(Item, orderitems, properties = {
                        'keywords' : relation(mapper(Keyword, keywords), itemkeywords, lazy=False)
                    }), lazy=False)
                }), lazy=False)
            })

        sess.clear()

        # first test straight eager load, 1 statement
        def go():
            l = sess.query(usermapper).select()
            self.assert_result(l, User, *user_all_result)
        self.assert_sql_count(testbase.db, go, 1)

        sess.clear()

        # then select just from users.  run it into instances.
        # then assert the data, which will launch 6 more lazy loads
        def go():
            r = users.select().execute()
            l = usermapper.instances(r, sess)
            self.assert_result(l, User, *user_all_result)
        self.assert_sql_count(testbase.db, go, 7)


    def test_lazyoptions(self):
        """tests that an eager relation can be upgraded to a lazy relation via the options method"""
        sess = create_session()
        mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = False)
        ))
        l = sess.query(User).options(lazyload('addresses')).select()
        def go():
            self.assert_result(l, User, *user_address_result)
        self.assert_sql_count(testbase.db, go, 3)

    def test_latecompile(self):
        """tests mappers compiling late in the game"""

        mapper(User, users, properties = {'orders': relation(Order)})
        mapper(Item, orderitems, properties={'keywords':relation(Keyword, secondary=itemkeywords)})
        mapper(Keyword, keywords)
        mapper(Order, orders, properties={'items':relation(Item)})

        sess = create_session()
        u = sess.query(User).select()
        def go():
            print u[0].orders[1].items[0].keywords[1]
        self.assert_sql_count(testbase.db, go, 3)

    def test_deepoptions(self):
        mapper(User, users,
            properties = {
                'orders': relation(mapper(Order, orders, properties = {
                    'items' : relation(mapper(Item, orderitems, properties = {
                        'keywords' : relation(mapper(Keyword, keywords), itemkeywords)
                    }))
                }))
            })

        sess = create_session()

        # eagerload nothing.
        u = sess.query(User).select()
        def go():
            print u[0].orders[1].items[0].keywords[1]
        self.assert_sql_count(testbase.db, go, 3)
        sess.clear()


        print "-------MARK----------"
        # eagerload orders.items.keywords; eagerload_all() implies eager load of orders, orders.items
        q2 = sess.query(User).options(eagerload_all('orders.items.keywords'))
        u = q2.select()
        def go():
            print u[0].orders[1].items[0].keywords[1]
        print "-------MARK2----------"
        self.assert_sql_count(testbase.db, go, 0)

        sess.clear()

        # same thing, with separate options calls
        q2 = sess.query(User).options(eagerload('orders')).options(eagerload('orders.items')).options(eagerload('orders.items.keywords'))
        u = q2.select()
        def go():
            print u[0].orders[1].items[0].keywords[1]
        print "-------MARK3----------"
        self.assert_sql_count(testbase.db, go, 0)
        print "-------MARK4----------"

        sess.clear()

        # eagerload "keywords" on items.  it will lazy load "orders", then lazy load
        # the "items" on the order, but on "items" it will eager load the "keywords"
        print "-------MARK5----------"
        q3 = sess.query(User).options(eagerload('orders.items.keywords'))
        u = q3.select()
        self.assert_sql_count(testbase.db, go, 2)


class DeferredTest(MapperSuperTest):

    def test_basic(self):
        """tests a basic "deferred" load"""

        m = mapper(Order, orders, properties={
            'description':deferred(orders.c.description)
        })

        o = Order()
        self.assert_(o.description is None)

        q = create_session().query(m)
        def go():
            l = q.select()
            o2 = l[2]
            print o2.description

        orderby = str(orders.default_order_by()[0].compile(bind=testbase.db))
        self.assert_sql(testbase.db, go, [
            ("SELECT orders.order_id AS orders_order_id, orders.user_id AS orders_user_id, orders.isopen AS orders_isopen FROM orders ORDER BY %s" % orderby, {}),
            ("SELECT orders.description AS orders_description FROM orders WHERE orders.order_id = :param_1", {'param_1':3})
        ])

    def test_unsaved(self):
        """test that deferred loading doesnt kick in when just PK cols are set"""
        m = mapper(Order, orders, properties={
            'description':deferred(orders.c.description)
        })

        sess = create_session()
        o = Order()
        sess.save(o)
        o.order_id = 7
        def go():
            o.description = "some description"
        self.assert_sql_count(testbase.db, go, 0)

    def test_unsavedgroup(self):
        """test that deferred loading doesnt kick in when just PK cols are set"""
        m = mapper(Order, orders, properties={
            'description':deferred(orders.c.description, group='primary'),
            'opened':deferred(orders.c.isopen, group='primary')
        })

        sess = create_session()
        o = Order()
        sess.save(o)
        o.order_id = 7
        def go():
            o.description = "some description"
        self.assert_sql_count(testbase.db, go, 0)

    def test_save(self):
        m = mapper(Order, orders, properties={
            'description':deferred(orders.c.description)
        })

        sess = create_session()
        q = sess.query(m)
        l = q.select()
        o2 = l[2]
        o2.isopen = 1
        sess.flush()

    def test_group(self):
        """tests deferred load with a group"""
        m = mapper(Order, orders, properties = {
            'userident':deferred(orders.c.user_id, group='primary'),
            'description':deferred(orders.c.description, group='primary'),
            'opened':deferred(orders.c.isopen, group='primary')
        })
        sess = create_session()
        q = sess.query(m)
        def go():
            l = q.select()
            o2 = l[2]
            print o2.opened, o2.description, o2.userident
            assert o2.opened == 1
            assert o2.userident == 7
            assert o2.description == 'order 3'
        orderby = str(orders.default_order_by()[0].compile(testbase.db))
        self.assert_sql(testbase.db, go, [
            ("SELECT orders.order_id AS orders_order_id FROM orders ORDER BY %s" % orderby, {}),
            ("SELECT orders.user_id AS orders_user_id, orders.description AS orders_description, orders.isopen AS orders_isopen FROM orders WHERE orders.order_id = :param_1", {'param_1':3})
        ])

        o2 = q.select()[2]
#        assert o2.opened == 1
        assert o2.description == 'order 3'
        assert o2 not in sess.dirty
        o2.description = 'order 3'
        def go():
            sess.flush()
        self.assert_sql_count(testbase.db, go, 0)

    def test_commitsstate(self):
        """test that when deferred elements are loaded via a group, they get the proper CommittedState
        and dont result in changes being committed"""

        m = mapper(Order, orders, properties = {
            'userident':deferred(orders.c.user_id, group='primary'),
            'description':deferred(orders.c.description, group='primary'),
            'opened':deferred(orders.c.isopen, group='primary')
        })
        sess = create_session()
        q = sess.query(m)
        o2 = q.select()[2]
        # this will load the group of attributes
        assert o2.description == 'order 3'
        assert o2 not in sess.dirty
        # this will mark it as 'dirty', but nothing actually changed
        o2.description = 'order 3'
        def go():
            # therefore the flush() shouldnt actually issue any SQL
            sess.flush()
        self.assert_sql_count(testbase.db, go, 0)

    def test_options(self):
        """tests using options on a mapper to create deferred and undeferred columns"""
        m = mapper(Order, orders)
        sess = create_session()
        q = sess.query(m)
        q2 = q.options(defer('user_id'))
        def go():
            l = q2.select()
            print l[2].user_id

        orderby = str(orders.default_order_by()[0].compile(testbase.db))
        self.assert_sql(testbase.db, go, [
            ("SELECT orders.order_id AS orders_order_id, orders.description AS orders_description, orders.isopen AS orders_isopen FROM orders ORDER BY %s" % orderby, {}),
            ("SELECT orders.user_id AS orders_user_id FROM orders WHERE orders.order_id = :param_1", {'param_1':3})
        ])
        sess.clear()
        q3 = q2.options(undefer('user_id'))
        def go():
            l = q3.select()
            print l[3].user_id
        self.assert_sql(testbase.db, go, [
            ("SELECT orders.order_id AS orders_order_id, orders.user_id AS orders_user_id, orders.description AS orders_description, orders.isopen AS orders_isopen FROM orders ORDER BY %s" % orderby, {}),
        ])

    def test_undefergroup(self):
        """tests undefer_group()"""
        m = mapper(Order, orders, properties = {
            'userident':deferred(orders.c.user_id, group='primary'),
            'description':deferred(orders.c.description, group='primary'),
            'opened':deferred(orders.c.isopen, group='primary')
        })
        sess = create_session()
        q = sess.query(m)
        def go():
            l = q.options(undefer_group('primary')).select()
            o2 = l[2]
            print o2.opened, o2.description, o2.userident
            assert o2.opened == 1
            assert o2.userident == 7
            assert o2.description == 'order 3'
        orderby = str(orders.default_order_by()[0].compile(testbase.db))
        self.assert_sql(testbase.db, go, [
            ("SELECT orders.user_id AS orders_user_id, orders.description AS orders_description, orders.isopen AS orders_isopen, orders.order_id AS orders_order_id FROM orders ORDER BY %s" % orderby, {}),
        ])
        
    def test_locates_col(self):
        """test that manually adding a col to the result undefers the column"""
        mapper(Order, orders, properties={
            'description':deferred(orders.c.description)
        })

        sess = create_session()
        o1 = sess.query(Order).first()
        def go():
            assert o1.description == 'order 1'
        self.assert_sql_count(testbase.db, go, 1)
        
        sess = create_session()
        o1 = sess.query(Order).add_column(orders.c.description).first()[0]
        def go():
            assert o1.description == 'order 1'
        self.assert_sql_count(testbase.db, go, 0)
        
    def test_deepoptions(self):
        m = mapper(User, users, properties={
            'orders':relation(mapper(Order, orders, properties={
                'items':relation(mapper(Item, orderitems, properties={
                    'item_name':deferred(orderitems.c.item_name)
                }))
            }))
        })
        sess = create_session()
        q = sess.query(m)
        l = q.select()
        item = l[0].orders[1].items[1]
        def go():
            print item.item_name
        self.assert_sql_count(testbase.db, go, 1)
        self.assert_(item.item_name == 'item 4')
        sess.clear()
        q2 = q.options(undefer('orders.items.item_name'))
        l = q2.select()
        item = l[0].orders[1].items[1]
        def go():
            print item.item_name
        self.assert_sql_count(testbase.db, go, 0)
        self.assert_(item.item_name == 'item 4')

class CompositeTypesTest(ORMTest):
    def define_tables(self, metadata):
        global graphs, edges
        graphs = Table('graphs', metadata,
            Column('id', Integer, primary_key=True),
            Column('version_id', Integer, primary_key=True),
            Column('name', String(30)))

        edges = Table('edges', metadata,
            Column('id', Integer, primary_key=True),
            Column('graph_id', Integer, nullable=False),
            Column('graph_version_id', Integer, nullable=False),
            Column('x1', Integer),
            Column('y1', Integer),
            Column('x2', Integer),
            Column('y2', Integer),
            ForeignKeyConstraint(['graph_id', 'graph_version_id'], ['graphs.id', 'graphs.version_id'])
            )

    def test_basic(self):
        class Point(object):
            def __init__(self, x, y):
                self.x = x
                self.y = y
            def __composite_values__(self):
                return [self.x, self.y]
            def __eq__(self, other):
                return other.x == self.x and other.y == self.y
            def __ne__(self, other):
                return not self.__eq__(other)

        class Graph(object):
            pass
        class Edge(object):
            def __init__(self, start, end):
                self.start = start
                self.end = end

        mapper(Graph, graphs, properties={
            'edges':relation(Edge)
        })
        mapper(Edge, edges, properties={
            'start':composite(Point, edges.c.x1, edges.c.y1),
            'end':composite(Point, edges.c.x2, edges.c.y2)
        })

        sess = create_session()
        g = Graph()
        g.id = 1
        g.version_id=1
        g.edges.append(Edge(Point(3, 4), Point(5, 6)))
        g.edges.append(Edge(Point(14, 5), Point(2, 7)))
        sess.save(g)
        sess.flush()

        sess.clear()
        g2 = sess.query(Graph).get([g.id, g.version_id])
        for e1, e2 in zip(g.edges, g2.edges):
            assert e1.start == e2.start
            assert e1.end == e2.end

        g2.edges[1].end = Point(18, 4)
        sess.flush()
        sess.clear()
        e = sess.query(Edge).get(g2.edges[1].id)
        assert e.end == Point(18, 4)

        e.end.x = 19
        e.end.y = 5
        sess.flush()
        sess.clear()
        assert sess.query(Edge).get(g2.edges[1].id).end == Point(19, 5)

        g.edges[1].end = Point(19, 5)

        sess.clear()
        def go():
            g2 = sess.query(Graph).options(eagerload('edges')).get([g.id, g.version_id])
            for e1, e2 in zip(g.edges, g2.edges):
                assert e1.start == e2.start
                assert e1.end == e2.end
        self.assert_sql_count(testbase.db, go, 1)

        # test comparison of CompositeProperties to their object instances
        g = sess.query(Graph).get([1, 1])
        assert sess.query(Edge).filter(Edge.start==Point(3, 4)).one() is g.edges[0]

        assert sess.query(Edge).filter(Edge.start!=Point(3, 4)).first() is g.edges[1]

        assert sess.query(Edge).filter(Edge.start==None).all() == []


    def test_pk(self):
        """test using a composite type as a primary key"""

        class Version(object):
            def __init__(self, id, version):
                self.id = id
                self.version = version
            def __composite_values__(self):
                # a tuple this time
                return (self.id, self.version)
            def __eq__(self, other):
                return other.id == self.id and other.version == self.version
            def __ne__(self, other):
                return not self.__eq__(other)

        class Graph(object):
            def __init__(self, version):
                self.version = version

        mapper(Graph, graphs, properties={
            'version':composite(Version, graphs.c.id, graphs.c.version_id)
        })

        sess = create_session()
        g = Graph(Version(1, 1))
        sess.save(g)
        sess.flush()

        sess.clear()
        g2 = sess.query(Graph).get([1, 1])
        assert g.version == g2.version
        sess.clear()

        g2 = sess.query(Graph).get(Version(1, 1))
        assert g.version == g2.version



class NoLoadTest(MapperSuperTest):
    def test_basic(self):
        """tests a basic one-to-many lazy load"""
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy=None)
        ))
        q = create_session().query(m)
        l = [None]
        def go():
            x = q.select(users.c.user_id == 7)
            x[0].addresses
            l[0] = x
        self.assert_sql_count(testbase.db, go, 1)

        self.assert_result(l[0], User,
            {'user_id' : 7, 'addresses' : (Address, [])},
            )
    def test_options(self):
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy=None)
        ))
        q = create_session().query(m).options(lazyload('addresses'))
        l = [None]
        def go():
            x = q.select(users.c.user_id == 7)
            x[0].addresses
            l[0] = x
        self.assert_sql_count(testbase.db, go, 2)

        self.assert_result(l[0], User,
            {'user_id' : 7, 'addresses' : (Address, [{'address_id' : 1}])},
            )

class MapperExtensionTest(MapperSuperTest):
    def setUpAll(self):
        tables.create()
    def tearDownAll(self):
        tables.drop()
    def tearDown(self):
        clear_mappers()
        tables.delete()
    def setUp(self):
        tables.data()

    def test_create_instance(self):
        class Ext(MapperExtension):
            def create_instance(self, *args, **kwargs):
                return User()
        m = mapper(Address, addresses)
        m = mapper(User, users, extension=Ext(), properties = dict(
            addresses = relation(Address, lazy=True),
        ))

        q = create_session().query(m)
        l = q.select();
        self.assert_result(l, User, *user_address_result)

    def test_methods(self):
        """test that common user-defined methods get called."""

        methods = set()
        class Ext(MapperExtension):
            def load(self, query, *args, **kwargs):
                methods.add('load')
                return EXT_CONTINUE

            def get(self, query, *args, **kwargs):
                methods.add('get')
                return EXT_CONTINUE

            def translate_row(self, mapper, context, row):
                methods.add('translate_row')
                return EXT_CONTINUE

            def create_instance(self, mapper, selectcontext, row, class_):
                methods.add('create_instance')
                return EXT_CONTINUE

            def append_result(self, mapper, selectcontext, row, instance, result, **flags):
                methods.add('append_result')
                return EXT_CONTINUE

            def populate_instance(self, mapper, selectcontext, row, instance, **flags):
                methods.add('populate_instance')
                return EXT_CONTINUE

            def before_insert(self, mapper, connection, instance):
                methods.add('before_insert')
                return EXT_CONTINUE

            def after_insert(self, mapper, connection, instance):
                methods.add('after_insert')
                return EXT_CONTINUE

            def before_update(self, mapper, connection, instance):
                methods.add('before_update')
                return EXT_CONTINUE

            def after_update(self, mapper, connection, instance):
                methods.add('after_update')
                return EXT_CONTINUE

            def before_delete(self, mapper, connection, instance):
                methods.add('before_delete')
                return EXT_CONTINUE

            def after_delete(self, mapper, connection, instance):
                methods.add('after_delete')
                return EXT_CONTINUE

        mapper(User, users, extension=Ext())
        sess = create_session()
        u = User()
        sess.save(u)
        sess.flush()
        u = sess.query(User).load(u.user_id)
        sess.clear()
        u = sess.query(User).get(u.user_id)
        u.user_name = 'foobar'
        sess.flush()
        sess.delete(u)
        sess.flush()
        assert methods == set(['load', 'append_result', 'before_delete', 'create_instance', 'translate_row', 'get',
                'after_delete', 'after_insert', 'before_update', 'before_insert', 'after_update', 'populate_instance'])


class RequirementsTest(AssertMixin):
    """Tests the contract for user classes."""

    def setUpAll(self):
        global metadata, t1, t2, t3, t4, t5, t6

        metadata = MetaData(testbase.db)
        t1 = Table('ht1', metadata,
                   Column('id', Integer, primary_key=True),
                   Column('value', String(10)))
        t2 = Table('ht2', metadata,
                   Column('id', Integer, primary_key=True),
                   Column('ht1_id', Integer, ForeignKey('ht1.id')),
                   Column('value', String(10)))
        t3 = Table('ht3', metadata,
                   Column('id', Integer, primary_key=True),
                   Column('value', String(10)))
        t4 = Table('ht4', metadata,
                   Column('ht1_id', Integer, ForeignKey('ht1.id'),
                          primary_key=True),
                   Column('ht3_id', Integer, ForeignKey('ht3.id'),
                          primary_key=True))
        t5 = Table('ht5', metadata,
                   Column('ht1_id', Integer, ForeignKey('ht1.id'),
                          primary_key=True),
                   Column('ht1_id', Integer, ForeignKey('ht1.id'),
                          primary_key=True))
        t6 = Table('ht6', metadata,
                   Column('ht1a_id', Integer, ForeignKey('ht1.id'),
                          primary_key=True),
                   Column('ht1b_id', Integer, ForeignKey('ht1.id'),
                          primary_key=True),
                   Column('value', String(10)))
        metadata.create_all()

    def setUp(self):
        clear_mappers()

    def tearDownAll(self):
        metadata.drop_all()

    def test_baseclass(self):
        class OldStyle:
            pass

        self.assertRaises(exceptions.ArgumentError, mapper, OldStyle, t1)

        class NoWeakrefSupport(str):
            pass

        # TODO: is weakref support detectable without an instance?
        #self.assertRaises(exceptions.ArgumentError, mapper, NoWeakrefSupport, t2)

    def test_comparison_overrides(self):
        """Simple tests to ensure users can supply comparison __methods__.

        The suite-level test --options are better suited to detect
        problems- they add selected __methods__ across the board on all
        ORM tests.  This test simply shoves a variety of operations
        through the ORM to catch basic regressions early in a standard
        test run.
        """

        # adding these methods directly to each class to avoid decoration
        # by the testlib decorators.
        class H1(object):
            def __init__(self, value='abc'):
                self.value = value
            def __nonzero__(self):
                return False
            def __hash__(self):
                return hash(self.value)
            def __eq__(self, other):
                if isinstance(other, type(self)):
                    return self.value == other.value
                return False
        class H2(object):
            def __init__(self, value='abc'):
                self.value = value
            def __nonzero__(self):
                return False
            def __hash__(self):
                return hash(self.value)
            def __eq__(self, other):
                if isinstance(other, type(self)):
                    return self.value == other.value
                return False
        class H3(object):
            def __init__(self, value='abc'):
                self.value = value
            def __nonzero__(self):
                return False
            def __hash__(self):
                return hash(self.value)
            def __eq__(self, other):
                if isinstance(other, type(self)):
                    return self.value == other.value
                return False
        class H6(object):
            def __init__(self, value='abc'):
                self.value = value
            def __nonzero__(self):
                return False
            def __hash__(self):
                return hash(self.value)
            def __eq__(self, other):
                if isinstance(other, type(self)):
                    return self.value == other.value
                return False

        mapper(H1, t1, properties={
            'h2s': relation(H2, backref='h1'),
            'h3s': relation(H3, secondary=t4, backref='h1s'),
            'h1s': relation(H1, secondary=t5, backref='parent_h1'),
            't6a': relation(H6, backref='h1a',
                            primaryjoin=t1.c.id==t6.c.ht1a_id),
            't6b': relation(H6, backref='h1b',
                            primaryjoin=t1.c.id==t6.c.ht1b_id),
            })
        mapper(H2, t2)
        mapper(H3, t3)
        mapper(H6, t6)

        s = create_session()
        for i in range(3):
            h1 = H1()
            s.save(h1)

        h1.h2s.append(H2())
        h1.h3s.extend([H3(), H3()])
        h1.h1s.append(H1())

        s.flush()

        h6 = H6()
        h6.h1a = h1
        h6.h1b = h1

        h6 = H6()
        h6.h1a = h1
        h6.h1b = H1()

        h6.h1b.h2s.append(H2())

        s.flush()

        h1.h2s.extend([H2(), H2()])
        s.flush()

        h1s = s.query(H1).options(eagerload('h2s')).all()
        self.assertEqual(len(h1s), 5)

        self.assert_unordered_result(h1s, H1,
                                     {'h2s': []},
                                     {'h2s': []},
                                     {'h2s': (H2, [{'value': 'abc'},
                                                   {'value': 'abc'},
                                                   {'value': 'abc'}])},
                                     {'h2s': []},
                                     {'h2s': (H2, [{'value': 'abc'}])})

        h1s = s.query(H1).options(eagerload('h3s')).all()

        self.assertEqual(len(h1s), 5)
        h1s = s.query(H1).options(eagerload_all('t6a.h1b'),
                                  eagerload('h2s'),
                                  eagerload_all('h3s.h1s')).all()
        self.assertEqual(len(h1s), 5)


if __name__ == "__main__":
    testbase.main()
