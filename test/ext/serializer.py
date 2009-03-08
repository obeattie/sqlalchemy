import testenv; testenv.configure_for_tests()

from sqlalchemy.ext import serializer
from sqlalchemy import exc
from testlib import sa, testing
from testlib.sa import MetaData, Table, Column, Integer, String, ForeignKey, select, desc, func, util
from testlib.sa.orm import relation, sessionmaker, scoped_session, class_mapper, mapper, eagerload, compile_mappers, aliased
from testlib.testing import eq_
from orm._base import ComparableEntity, MappedTest


class User(ComparableEntity):
    pass

class Address(ComparableEntity):
    pass

class SerializeTest(testing.ORMTest):
    keep_mappers = True
    keep_data = True
    
    def define_tables(self, metadata):
        global users, addresses
        users = Table('users', metadata, 
            Column('id', Integer, primary_key=True),
            Column('name', String(50))
        )
        addresses = Table('addresses', metadata, 
            Column('id', Integer, primary_key=True),
            Column('email', String(50)),
            Column('user_id', Integer, ForeignKey('users.id')),
        )

    def setup_mappers(self):
        global Session
        Session = scoped_session(sessionmaker())

        mapper(User, users, properties={
            'addresses':relation(Address, backref='user', order_by=addresses.c.id)
        })
        mapper(Address, addresses)

        compile_mappers()
        
    def insert_data(self):
        params = [dict(zip(('id', 'name'), column_values)) for column_values in 
            [(7, 'jack'),
            (8, 'ed'),
            (9, 'fred'),
            (10, 'chuck')]
        ]
        users.insert().execute(params)
    
        addresses.insert().execute(
            [dict(zip(('id', 'user_id', 'email'), column_values)) for column_values in 
                [(1, 7, "jack@bean.com"),
                (2, 8, "ed@wood.com"),
                (3, 8, "ed@bettyboop.com"),
                (4, 8, "ed@lala.com"),
                (5, 9, "fred@fred.com")]
            ]
        )
    
    def test_tables(self):
        assert serializer.loads(serializer.dumps(users), users.metadata, Session) is users

    def test_columns(self):
        assert serializer.loads(serializer.dumps(users.c.name), users.metadata, Session) is users.c.name
        
    def test_mapper(self):
        user_mapper = class_mapper(User)
        assert serializer.loads(serializer.dumps(user_mapper), None, None) is user_mapper
    
    def test_attribute(self):
        assert serializer.loads(serializer.dumps(User.name), None, None) is User.name
    
    def test_expression(self):
        
        expr = select([users]).select_from(users.join(addresses)).limit(5)
        re_expr = serializer.loads(serializer.dumps(expr), users.metadata, None)
        eq_(
            str(expr), 
            str(re_expr)
        )
        
        assert re_expr.bind is testing.db
        eq_(
            re_expr.execute().fetchall(),
            [(7, u'jack'), (8, u'ed'), (8, u'ed'), (8, u'ed'), (9, u'fred')]
        )
    
    # fails due to pure Python pickle bug:  http://bugs.python.org/issue998998
    @testing.fails_if(lambda: util.py3k) 
    def test_query(self):
        q = Session.query(User).filter(User.name=='ed').options(eagerload(User.addresses))
        eq_(q.all(), [User(name='ed', addresses=[Address(id=2), Address(id=3), Address(id=4)])])
        
        q2 = serializer.loads(serializer.dumps(q), users.metadata, Session)
        def go():
            eq_(q2.all(), [User(name='ed', addresses=[Address(id=2), Address(id=3), Address(id=4)])])
        self.assert_sql_count(testing.db, go, 1)
        
        eq_(q2.join(User.addresses).filter(Address.email=='ed@bettyboop.com').value(func.count('*')), 1)

        u1 = Session.query(User).get(8)
        
        q = Session.query(Address).filter(Address.user==u1).order_by(desc(Address.email))
        q2 = serializer.loads(serializer.dumps(q), users.metadata, Session)
        
        eq_(q2.all(), [Address(email='ed@wood.com'), Address(email='ed@lala.com'), Address(email='ed@bettyboop.com')])
        
        q = Session.query(User).join(User.addresses).filter(Address.email.like('%fred%'))
        q2 = serializer.loads(serializer.dumps(q), users.metadata, Session)
        eq_(q2.all(), [User(name='fred')])
        
        eq_(list(q2.values(User.id, User.name)), [(9, u'fred')])

    def test_aliases(self):
        u7, u8, u9, u10 = Session.query(User).order_by(User.id).all()

        ualias = aliased(User)
        q = Session.query(User, ualias).join((ualias, User.id < ualias.id)).filter(User.id<9).order_by(User.id, ualias.id)
        eq_(list(q.all()), [(u7, u8), (u7, u9), (u7, u10), (u8, u9), (u8, u10)])

        q2 = serializer.loads(serializer.dumps(q), users.metadata, Session)
        
        eq_(list(q2.all()), [(u7, u8), (u7, u9), (u7, u10), (u8, u9), (u8, u10)])

    def test_any(self):
        r = User.addresses.any(Address.email=='x')
        ser = serializer.dumps(r)
        x = serializer.loads(ser, users.metadata)
        eq_(str(r), str(x))
        
if __name__ == '__main__':
    testing.main()
