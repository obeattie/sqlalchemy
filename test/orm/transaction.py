import testenv; testenv.configure_for_tests()
import operator
from sqlalchemy import *
from sqlalchemy import exc as sa_exc
from sqlalchemy.orm import *
from testlib import *
from testlib.fixtures import *

class AutoExpireTest(FixtureTest):

    keep_mappers = True
    refresh_data = True

    def setup_mappers(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref='user'),
            })
        mapper(Address, addresses)

    def test_attrs_on_rollback(self):
        sess = self.session()
        u1 = sess.get(User, 7)
        u1.name = 'ed'
        sess.rollback()
        self.assertEquals(u1.name, 'jack')
    
    def test_expunge_pending_on_rollback(self):
        sess = self.session()
        u2= User(name='newuser')
        sess.add(u2)
        assert u2 in sess
        sess.rollback()
        assert u2 not in sess
    
    def test_commit_persistent(self):
        sess = self.session()
        u1 = sess.get(User, 7)
        u1.name = 'ed'
        sess.flush()
        sess.commit()
        self.assertEquals(u1.name, 'ed')

    def test_commit_pending(self):
        sess = self.session()
        u1 = User(name='newuser')
        sess.add(u1)
        sess.flush()
        sess.commit()
        self.assertEquals(u1.name, 'newuser')
    
    def session(self):
        return create_session(autoflush=True, autocommit=False, autoexpire=True)

    def test_concurrent_commit_persistent(self):
        s1 = self.session()
        u1 = s1.get(User, 7)
        u1.name = 'ed'
        s1.commit()

        s2 = self.session()
        u2 = s2.get(User, 7)
        assert u2.name == 'ed'
        u2.name = 'will'
        s2.commit()

        assert u1.name == 'will'

    def test_concurrent_commit_pending(self):
        s1 = self.session()
        u1 = User(name='edward')
        s1.add(u1)
        s1.commit()

        s2 = self.session()
        u2 = s2.query(User).filter(User.name=='edward').one()
        u2.name = 'will'
        s2.commit()

        assert u1.name == 'will'

# TODO!  subtransactions
# TODO!  SAVEPOINT transactions
# TODO!  continuing transactions after rollback()

class SavepointTest(FixtureTest):

    keep_mappers = True
    only_tables = True

    def setup_mappers(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref='user'),
            })
        mapper(Address, addresses)

    def session(self):
        return create_session(autoflush=True, autocommit=False, autoexpire=True)
    
    @testing.uses_savepoints()
    def test_basic(self):
        s = self.session()
        u1 = User(name='ed')
        u2 = User(name='jack')
        s.add_all([u1, u2])
        
        s.begin_nested()
        u3 = User(name='wendy')
        u4 = User(name='foo')
        u1.name = 'edward'
        u2.name = 'jackward'
        s.add_all([u3, u4])
        self.assertEquals(s.query(User.name).order_by(User.id).all(), [('edward',), ('jackward',), ('wendy',), ('foo',)])
        s.rollback()
        assert u1.name == 'ed'
        assert u2.name == 'jack'
        self.assertEquals(s.query(User.name).order_by(User.id).all(), [('ed',), ('jack',)])
        s.commit()
        assert u1.name == 'ed'
        assert u2.name == 'jack'
        self.assertEquals(s.query(User.name).order_by(User.id).all(), [('ed',), ('jack',)])
        
        
        
        


class AutocommitTest(TestBase):
    def test_begin_nested_requires_trans(self):
        sess = create_session(autocommit=True)
        self.assertRaises(sa_exc.InvalidRequestError, sess.begin_nested)



if __name__ == '__main__':
    testenv.main()
