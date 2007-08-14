# coding: utf-8
"""verrrrry basic unicode column name testing"""

import testbase
from sqlalchemy import *
from sqlalchemy.orm import mapper, relation, create_session, eagerload
from testlib import *
from testlib.engines import utf8_engine


class UnicodeSchemaTest(PersistTest):
    @testing.unsupported('oracle')
    def setUpAll(self):
        global unicode_bind, metadata, t1, t2, t3

        unicode_bind = utf8_engine()

        metadata = MetaData(unicode_bind)
        t1 = Table('unitable1', metadata,
            Column(u'méil', Integer, primary_key=True),
            Column(u'\u6e2c\u8a66', Integer),
            )
        t2 = Table(u'Unitéble2', metadata,
            Column(u'méil', Integer, primary_key=True, key="a"),
            Column(u'\u6e2c\u8a66', Integer, ForeignKey(u'unitable1.méil'),
                   key="b"),
            )
        t3 = Table(u'\u6e2c\u8a66', metadata,
                   Column(u'\u6e2c\u8a66_id', Integer, primary_key=True,
                          autoincrement=False),
                   Column(u'unitable1_\u6e2c\u8a66', Integer,
                            # lets leave these out for now so that PG tests pass, until
                            # the test can be broken out into a pg-passing version (or we figure it out)
                          #ForeignKey(u'unitable1.\u6e2c\u8a66')
                          ),
                   Column(u'Unitéble2_b', Integer,
                          #ForeignKey(u'Unitéble2.b')
                          ),
                   Column(u'\u6e2c\u8a66_self', Integer,
                          #ForeignKey(u'\u6e2c\u8a66.\u6e2c\u8a66_id')
                          )
                          )
        metadata.create_all()

    @testing.unsupported('oracle')
    def tearDown(self):
        if metadata.tables:
            t3.delete().execute()
            t2.delete().execute()
            t1.delete().execute()
        
    @testing.unsupported('oracle')
    def tearDownAll(self):
        global unicode_bind
        metadata.drop_all()
        del unicode_bind
        
    @testing.unsupported('oracle')
    def test_insert(self):
        t1.insert().execute({u'méil':1, u'\u6e2c\u8a66':5})
        t2.insert().execute({'a':1, 'b':1})
        t3.insert().execute({u'\u6e2c\u8a66_id': 1,
                             u'unitable1_\u6e2c\u8a66': 5,
                             u'Unitéble2_b': 1,
                             u'\u6e2c\u8a66_self': 1})

        assert t1.select().execute().fetchall() == [(1, 5)]
        assert t2.select().execute().fetchall() == [(1, 1)]
        assert t3.select().execute().fetchall() == [(1, 5, 1, 1)]
    
    @testing.unsupported('oracle')
    def test_reflect(self):
        t1.insert().execute({u'méil':2, u'\u6e2c\u8a66':7})
        t2.insert().execute({'a':2, 'b':2})
        t3.insert().execute({u'\u6e2c\u8a66_id': 2,
                             u'unitable1_\u6e2c\u8a66': 7,
                             u'Unitéble2_b': 2,
                             u'\u6e2c\u8a66_self': 2})

        meta = MetaData(unicode_bind)
        tt1 = Table(t1.name, meta, autoload=True)
        tt2 = Table(t2.name, meta, autoload=True)
        tt3 = Table(t3.name, meta, autoload=True)

        tt1.insert().execute({u'méil':1, u'\u6e2c\u8a66':5})
        tt2.insert().execute({u'méil':1, u'\u6e2c\u8a66':1})
        tt3.insert().execute({u'\u6e2c\u8a66_id': 1,
                              u'unitable1_\u6e2c\u8a66': 5,
                              u'Unitéble2_b': 1,
                              u'\u6e2c\u8a66_self': 1})

        self.assert_(tt1.select(order_by=desc(u'méil')).execute().fetchall() ==
                     [(2, 7), (1, 5)])
        self.assert_(tt2.select(order_by=desc(u'méil')).execute().fetchall() ==
                     [(2, 2), (1, 1)])
        self.assert_(tt3.select(order_by=desc(u'\u6e2c\u8a66_id')).
                     execute().fetchall() ==
                     [(2, 7, 2, 2), (1, 5, 1, 1)])
        meta.drop_all()
        metadata.create_all()
        
    @testing.unsupported('oracle')
    def test_mapping(self):
        # TODO: this test should be moved to the ORM tests, tests should be
        # added to this module testing SQL syntax and joins, etc.
        class A(object):pass
        class B(object):pass
        
        mapper(A, t1, properties={
            't2s':relation(B),
            'a':t1.c[u'méil'],
            'b':t1.c[u'\u6e2c\u8a66']
        })
        mapper(B, t2)
        sess = create_session()
        a1 = A()
        b1 = B()
        a1.t2s.append(b1)
        sess.save(a1)
        sess.flush()
        sess.clear()
        new_a1 = sess.query(A).filter(t1.c[u'méil'] == a1.a).one()
        assert new_a1.a == a1.a
        assert new_a1.t2s[0].a == b1.a
        sess.clear()

        new_a1 = sess.query(A).options(eagerload('t2s')).filter(t1.c[u'méil'] == a1.a).one()
        assert new_a1.a == a1.a
        assert new_a1.t2s[0].a == b1.a
        sess.clear()

        new_a1 = sess.query(A).filter(A.a == a1.a).one()
        assert new_a1.a == a1.a
        assert new_a1.t2s[0].a == b1.a
        sess.clear()
        
if __name__ == '__main__':
    testbase.main()
