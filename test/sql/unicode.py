# coding: utf-8
"""verrrrry basic unicode column name testing"""

import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from testlib import *
from testlib.engines import utf8_engine
from sqlalchemy.sql import column

class UnicodeSchemaTest(TestBase):
    __requires__ = ('unicode_ddl',)

    def setUpAll(self):
        global unicode_bind, metadata, t1, t2, t3

        unicode_bind = utf8_engine()

        metadata = MetaData(unicode_bind)
        t1 = Table('unitable1', metadata,
            Column('méil', Integer, primary_key=True),
            Column('\u6e2c\u8a66', Integer),
            test_needs_fk=True,
            )
        t2 = Table('Unitéble2', metadata,
            Column('méil', Integer, primary_key=True, key="a"),
            Column('\u6e2c\u8a66', Integer, ForeignKey('unitable1.méil'),
                   key="b"
                   ),
                   test_needs_fk=True,
            )

        # Few DBs support Unicode foreign keys
        if testing.against('sqlite'):
            t3 = Table('\u6e2c\u8a66', metadata,
                       Column('\u6e2c\u8a66_id', Integer, primary_key=True,
                              autoincrement=False),
                       Column('unitable1_\u6e2c\u8a66', Integer,
                              ForeignKey('unitable1.\u6e2c\u8a66')
                              ),
                       Column('Unitéble2_b', Integer,
                              ForeignKey('Unitéble2.b')
                              ),
                       Column('\u6e2c\u8a66_self', Integer,
                              ForeignKey('\u6e2c\u8a66.\u6e2c\u8a66_id')
                              ),
                       test_needs_fk=True,
                       )
        else:
            t3 = Table('\u6e2c\u8a66', metadata,
                       Column('\u6e2c\u8a66_id', Integer, primary_key=True,
                              autoincrement=False),
                       Column('unitable1_\u6e2c\u8a66', Integer),
                       Column('Unitéble2_b', Integer),
                       Column('\u6e2c\u8a66_self', Integer),
                       test_needs_fk=True,
                       )
        metadata.create_all()

    def tearDown(self):
        if metadata.tables:
            t3.delete().execute()
            t2.delete().execute()
            t1.delete().execute()

    def tearDownAll(self):
        global unicode_bind
        metadata.drop_all()
        del unicode_bind

    def test_insert(self):
        t1.insert().execute({'méil':1, '\u6e2c\u8a66':5})
        t2.insert().execute({'a':1, 'b':1})
        t3.insert().execute({'\u6e2c\u8a66_id': 1,
                             'unitable1_\u6e2c\u8a66': 5,
                             'Unitéble2_b': 1,
                             '\u6e2c\u8a66_self': 1})

        assert t1.select().execute().fetchall() == [(1, 5)]
        assert t2.select().execute().fetchall() == [(1, 1)]
        assert t3.select().execute().fetchall() == [(1, 5, 1, 1)]

    def test_reflect(self):
        t1.insert().execute({'méil':2, '\u6e2c\u8a66':7})
        t2.insert().execute({'a':2, 'b':2})
        t3.insert().execute({'\u6e2c\u8a66_id': 2,
                             'unitable1_\u6e2c\u8a66': 7,
                             'Unitéble2_b': 2,
                             '\u6e2c\u8a66_self': 2})

        meta = MetaData(unicode_bind)
        tt1 = Table(t1.name, meta, autoload=True)
        tt2 = Table(t2.name, meta, autoload=True)
        tt3 = Table(t3.name, meta, autoload=True)

        tt1.insert().execute({'méil':1, '\u6e2c\u8a66':5})
        tt2.insert().execute({'méil':1, '\u6e2c\u8a66':1})
        tt3.insert().execute({'\u6e2c\u8a66_id': 1,
                              'unitable1_\u6e2c\u8a66': 5,
                              'Unitéble2_b': 1,
                              '\u6e2c\u8a66_self': 1})

        self.assert_(tt1.select(order_by=desc('méil')).execute().fetchall() ==
                     [(2, 7), (1, 5)])
        self.assert_(tt2.select(order_by=desc('méil')).execute().fetchall() ==
                     [(2, 2), (1, 1)])
        self.assert_(tt3.select(order_by=desc('\u6e2c\u8a66_id')).
                     execute().fetchall() ==
                     [(2, 7, 2, 2), (1, 5, 1, 1)])
        meta.drop_all()
        metadata.create_all()

class EscapesDefaultsTest(testing.TestBase):
    def test_default_exec(self):
        metadata = MetaData(testing.db)
        t1 = Table('t1', metadata,
            Column('special_col', Integer, Sequence('special_col'), primary_key=True),
            Column('data', String(50)) # to appease SQLite without DEFAULT VALUES
            )
        t1.create()

        try:
            engine = metadata.bind
            
            # reset the identifier preparer, so that we can force it to cache
            # a unicode identifier
            engine.dialect.identifier_preparer = engine.dialect.preparer(engine.dialect)
            select([column('special_col')]).select_from(t1).execute()
            assert isinstance(engine.dialect.identifier_preparer.format_sequence(Sequence('special_col')), str)
            
            # now execute, run the sequence.  it should run in u"Special_col.nextid" or similar as 
            # a unicode object; cx_oracle asserts that this is None or a String (postgres lets it pass thru).
            # ensure that base.DefaultRunner is encoding.
            t1.insert().execute(data='foo')
        finally:
            t1.drop()


if __name__ == '__main__':
    testenv.main()
