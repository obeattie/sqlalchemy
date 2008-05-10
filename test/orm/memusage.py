import testenv; testenv.configure_for_tests()
import gc
from sqlalchemy.orm import mapper, relation, create_session, clear_mappers
from sqlalchemy.orm.mapper import _mapper_registry
from sqlalchemy.orm.session import _sessions

from testlib import testing
from testlib.sa import MetaData, Table, Column, Integer, String, ForeignKey
from orm import _base


class A(_base.ComparableEntity):
    pass
class B(_base.ComparableEntity):
    pass

def profile_memory(func):
    # run the test 50 times.  if length of gc.get_objects()
    # keeps growing, assert false
    def profile(*args):
        samples = [0 for x in range(0, 50)]
        for x in range(0, 50):
            func(*args)
            gc.collect()
            samples[x] = len(gc.get_objects())
        print "sample gc sizes:", samples

        assert len(_sessions) == 0
        
        # look in the last 20 entries.  we look for one of two patterns: 
        # either "flatline", i.e. 103240, 103240, 103240, 103240, ....
        # or "adjusting down", i.e. 103240, 103248, 103256, 103104, 103112, ....
        
        for i in range(len(samples) - 20, len(samples)):
            # adjusting down
            if samples[i] < samples[i-1]:
                break
        else:
            # no adjusting down.  check for "flatline"
            for i in range(len(samples) - 20, len(samples)):
                if samples[i] > samples[i-1]:
                    assert False, repr(samples) +  " %d > %d"  % (samples[i], samples[i-1])
    return profile

def assert_no_mappers():
    clear_mappers()
    gc.collect()
    assert len(_mapper_registry) == 0

class EnsureZeroed(_base.ORMTest):
    def setUp(self):
        _sessions.clear()
        _mapper_registry.clear()

class MemUsageTest(EnsureZeroed):
    
    # ensure a pure growing test trips the assertion
    @testing.fails_if(lambda:True)
    def test_fixture(self):
        class Foo(object):
            pass
            
        x = []
        @profile_memory
        def go():
            x[-1:] = [Foo(), Foo(), Foo(), Foo(), Foo(), Foo()]
        go()
            
    def test_session(self):
        metadata = MetaData(testing.db)

        table1 = Table("mytable", metadata,
            Column('col1', Integer, primary_key=True),
            Column('col2', String(30)))

        table2 = Table("mytable2", metadata,
            Column('col1', Integer, primary_key=True),
            Column('col2', String(30)),
            Column('col3', Integer, ForeignKey("mytable.col1")))

        metadata.create_all()

        m1 = mapper(A, table1, properties={
            "bs":relation(B, cascade="all, delete", order_by=table2.c.col1)},
            order_by=table1.c.col1)
        m2 = mapper(B, table2)

        m3 = mapper(A, table1, non_primary=True)

        @profile_memory
        def go():
            sess = create_session()
            a1 = A(col2="a1")
            a2 = A(col2="a2")
            a3 = A(col2="a3")
            a1.bs.append(B(col2="b1"))
            a1.bs.append(B(col2="b2"))
            a3.bs.append(B(col2="b3"))
            for x in [a1,a2,a3]:
                sess.add(x)
            sess.flush()
            sess.clear()

            alist = sess.query(A).all()
            self.assertEquals(
                [
                    A(col2="a1", bs=[B(col2="b1"), B(col2="b2")]),
                    A(col2="a2", bs=[]),
                    A(col2="a3", bs=[B(col2="b3")])
                ],
                alist)

            for a in alist:
                sess.delete(a)
            sess.flush()
        go()

        metadata.drop_all()
        del m1, m2, m3
        assert_no_mappers()

    def test_mapper_reset(self):
        metadata = MetaData(testing.db)

        table1 = Table("mytable", metadata,
            Column('col1', Integer, primary_key=True),
            Column('col2', String(30)))

        table2 = Table("mytable2", metadata,
            Column('col1', Integer, primary_key=True),
            Column('col2', String(30)),
            Column('col3', Integer, ForeignKey("mytable.col1")))

        @profile_memory
        def go():
            m1 = mapper(A, table1, properties={
                "bs":relation(B, order_by=table2.c.col1)
            })
            m2 = mapper(B, table2)

            m3 = mapper(A, table1, non_primary=True)

            sess = create_session()
            a1 = A(col2="a1")
            a2 = A(col2="a2")
            a3 = A(col2="a3")
            a1.bs.append(B(col2="b1"))
            a1.bs.append(B(col2="b2"))
            a3.bs.append(B(col2="b3"))
            for x in [a1,a2,a3]:
                sess.add(x)
            sess.flush()
            sess.clear()

            alist = sess.query(A).order_by(A.col1).all()
            self.assertEquals(
                [
                    A(col2="a1", bs=[B(col2="b1"), B(col2="b2")]),
                    A(col2="a2", bs=[]),
                    A(col2="a3", bs=[B(col2="b3")])
                ],
                alist)

            for a in alist:
                sess.delete(a)
            sess.flush()
            sess.close()
            clear_mappers()

        metadata.create_all()
        try:
            go()
        finally:
            metadata.drop_all()
        assert_no_mappers()

    def test_with_inheritance(self):
        metadata = MetaData(testing.db)

        table1 = Table("mytable", metadata,
            Column('col1', Integer, primary_key=True),
            Column('col2', String(30))
            )

        table2 = Table("mytable2", metadata,
            Column('col1', Integer, ForeignKey('mytable.col1'),
                   primary_key=True),
            Column('col3', String(30)),
            )

        @profile_memory
        def go():
            class A(_base.ComparableEntity):
                pass
            class B(A):
                pass

            mapper(A, table1,
                   polymorphic_on=table1.c.col2,
                   polymorphic_identity='a')
            mapper(B, table2,
                   inherits=A,
                   polymorphic_identity='b')

            sess = create_session()
            a1 = A()
            a2 = A()
            b1 = B(col3='b1')
            b2 = B(col3='b2')
            for x in [a1,a2,b1, b2]:
                sess.add(x)
            sess.flush()
            sess.clear()

            alist = sess.query(A).order_by(A.col1).all()
            self.assertEquals(
                [
                    A(), A(), B(col3='b1'), B(col3='b2')
                ],
                alist)

            for a in alist:
                sess.delete(a)
            sess.flush()

            # dont need to clear_mappers()
            del B
            del A

        metadata.create_all()
        try:
            go()
        finally:
            metadata.drop_all()
        assert_no_mappers()

    def test_with_manytomany(self):
        metadata = MetaData(testing.db)

        table1 = Table("mytable", metadata,
            Column('col1', Integer, primary_key=True),
            Column('col2', String(30))
            )

        table2 = Table("mytable2", metadata,
            Column('col1', Integer, primary_key=True),
            Column('col2', String(30)),
            )

        table3 = Table('t1tot2', metadata,
            Column('t1', Integer, ForeignKey('mytable.col1')),
            Column('t2', Integer, ForeignKey('mytable2.col1')),
            )

        @profile_memory
        def go():
            class A(_base.ComparableEntity):
                pass
            class B(_base.ComparableEntity):
                pass

            mapper(A, table1, properties={
                'bs':relation(B, secondary=table3, backref='as', order_by=table3.c.t1)
            })
            mapper(B, table2)

            sess = create_session()
            a1 = A(col2='a1')
            a2 = A(col2='a2')
            b1 = B(col2='b1')
            b2 = B(col2='b2')
            a1.bs.append(b1)
            a2.bs.append(b2)
            for x in [a1,a2]:
                sess.add(x)
            sess.flush()
            sess.clear()

            alist = sess.query(A).order_by(A.col1).all()
            self.assertEquals(
                [
                    A(bs=[B(col2='b1')]), A(bs=[B(col2='b2')])
                ],
                alist)

            for a in alist:
                sess.delete(a)
            sess.flush()

            # dont need to clear_mappers()
            del B
            del A

        metadata.create_all()
        try:
            go()
        finally:
            metadata.drop_all()
        assert_no_mappers()


if __name__ == '__main__':
    testenv.main()
