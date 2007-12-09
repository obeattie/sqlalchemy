import testbase
import gc
from sqlalchemy import MetaData, Integer, String, ForeignKey
from sqlalchemy.orm import mapper, relation, clear_mappers, create_session
from sqlalchemy.orm.mapper import Mapper
from testlib import *
from testlib.fixtures import Base

class A(Base):pass
class B(Base):pass

def profile_memory(func):
    # run the test 50 times.  if length of gc.get_objects()
    # keeps growing, assert false
    def profile(*args):
        samples = []
        for x in range(0, 50):
            func(*args)
            gc.collect()
            samples.append(len(gc.get_objects()))
        print "sample gc sizes:", samples
        # TODO: this test only finds pure "growing" tests
        for i, x in enumerate(samples):
            if i < len(samples) - 1 and samples[i+1] <= x:
                break
        else:
            assert False
        assert True
    return profile

class MemUsageTest(AssertMixin):
    
    def test_session(self):
        metadata = MetaData(testbase.db)

        table1 = Table("mytable", metadata, 
            Column('col1', Integer, primary_key=True),
            Column('col2', String(30))
            )

        table2 = Table("mytable2", metadata, 
            Column('col1', Integer, primary_key=True),
            Column('col2', String(30)),
            Column('col3', Integer, ForeignKey("mytable.col1"))
            )
    
        metadata.create_all()

        m1 = mapper(A, table1, properties={
            "bs":relation(B, cascade="all, delete")
        })
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
                sess.save(x)
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
        clear_mappers()
        
    def test_mapper_reset(self):
        metadata = MetaData(testbase.db)

        table1 = Table("mytable", metadata, 
            Column('col1', Integer, primary_key=True),
            Column('col2', String(30))
            )

        table2 = Table("mytable2", metadata, 
            Column('col1', Integer, primary_key=True),
            Column('col2', String(30)),
            Column('col3', Integer, ForeignKey("mytable.col1"))
            )

        @profile_memory
        def go():
            m1 = mapper(A, table1, properties={
                "bs":relation(B)
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
                sess.save(x)
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
            clear_mappers()
        
        metadata.create_all()
        try:
            go()
        finally:
            metadata.drop_all()

    
if __name__ == '__main__':
    testbase.main()
