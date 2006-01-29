from testbase import PersistTest, AssertMixin
import unittest, sys, os
from sqlalchemy import *
import StringIO
import testbase

from tables import *
import tables

objectstore.LOG = True

class Tester(object):
    def __init__(self, data=None):
        self.data = data
        print repr(self) + " (%d)" % (id(self))
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, repr(self.data))
        
class SelfCycleTest(AssertMixin):
    """tests a self-referential mapper, with an additional list of child objects."""
    def setUpAll(self):
        testbase.db.tables.clear()
        global t1
        global t2
        t1 = Table('t1', testbase.db, 
            Column('c1', Integer, primary_key=True),
            Column('parent_c1', Integer, ForeignKey('t1.c1')),
            Column('data', String(20))
        )
        t2 = Table('t2', testbase.db,
            Column('c1', Integer, primary_key=True),
            Column('c1id', Integer, ForeignKey('t1.c1')),
            Column('data', String(20))
        )
        t1.create()
        t2.create()
    def tearDownAll(self):
        t1.drop()    
        t2.drop()
    def setUp(self):
        objectstore.clear()
        clear_mappers()
    
    def testcycle(self):
        class C1(Tester):
            pass
        class C2(Tester):
            pass
        
        m1 = mapper(C1, t1, properties = {
            'c1s' : relation(C1),
            'c2s' : relation(C2, t2)
        })

        a = C1('head c1')
        a.c1s.append(C1('child1'))
        a.c1s.append(C1('child2'))
        a.c1s[0].c1s.append(C1('subchild1'))
        a.c1s[0].c1s.append(C1('subchild2'))
        a.c1s[1].c2s.append(C2('child2 data1'))
        a.c1s[1].c2s.append(C2('child2 data2'))
        objectstore.commit()
        
class CycleTest(AssertMixin):
    """tests two mappers with a bi-directional dependency"""
    def setUpAll(self):
        testbase.db.tables.clear()
        global t1
        global t2
        t1 = Table('t1', testbase.db, 
            Column('c1', Integer, primary_key=True),
            Column('c2', Integer, ForeignKey('t2.c1'))
        )
        t2 = Table('t2', testbase.db,
            Column('c1', Integer, primary_key=True),
            Column('c2', Integer)
        )
        t2.create()
        t1.create()
        t2.c.c2.append_item(ForeignKey('t1.c1'))
    def tearDownAll(self):
        t2.drop()
        t1.drop()    
    def setUp(self):
        objectstore.clear()
        objectstore.LOG = True
        clear_mappers()
    
    def testcycle(self):
        class C1(object):pass
        class C2(object):pass
        
        m2 = mapper(C2, t2)
        m1 = mapper(C1, t1, properties = {
            'c2s' : relation(m2, primaryjoin=t1.c.c2==t2.c.c1, uselist=True)
        })
        m2.add_property('c1s', relation(m1, primaryjoin=t2.c.c2==t1.c.c1, uselist=True))
        a = C1()
        b = C2()
        c = C1()
        d = C2()
        e = C2()
        f = C2()
        a.c2s.append(b)
        d.c1s.append(c)
        b.c1s.append(c)
        objectstore.commit()

class CycleWDepsTest(AssertMixin):
    """tests two mappers with a bi-directional dependency, and child objects on one of them"""
    def setUpAll(self):
        testbase.db.tables.clear()
        global t1
        global t2
        global t3
        t1 = Table('t1', testbase.db, 
            Column('c1', Integer, primary_key=True),
            Column('t2id', Integer, ForeignKey('t2.c1')),
            Column('data', String(20))
        )
        t2 = Table('t2', testbase.db,
            Column('c1', Integer, primary_key=True),
            Column('t1id', Integer),
            Column('data', String(20))
        )
        t2.create()
        t1.create()
        t2.c.t1id.append_item(ForeignKey('t1.c1'))
        t3 = Table('t1_data', testbase.db, 
            Column('c1', Integer, primary_key=True),
            Column('t1id', Integer, ForeignKey('t1.c1')),
            Column('data', String(20)))
        t3.create()
        
    def setUp(self):
        objectstore.clear()
        objectstore.LOG = True
        clear_mappers()

    def testcycle(self):
        class C1(Tester):pass
        class C2(Tester):pass
        class C1Data(Tester):pass

        m2 = mapper(C2, t2)
        m1 = mapper(C1, t1, properties = {
            'c2s' : relation(m2, primaryjoin=t2.c.t1id==t1.c.c1, uselist=True),
            'c1datas' : relation(C1Data, t3)
        })
        m2.add_property('c1s', relation(m1, primaryjoin=t1.c.t2id==t2.c.c1, uselist=True))
            
        a = C1('c1 a')
        b = C2('c2 b')
        c = C1('c1 c')
        d = C2('c2 d')
        e = C2('c2 e')
        f = C2('c2 f')
        a.c2s.append(b)
        b.c1s.append(c)
        d.c1s.append(a)
        d.c1s.append(c)
        a.c1datas.append(C1Data('c1data1'))
        a.c1datas.append(C1Data('c1data2'))
        c.c1datas.append(C1Data('c1data3'))
        objectstore.commit()

        objectstore.delete(d)
        objectstore.delete(c)
        objectstore.commit()
        
if __name__ == "__main__":
    testbase.main()        

