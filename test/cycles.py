from testbase import PersistTest, AssertMixin
import unittest, sys, os
from sqlalchemy import *
import StringIO
import testbase

from tables import *
import tables


class CycleTest(AssertMixin):
    def setUpAll(self):
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
        a.c2s.append(b)
        objectstore.commit()
        
if __name__ == "__main__":
    testbase.main()        

