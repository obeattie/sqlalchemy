from testbase import PersistTest, AssertMixin
import unittest, sys, os
from sqlalchemy import *
import sqlalchemy.attributes as attributes
from testbase import Table, Column
import StringIO
import testbase
import gc
import sqlalchemy.orm.session
import types
db = testbase.db

NUM = 250000

class SaveTest(AssertMixin):
    def setUpAll(self):
        global items, metadata
        metadata = MetaData(db)
        items = Table('items', metadata, 
            Column('item_id', Integer, primary_key=True),
            Column('value', String(100)))
        items.create()
    def tearDownAll(self):
        clear_mappers()
        metadata.drop_all()
        
    def testsave(self):
        class Item(object):pass
            
        m = mapper(Item, items)
        
        for x in range(0,NUM/50):
            sess = create_session()
            query = sess.query(Item)
            for y in range (0,50):
 #               print "x,y", (x,y)
                sess.save(Item())
            sess.flush()
            #self._profile()
            print "ROWS:", x * 50
    def _profile(self):
        print "------------------------"
        d = {}
        for o in gc.get_objects():
            t = type(o)
            if t is types.InstanceType:
                t = o.__class__
            d.setdefault(t, 0)
            d[t] += 1
        rep = [(key, value) for key, value in d.iteritems()]
        def sorter(a, b):
            return cmp(b[1], a[1])
        rep.sort(sorter)
        for x in rep[0:30]:
            print x
if __name__ == "__main__":
    testbase.main()        
