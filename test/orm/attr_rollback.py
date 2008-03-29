import testenv; testenv.configure_for_tests()
import pickle
import sqlalchemy.orm.attributes as attributes
from sqlalchemy.orm.collections import collection
from sqlalchemy import exceptions, util
from testlib import *
from testlib import fixtures

# these test classes defined at the module
# level to support pickling
class MyTest(object):pass
class MyTest2(object):pass

class AttrTestBase(object):
        
    def test_rback_to_empty(self):

        f = Foo()
        assert attributes.get_history(f._foostate, 'x') == ([], emptyhist, [])
        f._foostate.set_savepoint()
        f.x = data1
        assert attributes.get_history(f._foostate, 'x') == (hist1, [], [])
        f._foostate.rollback()
        assert attributes.get_history(f._foostate, 'x') == ([], emptyhist, [])

        assert f.x == empty
    
    def test_needs_savepoint(self):
        f = Foo()
        f.x = data1
        self.assertRaises(exceptions.InvalidRequestError, f._foostate.rollback)
        
    def test_rback_to_set(self):
        f = Foo()
        f.x = data1
        f._foostate.commit_all()
        f._foostate.set_savepoint()
        f.x = empty
        f._foostate.rollback()
        assert attributes.get_history(f._foostate, 'x') == ([], hist1, [])

    def test_rback_savepoint_to_set(self):
        f = Foo()
        f.x = data1
        f._foostate.set_savepoint()
        f.x = empty
        f._foostate.rollback()
        assert f.x == data1
        assert attributes.get_history(f._foostate, 'x') == (hist1, [], [])
        
    def test_rback_to_committed(self):
        f = Foo()
        f.x = data1
        f._foostate.commit_all()
        f._foostate.set_savepoint()
        assert attributes.get_history(f._foostate, 'x') == ([], hist1, [])

        f._foostate.rollback()
        assert attributes.get_history(f._foostate, 'x') == ([], hist1, [])

    def test_rback_savepoint_rback_to_committed(self):
        f = Foo()
        f.x = data1
        f._foostate.commit_all()
        assert attributes.get_history(f._foostate, 'x') == ([], hist1, [])

        f._foostate.set_savepoint()

        f.x = data2
        assert attributes.get_history(f._foostate, 'x') == (hist2, [], hist1)

        f._foostate.set_savepoint()
        assert attributes.get_history(f._foostate, 'x') == (hist2, [], hist1)

        f.x = data3
        assert attributes.get_history(f._foostate, 'x') == (hist3, [], hist1)
        
        f._foostate.rollback()
        assert attributes.get_history(f._foostate, 'x') == (hist2, [], hist1)

        f._foostate.rollback()
        assert attributes.get_history(f._foostate, 'x') == ([], hist1, [])

    def test_rback_savepoint_commit(self):
        f = Foo()
        f.x = data1
        f._foostate.commit_all()

        aeq = self.assertEquals

        aeq(attributes.get_history(f._foostate, 'x'), ([], hist1, []))

        f.x = data2
        aeq(attributes.get_history(f._foostate, 'x'), (hist2, [], hist1))
        f._foostate.set_savepoint()
        aeq(attributes.get_history(f._foostate, 'x'), (hist2, [], hist1))

        f.x = data3
        aeq(attributes.get_history(f._foostate, 'x'), (hist3, [], hist1))

        f._foostate.rollback()
        aeq(attributes.get_history(f._foostate, 'x'), (hist2, [], hist1))

        f._foostate.commit_all()
        aeq(attributes.get_history(f._foostate, 'x'), ([], hist2, []))
        
    def test_commit_savepoint_commit(self):
        f = Foo()
        f.x = data1
        f._foostate.commit_all()
        assert attributes.get_history(f._foostate, 'x') == ([], hist1, [])

        f.x = data2
        assert attributes.get_history(f._foostate, 'x') == (hist2, [], hist1)
        f._foostate.set_savepoint()
        assert attributes.get_history(f._foostate, 'x') == (hist2, [], hist1)

        f.x = data3
        assert attributes.get_history(f._foostate, 'x') == (hist3, [], hist1)

        f._foostate.remove_savepoint()

        f._foostate.commit_all()
        assert attributes.get_history(f._foostate, 'x') == ([], hist3, [])
    
    def test_rback_savepoint_to_empty(self):
        f = Foo()
        assert attributes.get_history(f._foostate, 'x') == ([], emptyhist, [])
        f._foostate.set_savepoint()
        assert attributes.get_history(f._foostate, 'x') == ([], emptyhist, [])
        f.x = data3
        assert attributes.get_history(f._foostate, 'x') == (hist3, [], [])
        f._foostate.rollback()
        assert attributes.get_history(f._foostate, 'x') == ([], emptyhist, [])
    
    def test_commit_to_savepoint(self):
        
        f = Foo()
        assert attributes.get_history(f._foostate, 'x') == ([], emptyhist, [])
        f._foostate.set_savepoint()
        f.x = data1
        f._foostate.commit_all()
        assert attributes.get_history(f._foostate, 'x') == ([], hist1, [])
        f._foostate.rollback()
        assert attributes.get_history(f._foostate, 'x') == ([], emptyhist, [])

    def test_multiple_commit_to_savepoint_rback(self):

        f = Foo()
        assert attributes.get_history(f._foostate, 'x') == ([], emptyhist, [])

        # load from DB
        f.x = data1
        
        # mark as "committed".  this means, "it matches the DB"
        f._foostate.commit_all()
        
        # data shows up as "unmodified" vs. DB
        assert attributes.get_history(f._foostate, 'x') == ([], hist1, [])

        # begin transaction
        f._foostate.set_savepoint()
        
        # change things
        f.x = data2

        # flush to DB.  changes show up:
        assert attributes.get_history(f._foostate, 'x') == (hist2, [], hist1)
        
        # write them to DB, then mark as "committed" 
        f._foostate.commit_all()
        
        # shows up as "committed"
        assert attributes.get_history(f._foostate, 'x') == ([], hist2, [])
        
        # change things again
        f.x = data3

        # flush to DB.  changes show up:
        assert attributes.get_history(f._foostate, 'x') == (hist3, [], hist2)
        
        # write them to DB, then mark as "committed"
        f._foostate.commit_all()
        
        # shows up as "committed"
        assert attributes.get_history(f._foostate, 'x') == ([], hist3, [])
        
        # rollback transaction
        f._foostate.rollback()
        
        # back to beginning
        assert f.x == data1
        assert attributes.get_history(f._foostate, 'x') == ([], hist1, [])

    def test_multiple_commit_to_savepoint_commit(self):
        f = Foo()
        assert attributes.get_history(f._foostate, 'x') == ([], emptyhist, [])
        f.x = data1
        f._foostate.commit_all()
        assert attributes.get_history(f._foostate, 'x') == ([], hist1, [])
        f._foostate.set_savepoint()
        f.x = data2
        assert attributes.get_history(f._foostate, 'x') == (hist2, [], hist1)
        f._foostate.commit_all()
        assert attributes.get_history(f._foostate, 'x') == ([], hist2, [])
        f.x = data3
        assert attributes.get_history(f._foostate, 'x') == (hist3, [], hist2)
        f._foostate.commit_all()
        assert attributes.get_history(f._foostate, 'x') == ([], hist3, [])
        
        f._foostate.commit_all()
        assert f.x == data3
        assert attributes.get_history(f._foostate, 'x') == ([], hist3, [])

    def test_multiple_commit_to_nested_savepoint_rback(self):

        f = Foo()
        assert attributes.get_history(f._foostate, 'x') == ([], emptyhist, [])
        f.x = data1
        f._foostate.commit_all()
        assert attributes.get_history(f._foostate, 'x') == ([], hist1, [])
        
        f._foostate.set_savepoint()
        
        f.x = data2
        assert attributes.get_history(f._foostate, 'x') == (hist2, [], hist1)
        f._foostate.commit_all()
        assert attributes.get_history(f._foostate, 'x') == ([], hist2, [])

        f._foostate.set_savepoint()
        
        f.x = data3
        assert attributes.get_history(f._foostate, 'x') == (hist3, [], hist2)
        f._foostate.commit_all()
        assert attributes.get_history(f._foostate, 'x') == ([], hist3, [])
        
        f._foostate.rollback()
        
        assert f.x == data2
        assert attributes.get_history(f._foostate, 'x') == ([], hist2, [])
        
        f._foostate.rollback()
        assert f.x == data1
        assert attributes.get_history(f._foostate, 'x') == ([], hist1, [])

    def test_multiple_commit_to_nested_savepoint_commit(self):

        f = Foo()
        assert attributes.get_history(f._foostate, 'x') == ([], emptyhist, [])
        f.x = data1
        f._foostate.commit_all()
        assert attributes.get_history(f._foostate, 'x') == ([], hist1, [])

        f._foostate.set_savepoint()

        f.x = data2
        assert attributes.get_history(f._foostate, 'x') == (hist2, [], hist1)
        f._foostate.commit_all()
        assert attributes.get_history(f._foostate, 'x') == ([], hist2, [])

        f._foostate.set_savepoint()

        f.x = data3
        assert attributes.get_history(f._foostate, 'x') == (hist3, [], hist2)
        f._foostate.commit_all()
        assert attributes.get_history(f._foostate, 'x') == ([], hist3, [])

        f._foostate.rollback()

        assert f.x == data2
        assert attributes.get_history(f._foostate, 'x') == ([], hist2, [])

        f._foostate.commit_all()
        assert f.x == data2
        assert attributes.get_history(f._foostate, 'x') == ([], hist2, [])
    
    def test_savepoint_rback_restores_previous_dirty_state(self):
        f = Foo()
        f._foostate.commit_all()
        f.x = data1
        
        f._foostate.set_savepoint()
        f.x = data2
        f._foostate.rollback()
        assert f.x == data1
        assert attributes.get_history(f._foostate, 'x') == (hist1, [], [])

    def test_savepoint_commit_removes_previous_dirty_state(self):
        f = Foo()
        f._foostate.commit_all()
        f.x = data1
        assert attributes.get_history(f._foostate, 'x') == (hist1, [], [])
        
        f._foostate.set_savepoint()
        f.x = data2
        assert attributes.get_history(f._foostate, 'x') == (hist2, [], [])
        
        f._foostate.remove_savepoint()
        f._foostate.commit_all()

        assert attributes.get_history(f._foostate, 'x') == ([], hist2, [])
        
    
        
class CollectionTestBase(AttrTestBase):

    def test_collection_rback_savepoint_rback_to_empty(self):
        b1, b2, b3, b4, b5 = Bar(), Bar(), Bar(), Bar(), Bar()
        f = Foo()

        f._foostate.set_savepoint()
        
        f.x.append(b2)
        f.x.append(b3)
        assert attributes.get_history(f._foostate, 'x') == ([b2, b3], [], [])
        assert f.x == make_collection([b2, b3])

        f._foostate.set_savepoint()
        assert attributes.get_history(f._foostate, 'x') == ([b2, b3], [], [])

        f.x.remove(b3)
        f.x.append(b1)
        assert attributes.get_history(f._foostate, 'x') == ([b2, b1], [], [])
        assert f.x == make_collection([b2, b1])

        f._foostate.rollback()
        assert attributes.get_history(f._foostate, 'x') == ([b2, b3], [], [])
        assert f.x == make_collection([b2, b3])

        f._foostate.rollback()
        assert attributes.get_history(f._foostate, 'x') == ([], [], [])

    def test_collection_rback_savepoint_commit(self):
        b1, b2, b3, b4, b5 = Bar(), Bar(), Bar(), Bar(), Bar()
        f = Foo()
        f.x.append(b2)
        f.x.append(b3)
        assert attributes.get_history(f._foostate, 'x') == ([b2, b3], [], [])
        assert f.x == make_collection([b2, b3])

        f._foostate.set_savepoint()
        assert attributes.get_history(f._foostate, 'x') == ([b2, b3], [], [])

        f.x.remove(b3)
        f.x.append(b1)
        assert attributes.get_history(f._foostate, 'x') == ([b2, b1], [], [])
        assert f.x == make_collection([b2, b1])

        f._foostate.rollback()
        assert attributes.get_history(f._foostate, 'x') == ([b2, b3], [], [])
        assert f.x == make_collection([b2, b3])

        f._foostate.commit_all()
        assert attributes.get_history(f._foostate, 'x') == ([], [b2, b3], [])

    def test_hasparent(self):
        b1, b2, b3 = Bar(), Bar(), Bar()
        
        assert not attributes.has_parent(Foo, b1, 'x', optimistic=False)
        assert not attributes.has_parent(Foo, b2, 'x', optimistic=False)
        assert not attributes.has_parent(Foo, b3, 'x', optimistic=False)
        f = Foo()
        f.x.append(b1)
        f.x.append(b2)

        assert attributes.has_parent(Foo, b1, 'x', optimistic=False)
        assert attributes.has_parent(Foo, b2, 'x', optimistic=False)
        assert not attributes.has_parent(Foo, b3, 'x', optimistic=False)

        for x in (f, b1, b2, b3):
            x._foostate.set_savepoint()
        f.x.append(b3)
        f.x.remove(b2)
        assert attributes.has_parent(Foo, b1, 'x', optimistic=False)
        assert not attributes.has_parent(Foo, b2, 'x', optimistic=False)
        assert attributes.has_parent(Foo, b3, 'x', optimistic=False)
        
        for x in (f, b1, b2, b3):
            x._foostate.rollback()

        assert attributes.has_parent(Foo, b1, 'x', optimistic=False)
        assert attributes.has_parent(Foo, b2, 'x', optimistic=False)
        assert not attributes.has_parent(Foo, b3, 'x', optimistic=False)

class ScalarTest(AttrTestBase, TestBase):
    def setUpAll(self):
        global Foo, data1, data2, data3, hist1, hist2, hist3, empty, emptyhist
        data1 = 5
        data2 = 9
        data3 = 12
        
        hist1 = [5]
        hist2 = [9]
        hist3 = [12]
        
        empty = None
        emptyhist = []
        
        class Foo(object):pass
        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'x', uselist=False, useobject=False)

class ScalarObjectTest(AttrTestBase, TestBase):
    def setUpAll(self):
        global Foo, data1, data2, data3, hist1, hist2, hist3, empty, emptyhist, Bar

        class Foo(object):pass
        class Bar(object):pass
        attributes.register_class(Foo)
        attributes.register_class(Bar)
        attributes.register_attribute(Foo, 'x', uselist=False, useobject=True, trackparent=True)

        data1 = Bar()
        data2 = Bar()
        data3 = Bar()

        hist1 = [data1]
        hist2 = [data2]
        hist3 = [data3]

        empty = None
        emptyhist = [None]

    def test_hasparent(self):
        f = Foo()
        b = Bar()
        f2 = Foo()
        f2.x = b
        assert attributes.has_parent(Foo, b, 'x', optimistic=False)
        
        for x in (f, f2, b):
            x._foostate.set_savepoint()
        f2.x = None
        f.x = b
        assert attributes.has_parent(Foo, b, 'x', optimistic=False)
        
        for x in (f, f2, b):
            x._foostate.rollback()
            
        assert f2.x == b
        assert attributes.has_parent(Foo, b, 'x', optimistic=False)

class ListTest(CollectionTestBase, TestBase):
    def setUpAll(self):
        global Foo, data1, data2, data3, hist1, hist2, hist3, empty, emptyhist, Bar, make_collection
        class Bar(object):pass

        class Foo(object):pass
        attributes.register_class(Foo)
        attributes.register_class(Bar)
        make_collection = list
        attributes.register_attribute(Foo, 'x', uselist=True, useobject=True, trackparent=True)

        data1 = make_collection([Bar()])
        data2 = make_collection([Bar(), Bar()])
        data3 = make_collection([Bar()])
        hist1 = list(data1)
        hist2 = list(data2)
        hist3 = list(data3)
        empty = make_collection([])
        emptyhist = empty

class SetTest(CollectionTestBase, TestBase):
    def setUpAll(self):
        global Foo, data1, data2, data3, hist1, hist2, hist3, empty, emptyhist, Bar, make_collection
        class Bar(object):pass

        class Foo(object):pass
        attributes.register_class(Foo)
        attributes.register_class(Bar)
        class myset(util.OrderedSet):
            def append(self, item):
                self.add(item)
        make_collection = myset
        attributes.register_attribute(Foo, 'x', uselist=True, useobject=True, typecallable=myset, trackparent=True)

        data1 = make_collection([Bar()])
        data2 = make_collection([Bar(), Bar()])
        data3 = make_collection([Bar()])
        hist1 = list(data1)
        hist2 = list(data2)
        hist3 = list(data3)
        empty = make_collection([])
        emptyhist = []

    
if __name__ == "__main__":
    testenv.main()
    
