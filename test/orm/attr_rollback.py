import testenv; testenv.configure_for_tests()
import pickle
import sqlalchemy.orm.attributes as attributes
from sqlalchemy.orm.collections import collection
from sqlalchemy import exceptions
from testlib import *
from testlib import fixtures

# these test classes defined at the module
# level to support pickling
class MyTest(object):pass
class MyTest2(object):pass

class AttrTestBase(object):
        
    def test_rback_to_empty(self):

        f = Foo()
        assert attributes.get_history(f._foostate, 'x') == ([], [], [])
        f.x = data1
        assert attributes.get_history(f._foostate, 'x') == (hist1, [], [])
        f._foostate.rollback()
        assert attributes.get_history(f._foostate, 'x') == ([], [], [])

        assert f.x == empty
        #assert attributes.get_history(f._foostate, 'x') == ([], [None], []) # this is idiosyncratic of scalar attributes

    def test_rback_to_committed(self):
        f = Foo()
        f.x = data1
        f._foostate.commit_all()
        assert attributes.get_history(f._foostate, 'x') == ([], hist1, [])

        f._foostate.rollback()
        assert attributes.get_history(f._foostate, 'x') == ([], hist1, [])

    def test_rback_savepoint_rback_to_committed(self):
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
        
        f._foostate.rollback()
        assert attributes.get_history(f._foostate, 'x') == (hist2, [], hist1), attributes.get_history(f._foostate, 'x')

        f._foostate.rollback()
        assert attributes.get_history(f._foostate, 'x') == ([], hist1, [])

    def test_rback_savepoint_commit(self):
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

        f._foostate.rollback()
        assert attributes.get_history(f._foostate, 'x') == (hist2, [], hist1)

        f._foostate.commit_all()
        assert attributes.get_history(f._foostate, 'x') == ([], hist2, [])

    def test_commit_savepoint_rback_tocommitted(self):
        f = Foo()
        f.x = data1
        f._foostate.commit_all()
        assert attributes.get_history(f._foostate, 'x') == ([], hist1, [])

        f.x = data2
        assert attributes.get_history(f._foostate, 'x') == (hist2, [], hist1)
        f._foostate.set_savepoint()
        assert attributes.get_history(f._foostate, 'x') == (hist2, [], hist1)
        assert f.x == data2

        f.x = data3
        assert attributes.get_history(f._foostate, 'x') == (hist3, [], hist1)

        f._foostate.remove_savepoint()
        assert not f._foostate.savepoints

        f._foostate.rollback()
        assert attributes.get_history(f._foostate, 'x') == ([], hist1, [])
        assert f.x == data1
        
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
    
    def test_rback_savepoint_rback_to_committed_nodata2(self):
        f = Foo()
        f.x = data1
        f._foostate.commit_all()
        assert attributes.get_history(f._foostate, 'x') == ([], hist1, [])
        f._foostate.set_savepoint()
        assert attributes.get_history(f._foostate, 'x') == ([], hist1, [])
        f.x = data3
        assert attributes.get_history(f._foostate, 'x') == (hist3, [], hist1)
        f._foostate.rollback()
        assert attributes.get_history(f._foostate, 'x') == ([], hist1, [])
        assert f.x == data1

    def test_rback_savepoint_to_empty(self):
        f = Foo()
        assert attributes.get_history(f._foostate, 'x') == ([], [], [])
        f._foostate.set_savepoint()
        assert attributes.get_history(f._foostate, 'x') == ([], [], [])
        f.x = data3
        assert attributes.get_history(f._foostate, 'x') == (hist3, [], [])
        f._foostate.rollback()
        assert attributes.get_history(f._foostate, 'x') == ([], [], [])
    
class CollectionTestBase(AttrTestBase):

    def test_collection_rback_savepoint_rback_to_empty(self):
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

class ScalarTest(AttrTestBase, TestBase):
    def setUpAll(self):
        global Foo, data1, data2, data3, hist1, hist2, hist3, empty
        data1 = 5
        data2 = 9
        data3 = 12
        
        hist1 = [5]
        hist2 = [9]
        hist3 = [12]
        
        empty = None
        
        class Foo(object):pass
        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'x', uselist=False, useobject=False)

class ListTest(CollectionTestBase, TestBase):
    def setUpAll(self):
        global Foo, data1, data2, data3, hist1, hist2, hist3, empty, Bar, make_collection
        class Bar(object):pass

        class Foo(object):pass
        attributes.register_class(Foo)
        make_collection = list
        attributes.register_attribute(Foo, 'x', uselist=True, useobject=True)

        data1 = make_collection([Bar()])
        data2 = make_collection([Bar(), Bar()])
        data3 = make_collection([Bar()])
        hist1 = list(data1)
        hist2 = list(data2)
        hist3 = list(data3)
        empty = make_collection([])

class SetTest(CollectionTestBase, TestBase):
    def setUpAll(self):
        global Foo, data1, data2, data3, hist1, hist2, hist3, empty, Bar, make_collection
        class Bar(object):pass

        class Foo(object):pass
        attributes.register_class(Foo)
        class myset(set):
            def append(self, item):
                self.add(item)
        make_collection = myset
        attributes.register_attribute(Foo, 'x', uselist=True, useobject=True, typecallable=myset)

        data1 = make_collection([Bar()])
        data2 = make_collection([Bar(), Bar()])
        data3 = make_collection([Bar()])
        hist1 = list(data1)
        hist2 = list(data2)
        hist3 = list(data3)
        empty = make_collection([])

    
if __name__ == "__main__":
    testenv.main()
    