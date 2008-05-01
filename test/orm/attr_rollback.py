import testenv; testenv.configure_for_tests()
import pickle
import sqlalchemy.orm.attributes as attributes
from sqlalchemy.orm.collections import collection
from sqlalchemy import exc as sa_exc, util
from testlib import *
from testlib import fixtures

# these test classes defined at the module
# level to support pickling
class MyTest(object):pass
class MyTest2(object):pass

class AttrTestBase(object):
        
    def test_rback_to_empty(self):

        f = Foo()
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], emptyhist, [])
        attributes.instance_state(f).set_savepoint(1)
        f.x = data1
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist1, [], [])
        attributes.instance_state(f).rollback(1)
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], emptyhist, [])

        assert f.x == empty
    
    def test_needs_savepoint(self):
        f = Foo()
        f.x = data1
        self.assertRaises(sa_exc.InvalidRequestError, attributes.instance_state(f).rollback, 1)

        self.assertRaises(sa_exc.InvalidRequestError, attributes.instance_state(f).remove_savepoint, 1)
    
    def test_savepoint_matchup(self):
        f = Foo()
        attributes.instance_state(f).set_savepoint(1)
        attributes.instance_state(f).set_savepoint(2)
        self.assertRaises(sa_exc.AssertionError, attributes.instance_state(f).rollback, 1)
        
        f = Foo()
        attributes.instance_state(f).set_savepoint(1)
        attributes.instance_state(f).set_savepoint(2)
        self.assertRaises(sa_exc.AssertionError, attributes.instance_state(f).remove_savepoint, 1)

        f = Foo()
        attributes.instance_state(f).set_savepoint(1)
        self.assertRaises(sa_exc.AssertionError, attributes.instance_state(f).rollback, 2)

        f = Foo()
        attributes.instance_state(f).set_savepoint(1)
        self.assertRaises(sa_exc.AssertionError, attributes.instance_state(f).remove_savepoint, 2)
        
    def test_rback_to_set(self):
        f = Foo()
        f.x = data1
        attributes.instance_state(f).commit_all()
        attributes.instance_state(f).set_savepoint(1)
        f.x = empty
        attributes.instance_state(f).rollback(1)
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist1, [])

    def test_rback_savepoint_to_set(self):
        f = Foo()
        f.x = data1
        attributes.instance_state(f).set_savepoint(1)
        f.x = empty
        attributes.instance_state(f).rollback(1)
        assert f.x == data1
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist1, [], [])
        
    def test_rback_to_committed(self):
        f = Foo()
        f.x = data1
        attributes.instance_state(f).commit_all()
        attributes.instance_state(f).set_savepoint(1)
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist1, [])

        attributes.instance_state(f).rollback(1)
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist1, [])

    def test_rback_savepoint_rback_to_committed(self):
        f = Foo()
        f.x = data1
        attributes.instance_state(f).commit_all()
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist1, [])

        attributes.instance_state(f).set_savepoint(1)

        f.x = data2
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist2, [], hist1)

        attributes.instance_state(f).set_savepoint(2)
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist2, [], hist1)

        f.x = data3
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist3, [], hist1)
        
        attributes.instance_state(f).rollback(2)
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist2, [], hist1)

        attributes.instance_state(f).rollback(1)
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist1, [])

    def test_rback_savepoint_commit(self):
        f = Foo()
        f.x = data1
        attributes.instance_state(f).commit_all()

        aeq = self.assertEquals

        aeq(attributes.get_history(attributes.instance_state(f), 'x'), ([], hist1, []))

        f.x = data2
        aeq(attributes.get_history(attributes.instance_state(f), 'x'), (hist2, [], hist1))
        attributes.instance_state(f).set_savepoint(1)
        aeq(attributes.get_history(attributes.instance_state(f), 'x'), (hist2, [], hist1))

        f.x = data3
        aeq(attributes.get_history(attributes.instance_state(f), 'x'), (hist3, [], hist1))

        attributes.instance_state(f).rollback(1)
        aeq(attributes.get_history(attributes.instance_state(f), 'x'), (hist2, [], hist1))

        attributes.instance_state(f).commit_all()
        aeq(attributes.get_history(attributes.instance_state(f), 'x'), ([], hist2, []))
        
    def test_commit_savepoint_commit(self):
        f = Foo()
        f.x = data1
        attributes.instance_state(f).commit_all()
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist1, [])

        f.x = data2
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist2, [], hist1)
        attributes.instance_state(f).set_savepoint(1)
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist2, [], hist1)

        f.x = data3
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist3, [], hist1)

        attributes.instance_state(f).remove_savepoint(1)

        attributes.instance_state(f).commit_all()
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist3, [])
    
    def test_rback_savepoint_to_empty(self):
        f = Foo()
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], emptyhist, [])
        attributes.instance_state(f).set_savepoint(1)
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], emptyhist, [])
        f.x = data3
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist3, [], [])
        attributes.instance_state(f).rollback(1)
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], emptyhist, [])
    
    def test_commit_to_savepoint(self):
        
        f = Foo()
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], emptyhist, [])
        attributes.instance_state(f).set_savepoint(1)
        f.x = data1
        attributes.instance_state(f).commit_all()
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist1, [])
        attributes.instance_state(f).rollback(1)
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], emptyhist, [])

    def test_multiple_commit_to_savepoint_rback(self):

        f = Foo()
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], emptyhist, [])

        # load from DB
        f.x = data1
        
        # mark as "committed".  this means, "it matches the DB"
        attributes.instance_state(f).commit_all()
        
        # data shows up as "unmodified" vs. DB
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist1, [])

        # begin transaction
        attributes.instance_state(f).set_savepoint(1)
        
        # change things
        f.x = data2

        # flush to DB.  changes show up:
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist2, [], hist1)
        
        # write them to DB, then mark as "committed" 
        attributes.instance_state(f).commit_all()
        
        # shows up as "committed"
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist2, [])
        
        # change things again
        f.x = data3

        # flush to DB.  changes show up:
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist3, [], hist2)
        
        # write them to DB, then mark as "committed"
        attributes.instance_state(f).commit_all()
        
        # shows up as "committed"
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist3, [])
        
        # rollback transaction
        attributes.instance_state(f).rollback(1)
        
        # back to beginning
        assert f.x == data1
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist1, [])

    def test_multiple_commit_to_savepoint_commit(self):
        f = Foo()
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], emptyhist, [])
        f.x = data1
        attributes.instance_state(f).commit_all()
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist1, [])
        attributes.instance_state(f).set_savepoint(1)
        f.x = data2
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist2, [], hist1)
        attributes.instance_state(f).commit_all()
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist2, [])
        f.x = data3
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist3, [], hist2)
        attributes.instance_state(f).commit_all()
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist3, [])
        
        attributes.instance_state(f).commit_all()
        assert f.x == data3
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist3, [])

    def test_multiple_commit_to_nested_savepoint_rback(self):

        f = Foo()
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], emptyhist, [])
        f.x = data1
        attributes.instance_state(f).commit_all()
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist1, [])
        
        attributes.instance_state(f).set_savepoint(1)
        
        f.x = data2
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist2, [], hist1)
        attributes.instance_state(f).commit_all()
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist2, [])

        attributes.instance_state(f).set_savepoint(2)
        
        f.x = data3
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist3, [], hist2)
        attributes.instance_state(f).commit_all()
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist3, [])
        
        attributes.instance_state(f).rollback(2)
        
        assert f.x == data2
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist2, [])
        
        attributes.instance_state(f).rollback(1)
        assert f.x == data1
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist1, [])

    def test_multiple_commit_to_nested_savepoint_rback_inline(self):

        f = Foo()
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], emptyhist, [])
        f.x = data1
        attributes.instance_state(f).commit_all(savepoint_id="X")
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist1, [])

        f.x = data2
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist2, [], hist1)
        attributes.instance_state(f).commit_all(savepoint_id="Y")
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist2, [])

        f.x = data3
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist3, [], hist2)
        attributes.instance_state(f).commit_all(savepoint_id="Z")
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist3, [])

        attributes.instance_state(f).rollback(id_="Z")
        attributes.instance_state(f).rollback(id_="Y")

        assert f.x == data2
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist2, [])

        attributes.instance_state(f).rollback(id_="X")
        assert f.x == data1
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist1, [])

    def test_multiple_commit_to_nested_savepoint_commit(self):

        f = Foo()
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], emptyhist, [])
        f.x = data1
        attributes.instance_state(f).commit_all()
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist1, [])

        attributes.instance_state(f).set_savepoint(1)

        f.x = data2
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist2, [], hist1)
        attributes.instance_state(f).commit_all()
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist2, [])

        attributes.instance_state(f).set_savepoint(2)

        f.x = data3
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist3, [], hist2)
        attributes.instance_state(f).commit_all()
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist3, [])

        attributes.instance_state(f).rollback(2)

        assert f.x == data2
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist2, [])

        attributes.instance_state(f).commit_all()
        assert f.x == data2
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist2, [])

    def test_multiple_commit_to_nested_savepoint_commit_inline(self):

        f = Foo()
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], emptyhist, [])
        f.x = data1
        attributes.instance_state(f).commit_all(savepoint_id="X")
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist1, [])

        f.x = data2
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist2, [], hist1)
        attributes.instance_state(f).commit_all(savepoint_id="Y")
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist2, [])

        f.x = data3
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist3, [], hist2)
        attributes.instance_state(f).commit_all()
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist3, [])

        attributes.instance_state(f).rollback(id_="Y")

        assert f.x == data2
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist2, [])

        attributes.instance_state(f).commit_all()
        assert f.x == data2
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist2, [])
    
    def test_savepoint_rback_restores_previous_dirty_state(self):
        f = Foo()
        attributes.instance_state(f).commit_all()
        f.x = data1
        
        attributes.instance_state(f).set_savepoint(1)
        f.x = data2
        attributes.instance_state(f).rollback(1)
        assert f.x == data1
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist1, [], [])

    def test_savepoint_commit_removes_previous_dirty_state(self):
        f = Foo()
        attributes.instance_state(f).commit_all()
        f.x = data1
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist1, [], [])
        
        attributes.instance_state(f).set_savepoint(1)
        f.x = data2
        assert attributes.get_history(attributes.instance_state(f), 'x') == (hist2, [], [])
        
        attributes.instance_state(f).remove_savepoint(1)
        attributes.instance_state(f).commit_all()

        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist2, [])
        
    
        
class CollectionTestBase(AttrTestBase):

    def test_collection_rback_savepoint_rback_to_empty(self):
        b1, b2, b3, b4, b5 = Bar(), Bar(), Bar(), Bar(), Bar()
        f = Foo()

        attributes.instance_state(f).set_savepoint(1)
        
        f.x.append(b2)
        f.x.append(b3)
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([b2, b3], [], [])
        assert f.x == make_collection([b2, b3])

        attributes.instance_state(f).set_savepoint(2)
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([b2, b3], [], [])

        f.x.remove(b3)
        f.x.append(b1)
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([b2, b1], [], [])
        assert f.x == make_collection([b2, b1])

        attributes.instance_state(f).rollback(2)
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([b2, b3], [], [])
        assert f.x == make_collection([b2, b3])

        attributes.instance_state(f).rollback(1)
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], [], [])

    def test_collection_rback_savepoint_commit(self):
        b1, b2, b3, b4, b5 = Bar(), Bar(), Bar(), Bar(), Bar()
        f = Foo()
        f.x.append(b2)
        f.x.append(b3)
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([b2, b3], [], [])
        assert f.x == make_collection([b2, b3])

        attributes.instance_state(f).set_savepoint(1)
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([b2, b3], [], [])

        f.x.remove(b3)
        f.x.append(b1)
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([b2, b1], [], [])
        assert f.x == make_collection([b2, b1])

        attributes.instance_state(f).rollback(1)
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([b2, b3], [], [])
        assert f.x == make_collection([b2, b3])

        attributes.instance_state(f).commit_all()
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], [b2, b3], [])

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
            x._foostate.set_savepoint(1)
        f.x.append(b3)
        f.x.remove(b2)
        assert attributes.has_parent(Foo, b1, 'x', optimistic=False)
        assert not attributes.has_parent(Foo, b2, 'x', optimistic=False)
        assert attributes.has_parent(Foo, b3, 'x', optimistic=False)
        
        for x in (f, b1, b2, b3):
            x._foostate.rollback(1)

        assert attributes.has_parent(Foo, b1, 'x', optimistic=False)
        assert attributes.has_parent(Foo, b2, 'x', optimistic=False)
        assert not attributes.has_parent(Foo, b3, 'x', optimistic=False)

    def test_pending(self):
        class CompareByName(object):
            def __init__(self, name):
                self.name = name
            def __eq__(self, other):
                return other.name == self.name
            def __hash__(self):
                return hash(self.name)
                
        class Post(CompareByName):
            pass

        class Blog(CompareByName):
            pass

        called = [0]

        lazy_load = []
        def lazy_posts(instance):
            def load():
                called[0] += 1
                return lazy_load
            return load

        attributes.register_class(Post)
        attributes.register_class(Blog)
        attributes.register_attribute(Post, 'blog', uselist=False, extension=attributes.GenericBackrefExtension('posts'), trackparent=True, useobject=True)
        attributes.register_attribute(Blog, 'posts', uselist=True, extension=attributes.GenericBackrefExtension('blog'), callable_=lazy_posts, trackparent=True, useobject=True, typecallable=make_collection)
        
        (p1, p2, p3) = (Post(name='p1'), Post(name='p2'), Post(name='p3'))
        lazy_load += [p1, p2, p3]
        
        b1 = Blog(name='b1')
        
        for x in [b1, p1, p2, p3]:
            attributes.instance_state(x).commit_all(savepoint_id=1)
        
        p4 = Post(name='p4')
        p5 = Post(name='p5')
        p4.blog = b1
        p5.blog = b1

        assert b1.posts ==  make_collection([Post(name='p1'), Post(name='p2'), Post(name='p3'), Post(name='p4'), Post(name='p5')])
        assert attributes.get_history(attributes.instance_state(b1), 'posts') == ([p4, p5], [p1, p2, p3], [])

        for x in [b1, p1, p2, p3]:
            attributes.instance_state(x).rollback(1)
        assert attributes.get_history(attributes.instance_state(b1), 'posts') == ([], [p1, p2, p3], [])
        assert b1.posts == make_collection([p1, p2, p3])
        
        # TODO: more tests needed

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

class MutableScalarTest(AttrTestBase, TestBase):
    def setUpAll(self):
        global Foo, data1, data2, data3, hist1, hist2, hist3, empty, emptyhist
        data1 = {'data':5}
        data2 = {'data':9}
        data3 = {'data':12}

        hist1 = [{'data':5}]
        hist2 = [{'data':9}]
        hist3 = [{'data':12}]

        empty = None
        emptyhist = []

        class Foo(object):pass
        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'x', uselist=False, useobject=False, mutable_scalars=True, copy_function=dict)

    def test_mutable1(self):
        f = Foo()

        f.x = {'data':5}
        attributes.instance_state(f).commit_all()
        attributes.instance_state(f).set_savepoint(1)
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist1, [])

        f.x['foo'] = 9
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([{'data':5, 'foo':9}], [], [{'data':5}])
        
        attributes.instance_state(f).rollback(1)
        
        assert attributes.get_history(attributes.instance_state(f), 'x') == ([], hist1, [])
        
        
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
            x._foostate.set_savepoint(1)
        f2.x = None
        f.x = b
        assert attributes.has_parent(Foo, b, 'x', optimistic=False)
        
        for x in (f, f2, b):
            x._foostate.rollback(1)
            
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

#class DictTest(CollectionTestBase, TestBase):  # TODO
    
if __name__ == "__main__":
    testenv.main()
    
