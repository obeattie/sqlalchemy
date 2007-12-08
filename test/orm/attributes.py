import testbase
import pickle
import sqlalchemy.orm.attributes as attributes
from sqlalchemy.orm.collections import collection
from sqlalchemy import exceptions
from testlib import *

ROLLBACK_SUPPORTED=False

# these test classes defined at the module
# level to support pickling
class MyTest(object):pass
class MyTest2(object):pass

class AttributesTest(PersistTest):
    """tests for the attributes.py module, which deals with tracking attribute changes on an object."""
    def test_basic(self):
        class User(object):pass
        
        attributes.register_class(User)
        attributes.register_attribute(User, 'user_id', uselist = False, useobject=False)
        attributes.register_attribute(User, 'user_name', uselist = False, useobject=False)
        attributes.register_attribute(User, 'email_address', uselist = False, useobject=False)
        
        u = User()
        print repr(u.__dict__)
        
        u.user_id = 7
        u.user_name = 'john'
        u.email_address = 'lala@123.com'
        
        print repr(u.__dict__)
        self.assert_(u.user_id == 7 and u.user_name == 'john' and u.email_address == 'lala@123.com')
        u._state.commit_all()
        print repr(u.__dict__)
        self.assert_(u.user_id == 7 and u.user_name == 'john' and u.email_address == 'lala@123.com')

        u.user_name = 'heythere'
        u.email_address = 'foo@bar.com'
        print repr(u.__dict__)
        self.assert_(u.user_id == 7 and u.user_name == 'heythere' and u.email_address == 'foo@bar.com')

        if ROLLBACK_SUPPORTED:
            attributes.rollback(u)
            print repr(u.__dict__)
            self.assert_(u.user_id == 7 and u.user_name == 'john' and u.email_address == 'lala@123.com')

    def test_pickleness(self):

        
        attributes.register_class(MyTest)
        attributes.register_class(MyTest2)
        attributes.register_attribute(MyTest, 'user_id', uselist = False, useobject=False)
        attributes.register_attribute(MyTest, 'user_name', uselist = False, useobject=False)
        attributes.register_attribute(MyTest, 'email_address', uselist = False, useobject=False)
        attributes.register_attribute(MyTest2, 'a', uselist = False, useobject=False)
        attributes.register_attribute(MyTest2, 'b', uselist = False, useobject=False)
        # shouldnt be pickling callables at the class level
        def somecallable(*args):
            return None
        attr_name = 'mt2'
        attributes.register_attribute(MyTest, attr_name, uselist = True, trackparent=True, callable_=somecallable, useobject=True)

        o = MyTest()
        o.mt2.append(MyTest2())
        o.user_id=7
        o.mt2[0].a = 'abcde'
        pk_o = pickle.dumps(o)

        o2 = pickle.loads(pk_o)

        # so... pickle is creating a new 'mt2' string after a roundtrip here,
        # so we'll brute-force set it to be id-equal to the original string 
        o_mt2_str = [ k for k in o.__dict__ if k == 'mt2'][0]
        o2_mt2_str = [ k for k in o2.__dict__ if k == 'mt2'][0]
        self.assert_(o_mt2_str == o2_mt2_str)
        self.assert_(o_mt2_str is not o2_mt2_str)
        # change the id of o2.__dict__['mt2']
        former = o2.__dict__['mt2']
        del o2.__dict__['mt2']
        o2.__dict__[o_mt2_str] = former

        pk_o2 = pickle.dumps(o2)

        self.assert_(pk_o == pk_o2)

        # the above is kind of distrurbing, so let's do it again a little
        # differently.  the string-id in serialization thing is just an
        # artifact of pickling that comes up in the first round-trip.
        # a -> b differs in pickle memoization of 'mt2', but b -> c will
        # serialize identically.

        o3 = pickle.loads(pk_o2)
        pk_o3 = pickle.dumps(o3)
        o4 = pickle.loads(pk_o3)
        pk_o4 = pickle.dumps(o4)

        self.assert_(pk_o3 == pk_o4)

        # and lastly make sure we still have our data after all that.
        # identical serialzation is great, *if* it's complete :)
        self.assert_(o4.user_id == 7)
        self.assert_(o4.user_name is None)
        self.assert_(o4.email_address is None)
        self.assert_(len(o4.mt2) == 1)
        self.assert_(o4.mt2[0].a == 'abcde')
        self.assert_(o4.mt2[0].b is None)

    def test_list(self):
        class User(object):pass
        class Address(object):pass
        
        attributes.register_class(User)
        attributes.register_class(Address)
        attributes.register_attribute(User, 'user_id', uselist = False, useobject=False)
        attributes.register_attribute(User, 'user_name', uselist = False, useobject=False)
        attributes.register_attribute(User, 'addresses', uselist = True, useobject=True)
        attributes.register_attribute(Address, 'address_id', uselist = False, useobject=False)
        attributes.register_attribute(Address, 'email_address', uselist = False, useobject=False)
        
        u = User()
        print repr(u.__dict__)

        u.user_id = 7
        u.user_name = 'john'
        u.addresses = []
        a = Address()
        a.address_id = 10
        a.email_address = 'lala@123.com'
        u.addresses.append(a)

        print repr(u.__dict__)
        self.assert_(u.user_id == 7 and u.user_name == 'john' and u.addresses[0].email_address == 'lala@123.com')
        u, a._state.commit_all()
        print repr(u.__dict__)
        self.assert_(u.user_id == 7 and u.user_name == 'john' and u.addresses[0].email_address == 'lala@123.com')

        u.user_name = 'heythere'
        a = Address()
        a.address_id = 11
        a.email_address = 'foo@bar.com'
        u.addresses.append(a)
        print repr(u.__dict__)
        self.assert_(u.user_id == 7 and u.user_name == 'heythere' and u.addresses[0].email_address == 'lala@123.com' and u.addresses[1].email_address == 'foo@bar.com')

        if ROLLBACK_SUPPORTED:
            attributes.rollback(u, a)
            print repr(u.__dict__)
            print repr(u.addresses[0].__dict__)
            self.assert_(u.user_id == 7 and u.user_name == 'john' and u.addresses[0].email_address == 'lala@123.com')
            self.assert_(len(attributes.get_history(u, 'addresses').unchanged_items()) == 1)

        
    def test_lazytrackparent(self):
        """test that the "hasparent" flag works properly when lazy loaders and backrefs are used"""

        class Post(object):pass
        class Blog(object):pass
        attributes.register_class(Post)
        attributes.register_class(Blog)
        
        # set up instrumented attributes with backrefs    
        attributes.register_attribute(Post, 'blog', uselist=False, extension=attributes.GenericBackrefExtension('posts'), trackparent=True, useobject=True)
        attributes.register_attribute(Blog, 'posts', uselist=True, extension=attributes.GenericBackrefExtension('blog'), trackparent=True, useobject=True)

        # create objects as if they'd been freshly loaded from the database (without history)
        b = Blog()
        p1 = Post()
        b._state.set_callable('posts', lambda:[p1])
        p1._state.set_callable('blog', lambda:b)
        p1, b._state.commit_all()

        # no orphans (called before the lazy loaders fire off)
        assert attributes.has_parent(Blog, p1, 'posts', optimistic=True)
        assert attributes.has_parent(Post, b, 'blog', optimistic=True)

        # assert connections
        assert p1.blog is b
        assert p1 in b.posts
        
        # manual connections
        b2 = Blog()
        p2 = Post()
        b2.posts.append(p2)
        assert attributes.has_parent(Blog, p2, 'posts')
        assert attributes.has_parent(Post, b2, 'blog')
        
    def test_inheritance(self):
        """tests that attributes are polymorphic"""
        class Foo(object):pass
        class Bar(Foo):pass
        
        
        attributes.register_class(Foo)
        attributes.register_class(Bar)
        
        def func1():
            print "func1"
            return "this is the foo attr"
        def func2():
            print "func2"
            return "this is the bar attr"
        def func3():
            print "func3"
            return "this is the shared attr"
        attributes.register_attribute(Foo, 'element', uselist=False, callable_=lambda o:func1, useobject=True)
        attributes.register_attribute(Foo, 'element2', uselist=False, callable_=lambda o:func3, useobject=True)
        attributes.register_attribute(Bar, 'element', uselist=False, callable_=lambda o:func2, useobject=True)
        
        x = Foo()
        y = Bar()
        assert x.element == 'this is the foo attr'
        assert y.element == 'this is the bar attr'
        assert x.element2 == 'this is the shared attr'
        assert y.element2 == 'this is the shared attr'

    def test_inheritance2(self):
        """test that the attribute manager can properly traverse the managed attributes of an object,
        if the object is of a descendant class with managed attributes in the parent class"""
        class Foo(object):pass
        class Bar(Foo):pass
        
        attributes.register_class(Foo)
        attributes.register_class(Bar)
        attributes.register_attribute(Foo, 'element', uselist=False, useobject=True)
        x = Bar()
        x.element = 'this is the element'
        (added, unchanged, deleted) = attributes.get_history(x, 'element')
        assert added == ['this is the element']
        x._state.commit_all()
        (added, unchanged, deleted) = attributes.get_history(x, 'element')
        assert added == []
        assert unchanged == ['this is the element']

    def test_lazyhistory(self):
        """tests that history functions work with lazy-loading attributes"""

        class Foo(object):pass
        class Bar(object):
            def __init__(self, id):
                self.id = id
            def __repr__(self):
                return "Bar: id %d" % self.id
                
        
        attributes.register_class(Foo)
        attributes.register_class(Bar)

        def func1():
            return "this is func 1"
        def func2():
            return [Bar(1), Bar(2), Bar(3)]

        attributes.register_attribute(Foo, 'col1', uselist=False, callable_=lambda o:func1, useobject=True)
        attributes.register_attribute(Foo, 'col2', uselist=True, callable_=lambda o:func2, useobject=True)
        attributes.register_attribute(Bar, 'id', uselist=False, useobject=True)

        x = Foo()
        x._state.commit_all()
        x.col2.append(Bar(4))
        (added, unchanged, deleted) = attributes.get_history(x, 'col2')

        
    def test_parenttrack(self):    
        class Foo(object):pass
        class Bar(object):pass
        
        attributes.register_class(Foo)
        attributes.register_class(Bar)
        
        attributes.register_attribute(Foo, 'element', uselist=False, trackparent=True, useobject=True)
        attributes.register_attribute(Bar, 'element', uselist=False, trackparent=True, useobject=True)
        
        f1 = Foo()
        f2 = Foo()
        b1 = Bar()
        b2 = Bar()
        
        f1.element = b1
        b2.element = f2
        
        assert attributes.has_parent(Foo, b1, 'element')
        assert not attributes.has_parent(Foo, b2, 'element')
        assert not attributes.has_parent(Foo, f2, 'element')
        assert attributes.has_parent(Bar, f2, 'element')
        
        b2.element = None
        assert not attributes.has_parent(Bar, f2, 'element')

    def test_mutablescalars(self):
        """test detection of changes on mutable scalar items"""
        class Foo(object):pass
        
        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'element', uselist=False, copy_function=lambda x:[y for y in x], mutable_scalars=True, useobject=False)
        x = Foo()
        x.element = ['one', 'two', 'three']    
        x._state.commit_all()
        x.element[1] = 'five'
        assert attributes.is_modified(x)
        
        attributes.unregister_class(Foo)
        
        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'element', uselist=False, useobject=False)
        x = Foo()
        x.element = ['one', 'two', 'three']    
        x._state.commit_all()
        x.element[1] = 'five'
        assert not attributes.is_modified(x)
        
    def test_descriptorattributes(self):
        """changeset: 1633 broke ability to use ORM to map classes with unusual
        descriptor attributes (for example, classes that inherit from ones
        implementing zope.interface.Interface).
        This is a simple regression test to prevent that defect.
        """
        class des(object):
            def __get__(self, instance, owner): raise AttributeError('fake attribute')

        class Foo(object):
            A = des()

        
        attributes.unregister_class(Foo)
    
    def test_collectionclasses(self):
        
        class Foo(object):pass
        attributes.register_class(Foo)
        attributes.register_attribute(Foo, "collection", uselist=True, typecallable=set, useobject=True)
        assert isinstance(Foo().collection, set)
        
        attributes.unregister_attribute(Foo, "collection")

        try:
            attributes.register_attribute(Foo, "collection", uselist=True, typecallable=dict, useobject=True)
            assert False
        except exceptions.ArgumentError, e:
            assert str(e) == "Type InstrumentedDict must elect an appender method to be a collection class"
        
        class MyDict(dict):
            @collection.appender
            def append(self, item):
                self[item.foo] = item
            @collection.remover
            def remove(self, item):
                del self[item.foo]
        attributes.register_attribute(Foo, "collection", uselist=True, typecallable=MyDict, useobject=True)
        assert isinstance(Foo().collection, MyDict)

        attributes.unregister_attribute(Foo, "collection")
        
        class MyColl(object):pass
        try:
            attributes.register_attribute(Foo, "collection", uselist=True, typecallable=MyColl, useobject=True)
            assert False
        except exceptions.ArgumentError, e:
            assert str(e) == "Type MyColl must elect an appender method to be a collection class"
        
        class MyColl(object):
            @collection.iterator
            def __iter__(self):
                return iter([])
            @collection.appender
            def append(self, item):
                pass
            @collection.remover
            def remove(self, item):
                pass
        attributes.register_attribute(Foo, "collection", uselist=True, typecallable=MyColl, useobject=True)
        try:
            Foo().collection
            assert True
        except exceptions.ArgumentError, e:
            assert False


class BackrefTest(PersistTest):
        
    def test_manytomany(self):
        class Student(object):pass
        class Course(object):pass

        attributes.register_class(Student)
        attributes.register_class(Course)
        attributes.register_attribute(Student, 'courses', uselist=True, extension=attributes.GenericBackrefExtension('students'), useobject=True)
        attributes.register_attribute(Course, 'students', uselist=True, extension=attributes.GenericBackrefExtension('courses'), useobject=True)

        s = Student()
        c = Course()
        s.courses.append(c)
        self.assert_(c.students == [s])
        s.courses.remove(c)
        self.assert_(c.students == [])

        (s1, s2, s3) = (Student(), Student(), Student())

        c.students = [s1, s2, s3]
        self.assert_(s2.courses == [c])
        self.assert_(s1.courses == [c])
        print "--------------------------------"
        print s1
        print s1.courses
        print c
        print c.students
        s1.courses.remove(c)
        self.assert_(c.students == [s2,s3])        
    
    def test_onetomany(self):
        class Post(object):pass
        class Blog(object):pass
        
        attributes.register_class(Post)
        attributes.register_class(Blog)
        attributes.register_attribute(Post, 'blog', uselist=False, extension=attributes.GenericBackrefExtension('posts'), trackparent=True, useobject=True)
        attributes.register_attribute(Blog, 'posts', uselist=True, extension=attributes.GenericBackrefExtension('blog'), trackparent=True, useobject=True)
        b = Blog()
        (p1, p2, p3) = (Post(), Post(), Post())
        b.posts.append(p1)
        b.posts.append(p2)
        b.posts.append(p3)
        self.assert_(b.posts == [p1, p2, p3])
        self.assert_(p2.blog is b)

        p3.blog = None
        self.assert_(b.posts == [p1, p2])
        p4 = Post()
        p4.blog = b
        self.assert_(b.posts == [p1, p2, p4])

        p4.blog = b
        p4.blog = b
        self.assert_(b.posts == [p1, p2, p4])

        # assert no failure removing None
        p5 = Post()
        p5.blog = None
        del p5.blog

    def test_onetoone(self):
        class Port(object):pass
        class Jack(object):pass
        attributes.register_class(Port)
        attributes.register_class(Jack)
        attributes.register_attribute(Port, 'jack', uselist=False, extension=attributes.GenericBackrefExtension('port'), useobject=True)
        attributes.register_attribute(Jack, 'port', uselist=False, extension=attributes.GenericBackrefExtension('jack'), useobject=True)
        p = Port()
        j = Jack()
        p.jack = j
        self.assert_(j.port is p)
        self.assert_(p.jack is not None)

        j.port = None
        self.assert_(p.jack is None)

class DeferredBackrefTest(PersistTest):
    def setUp(self):
        global Post, Blog, called, lazy_load
        
        class Post(object):
            def __init__(self, name):
                self.name = name
            def __eq__(self, other):
                return other.name == self.name

        class Blog(object):
            def __init__(self, name):
                self.name = name
            def __eq__(self, other):
                return other.name == self.name

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
        attributes.register_attribute(Blog, 'posts', uselist=True, extension=attributes.GenericBackrefExtension('blog'), callable_=lazy_posts, trackparent=True, useobject=True)

    def test_lazy_add(self):
        global lazy_load

        p1, p2, p3 = Post("post 1"), Post("post 2"), Post("post 3")
        lazy_load = [p1, p2, p3]

        b = Blog("blog 1")
        p = Post("post 4")
        p.blog = b
        p = Post("post 5")
        p.blog = b
        # setting blog doesnt call 'posts' callable
        assert called[0] == 0

        # calling backref calls the callable, populates extra posts
        assert b.posts == [p1, p2, p3, Post("post 4"), Post("post 5")]
        assert called[0] == 1

    def test_lazy_remove(self):
        global lazy_load
        called[0] = 0
        lazy_load = []

        b = Blog("blog 1")
        p = Post("post 1")
        p.blog = b
        assert called[0] == 0

        lazy_load = [p]

        p.blog = None
        p2 = Post("post 2")
        p2.blog = b
        assert called[0] == 0
        assert b.posts == [p2]
        assert called[0] == 1

    def test_normal_load(self):
        global lazy_load
        lazy_load = (p1, p2, p3) = [Post("post 1"), Post("post 2"), Post("post 3")]
        called[0] = 0

        b = Blog("blog 1")

        # assign without using backref system
        p2.__dict__['blog'] = b

        assert b.posts == [Post("post 1"), Post("post 2"), Post("post 3")]
        assert called[0] == 1
        p2.blog = None
        p4 = Post("post 4")
        p4.blog = b
        assert b.posts == [Post("post 1"), Post("post 3"), Post("post 4")]
        assert called[0] == 1

        called[0] = 0
        lazy_load = (p1, p2, p3) = [Post("post 1"), Post("post 2"), Post("post 3")]
        
if __name__ == "__main__":
    testbase.main()
