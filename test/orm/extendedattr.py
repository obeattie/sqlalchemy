import testenv; testenv.configure_for_tests()
import pickle
import sqlalchemy.orm.attributes as attributes
from sqlalchemy.orm.collections import collection
from sqlalchemy.orm.attributes import InstrumentClass, set_attribute, get_attribute, del_attribute, is_instrumented

from sqlalchemy import exceptions
from testlib import *

class MyClassState(InstrumentClass):
    
    def instrument_attribute(self, class_, key, attr):
        pass
        
    def install_descriptor(self, class_, key, attr):
        pass
        
    def uninstall_descriptor(self, class_, key):
        pass
        
    def instrument_collection_class(self, class_, key, collection_class):
        return MyListLike
    
    def get_instance_dict(self, instance):
        return instance._goofy_dict
        
    def initialize_instance_dict(self, instance):
        instance.__dict__['_goofy_dict'] = {}

class MyListLike(list):
    # add @appender, @remover decorators as needed
    _sa_iterator = list.__iter__
    def _sa_appender(self, item, _sa_initiator=None):
        if _sa_initiator is not False:
            self._sa_adapter.fire_append_event(item, _sa_initiator)
        list.append(self, item)
    append = _sa_appender
    def _sa_remover(self, item, _sa_initiator=None):
        self._sa_adapter.fire_pre_remove_event(_sa_initiator)
        if _sa_initiator is not False:
            self._sa_adapter.fire_remove_event(item, _sa_initiator)
        list.remove(self, item)
    remove = _sa_remover

class MyBaseClass(object):
    __sa_instrument_class__ = InstrumentClass
    
class MyClass(object):
    __sa_instrument_class__ = MyClassState

    def __init__(self, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])
            
    def __getattr__(self, key):
        if is_instrumented(self, key):
            return get_attribute(self, key)
        else:
            try:
                return self._goofy_dict[key]
            except KeyError:
                raise AttributeError(key)

    def __setattr__(self, key, value):
        if is_instrumented(self, key):
            set_attribute(self, key, value)
        # fixme: needs the pluggable attributes.has_state
        elif key == attributes.STATE_ATTR:
            self.__dict__[key] = value
        else:
            self._goofy_dict[key] = value

    def __hasattr__(self, key):
        if is_instrumented(self, key):
            return True
        elif key == attributes.STATE_ATTR:
            return key in self.__dict__
        else:
            return key in self._goofy_dict

    def __delattr__(self, key):
        if is_instrumented(self, key):
            del_attribute(self, key)
        else:
            del self._goofy_dict[key]

class UserDefinedExtensionTest(TestBase):
    def test_basic(self):
        for base in (object, MyBaseClass, MyClass):
            class User(base):
                pass

            attributes.register_class(User)
            attributes.register_attribute(User, 'user_id', uselist = False, useobject=False)
            attributes.register_attribute(User, 'user_name', uselist = False, useobject=False)
            attributes.register_attribute(User, 'email_address', uselist = False, useobject=False)

            u = User()
            u.user_id = 7
            u.user_name = 'john'
            u.email_address = 'lala@123.com'

            self.assert_(u.user_id == 7 and u.user_name == 'john' and u.email_address == 'lala@123.com')
            attributes.state_getter(u).commit_all()
            self.assert_(u.user_id == 7 and u.user_name == 'john' and u.email_address == 'lala@123.com')

            u.user_name = 'heythere'
            u.email_address = 'foo@bar.com'
            self.assert_(u.user_id == 7 and u.user_name == 'heythere' and u.email_address == 'foo@bar.com')

    def test_deferred(self):
        for base in (object, MyBaseClass, MyClass):
            class Foo(base):pass

            data = {'a':'this is a', 'b':12}
            def loader(instance, keys):
                for k in keys:
                    attributes.state_getter(instance).dict[k] = data[k]
                return attributes.ATTR_WAS_SET

            attributes.register_class(Foo, deferred_scalar_loader=loader)
            attributes.register_attribute(Foo, 'a', uselist=False, useobject=False)
            attributes.register_attribute(Foo, 'b', uselist=False, useobject=False)

            f = Foo()
            attributes.state_getter(f).expire_attributes(None)
            self.assertEquals(f.a, "this is a")
            self.assertEquals(f.b, 12)

            f.a = "this is some new a"
            attributes.state_getter(f).expire_attributes(None)
            self.assertEquals(f.a, "this is a")
            self.assertEquals(f.b, 12)

            attributes.state_getter(f).expire_attributes(None)
            f.a = "this is another new a"
            self.assertEquals(f.a, "this is another new a")
            self.assertEquals(f.b, 12)

            attributes.state_getter(f).expire_attributes(None)
            self.assertEquals(f.a, "this is a")
            self.assertEquals(f.b, 12)

            del f.a
            self.assertEquals(f.a, None)
            self.assertEquals(f.b, 12)

            attributes.state_getter(f).commit_all()
            self.assertEquals(f.a, None)
            self.assertEquals(f.b, 12)

    def test_inheritance(self):
        """tests that attributes are polymorphic"""

        for base in (object, MyBaseClass, MyClass):
            class Foo(base):pass
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
            assert y.element == 'this is the bar attr', y.element
            assert x.element2 == 'this is the shared attr'
            assert y.element2 == 'this is the shared attr'

    def test_collection_with_backref(self):
        for base in (object, MyBaseClass, MyClass):
            class Post(base):pass
            class Blog(base):pass

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

    def test_history(self):
        for base in (object, MyBaseClass, MyClass):
            class Foo(base):
                pass
            class Bar(base):
                pass
            
            attributes.register_class(Foo)
            attributes.register_class(Bar)
            attributes.register_attribute(Foo, "name", uselist=False, useobject=False)
            attributes.register_attribute(Foo, "bars", uselist=True, trackparent=True, useobject=True)
            attributes.register_attribute(Bar, "name", uselist=False, useobject=False)
            
            
            f1 = Foo()
            f1.name = 'f1'
            
            self.assertEquals(attributes.get_history(attributes.state_getter(f1), 'name'), (['f1'], [], []))
            
            b1 = Bar()
            b1.name = 'b1'
            f1.bars.append(b1)
            self.assertEquals(attributes.get_history(attributes.state_getter(f1), 'bars'), ([b1], [], []))
            
            attributes.state_getter(f1).commit_all()
            attributes.state_getter(b1).commit_all()
            
            self.assertEquals(attributes.get_history(attributes.state_getter(f1), 'name'), ([], ['f1'], []))
            self.assertEquals(attributes.get_history(attributes.state_getter(f1), 'bars'), ([], [b1], []))
            
            f1.name = 'f1mod'
            b2 = Bar()
            b2.name = 'b2'
            f1.bars.append(b2)
            self.assertEquals(attributes.get_history(attributes.state_getter(f1), 'name'), (['f1mod'], [], ['f1']))
            self.assertEquals(attributes.get_history(attributes.state_getter(f1), 'bars'), ([b2], [b1], []))
            f1.bars.remove(b1)
            self.assertEquals(attributes.get_history(attributes.state_getter(f1), 'bars'), ([b2], [], [b1]))
    
    def test_null_instrumentation(self):
        class Foo(MyBaseClass):
            pass
        attributes.register_class(Foo)
        attributes.register_attribute(Foo, "name", uselist=False, useobject=False)
        attributes.register_attribute(Foo, "bars", uselist=True, trackparent=True, useobject=True)
        
        assert Foo.name == attributes.class_state_getter(Foo).get_inst('name')
        assert Foo.bars == attributes.class_state_getter(Foo).get_inst('bars')
        

if __name__ == '__main__':
    testing.main()
