from sqlalchemy import *
from sqlalchemy.orm import *

from sqlalchemy.orm.attributes import ClassState, InstanceState

class MyClassState(ClassState):
    def instrument_attribute(self, key, inst):
        self.attrs[key] = inst
        
    def pre_instrument_attribute(self, key, inst):
        pass
        
    def is_instrumented(self, key):
        return key in self.attrs

    def get_impl(self, key):
        # TODO: needs to work for superclasses too
        return self.attrs[key].impl
    
    def get_inst(self, key):
        # TODO: needs to work for superclasses too
        return self.attrs[key]
        
    def manage(self, instance, state=None):
        if state:
            # used during a weakref "resurrection"
            instance.__dict__['_state'] = state
        else:
            instance.__dict__['_state'] = MyInstanceState(instance)
            
class MyInstanceState(InstanceState):
    def __init__(self, obj):
        InstanceState.__init__(self, obj)
        self.dict = obj.__dict__['_goofy_dict'] = {}

    def custom_get(self, key):
        if self.class_._class_state.is_instrumented(key):
            return self.get_impl(key).get(self)
        else:
            try:
                return self.dict[key]
            except KeyError:
                raise AttributeError(key)
            
    def custom_set(self, key, value):
        if self.class_._class_state.is_instrumented(key):
            self.get_impl(key).set(self, value, None)
        else:
            self.dict[key] = value
        
    def custom_del(self, key):
        if self.class_._class_state.is_instrumented(key):
            self.get_impl(key).delete(self)
        else:
            try:
                del self.dict[key]
            except KeyError:
                raise AttributeError(key)
        
meta = MetaData(create_engine('sqlite://'))

table1 = Table('table1', meta, Column('id', Integer, primary_key=True), Column('name', Text))
meta.create_all()


class MyClass(object):
    def __getattr__(self, key):
        return self._state.custom_get(key)
    
    def __setattr__(self, key, value):
        self._state.custom_set(key, value)
    
    def __delattr__(self, key):
        self._state.custom_del(key)
            
MyClass._class_state = MyClassState(MyClass)

mapper(MyClass, table1)
    
mc = MyClass()
mc.name = 'my instance'
assert mc.name == 'my instance'

sess = create_session()
sess.save(mc)

sess.flush()
sess.clear()

mc = sess.query(MyClass).get(mc.id)
assert mc.name == 'my instance'
