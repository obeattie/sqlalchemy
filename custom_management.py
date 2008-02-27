from sqlalchemy import *
from sqlalchemy.orm import *

from sqlalchemy.orm.attributes import InstrumentClass, set_attribute, get_attribute, del_attribute, is_instrumented

class MyClassState(InstrumentClass):
    
    def instrument_attribute(self, class_, key, attr):
        pass
        
    def pre_instrument_attribute(self, class_, key, attr):
        pass
        
    def instrument_collection_class(self, class_, key, collection_class):
        return MyListLike
    
    def get_instance_dict(self, instance):
        return instance._goofy_dict
        
    def initialize_instance_dict(self, instance):
        instance.__dict__['_goofy_dict'] = {}

class MyListLike(list):
    # add @appender, @remover decorators as needed
    pass

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
        else:
            self._goofy_dict[key] = value

    def __delattr__(self, key):
        if is_instrumented(self, key):
            del_attribute(self, key)
        else:
            del self._goofy_dict[key]

if __name__ == '__main__':
    meta = MetaData(create_engine('sqlite://'))

    table1 = Table('table1', meta, Column('id', Integer, primary_key=True), Column('name', Text))
    table2 = Table('table2', meta, Column('id', Integer, primary_key=True), Column('name', Text), Column('t1id', Integer, ForeignKey('table1.id')))
    meta.create_all()

    class A(MyClass):
        pass
    
    class B(MyClass):
        pass
    
    mapper(A, table1, properties={
        'bs':relation(B)
    })
    
    mapper(B, table2)
    
    a1 = A(name='a1', bs=[B(name='b1'), B(name='b2')])

    assert a1.name == 'a1'
    assert a1.bs[0].name == 'b1'
    assert isinstance(a1.bs, MyListLike)
    
    sess = create_session()
    sess.save(a1)

    sess.flush()
    sess.clear()

    a1 = sess.query(A).get(a1.id)

    assert a1.name == 'a1'
    assert a1.bs[0].name == 'b1'
    assert isinstance(a1.bs, MyListLike)
