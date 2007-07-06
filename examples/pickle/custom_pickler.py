"""illustrates one way to use a custom pickler that is session-aware."""

from sqlalchemy import *
from sqlalchemy.orm.session import object_session
from cStringIO import StringIO
from pickle import Pickler, Unpickler
import threading

meta = MetaData('sqlite://', echo=True)

class MyExt(MapperExtension):
    def populate_instance(self, mapper, selectcontext, row, instance, identitykey, isnew):
        MyPickler.sessions.current = selectcontext.session
        return EXT_PASS
    def before_insert(self, mapper, connection, instance):
        MyPickler.sessions.current = object_session(instance)
        return EXT_PASS
    def before_update(self, mapper, connection, instance):
        MyPickler.sessions.current = object_session(instance)
        return EXT_PASS
    
class MyPickler(object):
    sessions = threading.local()

    def persistent_id(self, obj):
        if getattr(obj, "id", None) is None:
            sess = MyPickler.sessions.current
            newsess = create_session(bind_to=sess.connection(class_mapper(Bar)))
            newsess.save(obj)
            newsess.flush()
        key = "%s:%s" % (type(obj).__name__, obj.id)
        return key

    def persistent_load(self, key):
        name, ident = key.split(":")
        sess = MyPickler.sessions.current
        return sess.query(Bar).get(ident)

    def dumps(self, graph, protocol):
        src = StringIO()
        pickler = Pickler(src)
        pickler.persistent_id = self.persistent_id
        pickler.dump(graph)
        return src.getvalue()

    def loads(self, data):
        dst = StringIO(data)
        unpickler = Unpickler(dst)
        unpickler.persistent_load = self.persistent_load
        return unpickler.load()

foo_table = Table('foo', meta, 
    Column('id', Integer, primary_key=True),
    Column('bar', PickleType(pickler=MyPickler()), nullable=False))

bar_table = Table('bar', meta,
    Column('id', Integer, primary_key=True),
    Column('data', String(40)))

meta.create_all()

class Foo(object):
    pass

class Bar(object):
    def __init__(self, value):
        self.data = value
    
mapper(Foo, foo_table, extension=MyExt())
mapper(Bar, bar_table)

sess = create_session()
f = Foo()
f.bar = Bar('some bar')
sess.save(f)
sess.flush()
sess.clear()

del MyPickler.sessions.current

f = sess.query(Foo).get(f.id)
assert f.bar.data == 'some bar'
