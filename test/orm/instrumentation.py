import testenv; testenv.configure_for_tests()

from testlib import sa
from testlib.sa import MetaData, Table, Column, Integer, ForeignKey
from testlib.sa.orm import mapper, relation, create_session, attributes
from testlib.testing import eq_, ne_
from testlib.compat import _function_named
from orm import _base


def modifies_instrumentation_finders(fn):
    def decorated(*args, **kw):
        pristine = attributes.instrumentation_finders[:]
        try:
            fn(*args, **kw)
        finally:
            del attributes.instrumentation_finders[:]
            attributes.instrumentation_finders.extend(pristine)
    return _function_named(decorated, fn.func_name)

def with_lookup_strategy(strategy):
    def decorate(fn):
        def wrapped(*args, **kw):
            current = attributes._lookup_strategy
            try:
                attributes._install_lookup_strategy(strategy)
                return fn(*args, **kw)
            finally:
                attributes._install_lookup_strategy(current)
        return _function_named(wrapped, fn.func_name)
    return decorate


class InitTest(_base.ORMTest):
    def fixture(self):
        return Table('t', MetaData(),
                     Column('id', Integer, primary_key=True),
                     Column('type', Integer),
                     Column('x', Integer),
                     Column('y', Integer))

    def register(self, cls, canary):
        original_init = cls.__init__
        attributes.register_class(cls)
        ne_(cls.__init__, original_init)
        manager = attributes.manager_of_class(cls)
        def on_init(state, instance, args, kwargs):
            canary.append((cls, 'on_init', type(instance)))
        manager.events.add_listener('on_init', on_init)

    def test_ai(self):
        inits = []

        class A(object):
            def __init__(self):
                inits.append((A, '__init__'))

        obj = A()
        eq_(inits, [(A, '__init__')])

    def test_A(self):
        inits = []

        class A(object): pass
        self.register(A, inits)

        obj = A()
        eq_(inits, [(A, 'on_init', A)])

    def test_Ai(self):
        inits = []

        class A(object):
            def __init__(self):
                inits.append((A, '__init__'))
        self.register(A, inits)

        obj = A()
        eq_(inits, [(A, 'on_init', A), (A, '__init__')])

    def test_ai_B(self):
        inits = []

        class A(object):
            def __init__(self):
                inits.append((A, '__init__'))

        class B(A): pass
        self.register(B, inits)

        obj = A()
        eq_(inits, [(A, '__init__')])

        del inits[:]

        obj = B()
        eq_(inits, [(B, 'on_init', B), (A, '__init__')])

    def test_ai_Bi(self):
        inits = []

        class A(object):
            def __init__(self):
                inits.append((A, '__init__'))

        class B(A):
            def __init__(self):
                inits.append((B, '__init__'))
                super(B, self).__init__()
        self.register(B, inits)

        obj = A()
        eq_(inits, [(A, '__init__')])

        del inits[:]

        obj = B()
        eq_(inits, [(B, 'on_init', B), (B, '__init__'), (A, '__init__')])

    def test_Ai_bi(self):
        inits = []

        class A(object):
            def __init__(self):
                inits.append((A, '__init__'))
        self.register(A, inits)

        class B(A):
            def __init__(self):
                inits.append((B, '__init__'))
                super(B, self).__init__()

        obj = A()
        eq_(inits, [(A, 'on_init', A), (A, '__init__')])

        del inits[:]

        obj = B()
        eq_(inits, [(B, '__init__'), (A, 'on_init', B), (A, '__init__')])

    def test_Ai_Bi(self):
        inits = []

        class A(object):
            def __init__(self):
                inits.append((A, '__init__'))
        self.register(A, inits)

        class B(A):
            def __init__(self):
                inits.append((B, '__init__'))
                super(B, self).__init__()
        self.register(B, inits)

        obj = A()
        eq_(inits, [(A, 'on_init', A), (A, '__init__')])

        del inits[:]

        obj = B()
        eq_(inits, [(B, 'on_init', B), (B, '__init__'), (A, '__init__')])

    def test_Ai_B(self):
        inits = []

        class A(object):
            def __init__(self):
                inits.append((A, '__init__'))
        self.register(A, inits)

        class B(A): pass
        self.register(B, inits)

        obj = A()
        eq_(inits, [(A, 'on_init', A), (A, '__init__')])

        del inits[:]

        obj = B()
        eq_(inits, [(B, 'on_init', B), (A, '__init__')])

    def test_Ai_Bi_Ci(self):
        inits = []

        class A(object):
            def __init__(self):
                inits.append((A, '__init__'))
        self.register(A, inits)

        class B(A):
            def __init__(self):
                inits.append((B, '__init__'))
                super(B, self).__init__()
        self.register(B, inits)

        class C(B):
            def __init__(self):
                inits.append((C, '__init__'))
                super(C, self).__init__()
        self.register(C, inits)

        obj = A()
        eq_(inits, [(A, 'on_init', A), (A, '__init__')])

        del inits[:]

        obj = B()
        eq_(inits, [(B, 'on_init', B), (B, '__init__'), (A, '__init__')])

        del inits[:]
        obj = C()
        eq_(inits, [(C, 'on_init', C), (C, '__init__'), (B, '__init__'),
                   (A, '__init__')])

    def test_Ai_bi_Ci(self):
        inits = []

        class A(object):
            def __init__(self):
                inits.append((A, '__init__'))
        self.register(A, inits)

        class B(A):
            def __init__(self):
                inits.append((B, '__init__'))
                super(B, self).__init__()

        class C(B):
            def __init__(self):
                inits.append((C, '__init__'))
                super(C, self).__init__()
        self.register(C, inits)

        obj = A()
        eq_(inits, [(A, 'on_init', A), (A, '__init__')])

        del inits[:]

        obj = B()
        eq_(inits, [(B, '__init__'), (A, 'on_init', B), (A, '__init__')])

        del inits[:]
        obj = C()
        eq_(inits, [(C, 'on_init', C), (C, '__init__'),  (B, '__init__'),
                   (A, '__init__')])

    def test_Ai_b_Ci(self):
        inits = []

        class A(object):
            def __init__(self):
                inits.append((A, '__init__'))
        self.register(A, inits)

        class B(A): pass

        class C(B):
            def __init__(self):
                inits.append((C, '__init__'))
                super(C, self).__init__()
        self.register(C, inits)

        obj = A()
        eq_(inits, [(A, 'on_init', A), (A, '__init__')])

        del inits[:]

        obj = B()
        eq_(inits, [(A, 'on_init', B), (A, '__init__')])

        del inits[:]
        obj = C()
        eq_(inits, [(C, 'on_init', C), (C, '__init__'), (A, '__init__')])

    def test_Ai_B_Ci(self):
        inits = []

        class A(object):
            def __init__(self):
                inits.append((A, '__init__'))
        self.register(A, inits)

        class B(A): pass
        self.register(B, inits)

        class C(B):
            def __init__(self):
                inits.append((C, '__init__'))
                super(C, self).__init__()
        self.register(C, inits)

        obj = A()
        eq_(inits, [(A, 'on_init', A), (A, '__init__')])

        del inits[:]

        obj = B()
        eq_(inits, [(B, 'on_init', B), (A, '__init__')])

        del inits[:]
        obj = C()
        eq_(inits, [(C, 'on_init', C), (C, '__init__'), (A, '__init__')])

    def test_Ai_B_C(self):
        inits = []

        class A(object):
            def __init__(self):
                inits.append((A, '__init__'))
        self.register(A, inits)

        class B(A): pass
        self.register(B, inits)

        class C(B): pass
        self.register(C, inits)

        obj = A()
        eq_(inits, [(A, 'on_init', A), (A, '__init__')])

        del inits[:]

        obj = B()
        eq_(inits, [(B, 'on_init', B), (A, '__init__')])

        del inits[:]
        obj = C()
        eq_(inits, [(C, 'on_init', C), (A, '__init__')])

    def test_A_Bi_C(self):
        inits = []

        class A(object): pass
        self.register(A, inits)

        class B(A):
            def __init__(self):
                inits.append((B, '__init__'))
        self.register(B, inits)

        class C(B): pass
        self.register(C, inits)

        obj = A()
        eq_(inits, [(A, 'on_init', A)])

        del inits[:]

        obj = B()
        eq_(inits, [(B, 'on_init', B), (B, '__init__')])

        del inits[:]
        obj = C()
        eq_(inits, [(C, 'on_init', C), (B, '__init__')])

    def test_A_B_Ci(self):
        inits = []

        class A(object): pass
        self.register(A, inits)

        class B(A): pass
        self.register(B, inits)

        class C(B):
            def __init__(self):
                inits.append((C, '__init__'))
        self.register(C, inits)

        obj = A()
        eq_(inits, [(A, 'on_init', A)])

        del inits[:]

        obj = B()
        eq_(inits, [(B, 'on_init', B)])

        del inits[:]
        obj = C()
        eq_(inits, [(C, 'on_init', C), (C, '__init__')])

    def test_A_B_C(self):
        inits = []

        class A(object): pass
        self.register(A, inits)

        class B(A): pass
        self.register(B, inits)

        class C(B): pass
        self.register(C, inits)

        obj = A()
        eq_(inits, [(A, 'on_init', A)])

        del inits[:]

        obj = B()
        eq_(inits, [(B, 'on_init', B)])

        del inits[:]
        obj = C()
        eq_(inits, [(C, 'on_init', C)])


class MapperInitTest(_base.ORMTest):

    def fixture(self):
        return Table('t', MetaData(),
                     Column('id', Integer, primary_key=True),
                     Column('type', Integer),
                     Column('x', Integer),
                     Column('y', Integer))

    def test_partially_mapped_inheritance(self):
        class A(object):
            pass

        class B(A):
            pass

        class C(B):
            def __init__(self):
                pass

        mapper(A, self.fixture())

        a = attributes.instance_state(A())
        assert isinstance(a, attributes.InstanceState)
        assert type(a) is not attributes.InstanceState

        b = attributes.instance_state(B())
        assert isinstance(b, attributes.InstanceState)
        assert type(b) is not attributes.InstanceState

        # C is unmanaged
        cobj = C()
        self.assertRaises((AttributeError, TypeError),
                          attributes.instance_state, cobj)

class InstrumentationCollisionTest(_base.ORMTest):
    def test_none(self):
        class A(object): pass
        attributes.register_class(A)

        mgr_factory = lambda cls: attributes.ClassManager(cls)
        class B(object):
            __sa_instrumentation_manager__ = staticmethod(mgr_factory)
        attributes.register_class(B)

        class C(object):
            __sa_instrumentation_manager__ = attributes.ClassManager
        attributes.register_class(C)

    def test_single_down(self):
        class A(object): pass
        attributes.register_class(A)

        mgr_factory = lambda cls: attributes.ClassManager(cls)
        class B(A):
            __sa_instrumentation_manager__ = staticmethod(mgr_factory)

        self.assertRaises(TypeError, attributes.register_class, B)

    def test_single_up(self):

        class A(object): pass
        # delay registration

        mgr_factory = lambda cls: attributes.ClassManager(cls)
        class B(A):
            __sa_instrumentation_manager__ = staticmethod(mgr_factory)
        attributes.register_class(B)
        self.assertRaises(TypeError, attributes.register_class, A)

    def test_diamond_b1(self):
        mgr_factory = lambda cls: attributes.ClassManager(cls)

        class A(object): pass
        class B1(A): pass
        class B2(A):
            __sa_instrumentation_manager__ = mgr_factory
        class C(object): pass

        self.assertRaises(TypeError, attributes.register_class, B1)

    def test_diamond_b2(self):
        mgr_factory = lambda cls: attributes.ClassManager(cls)

        class A(object): pass
        class B1(A): pass
        class B2(A):
            __sa_instrumentation_manager__ = mgr_factory
        class C(object): pass

        self.assertRaises(TypeError, attributes.register_class, B2)

    def test_diamond_c_b(self):
        mgr_factory = lambda cls: attributes.ClassManager(cls)

        class A(object): pass
        class B1(A): pass
        class B2(A):
            __sa_instrumentation_manager__ = mgr_factory
        class C(object): pass

        attributes.register_class(C)
        self.assertRaises(TypeError, attributes.register_class, B1)


class OnLoadTest(_base.ORMTest):
    """Check that Events.on_load is not hit in regular attributes operations."""

    def test_basic(self):
        import pickle

        global A
        class A(object):
            pass

        def canary(instance): assert False

        try:
            attributes.register_class(A)
            manager = attributes.manager_of_class(A)
            manager.events.add_listener('on_load', canary)

            a = A()
            p_a = pickle.dumps(a)
            re_a = pickle.loads(p_a)
        finally:
            del A


class ExtendedEventsTest(_base.ORMTest):
    """Allow custom Events implementations."""

    @modifies_instrumentation_finders
    def test_subclassed(self):
        class MyEvents(attributes.Events):
            pass
        class MyClassManager(attributes.ClassManager):
            event_registry_factory = MyEvents

        attributes.instrumentation_finders.insert(0, lambda cls: MyClassManager)

        class A(object): pass

        attributes.register_class(A)
        manager = attributes.manager_of_class(A)
        assert isinstance(manager.events, MyEvents)


class NativeInstrumentationTest(_base.ORMTest):
    @with_lookup_strategy(sa.util.symbol('native'))
    def test_register_reserved_attribute(self):
        class T(object): pass

        attributes.register_class(T)
        manager = attributes.manager_of_class(T)

        sa = attributes.ClassManager.STATE_ATTR
        ma = attributes.ClassManager.MANAGER_ATTR

        fails = lambda method, attr: self.assertRaises(
            KeyError, getattr(manager, method), attr, property())

        fails('install_member', sa)
        fails('install_member', ma)
        fails('install_descriptor', sa)
        fails('install_descriptor', ma)

    @with_lookup_strategy(sa.util.symbol('native'))
    def test_mapped_stateattr(self):
        t = Table('t', MetaData(),
                  Column('id', Integer, primary_key=True),
                  Column(attributes.ClassManager.STATE_ATTR, Integer))

        class T(object): pass

        self.assertRaises(KeyError, mapper, T, t)

    @with_lookup_strategy(sa.util.symbol('native'))
    def test_mapped_managerattr(self):
        t = Table('t', MetaData(),
                  Column('id', Integer, primary_key=True),
                  Column(attributes.ClassManager.MANAGER_ATTR, Integer))

        class T(object): pass
        self.assertRaises(KeyError, mapper, T, t)


class MiscTest(_base.ORMTest):
    """Seems basic, but not directly covered elsewhere!"""

    def test_compileonattr(self):
        t = Table('t', MetaData(),
                  Column('id', Integer, primary_key=True),
                  Column('x', Integer))
        class A(object): pass
        mapper(A, t)

        a = A()
        assert a.id is None

    def test_compileonattr_rel(self):
        m = MetaData()
        t1 = Table('t1', m,
                   Column('id', Integer, primary_key=True),
                   Column('x', Integer))
        t2 = Table('t2', m,
                   Column('id', Integer, primary_key=True),
                   Column('t1_id', Integer, ForeignKey('t1.id')))
        class A(object): pass
        class B(object): pass
        mapper(A, t1, properties=dict(bs=relation(B)))
        mapper(B, t2)

        a = A()
        assert not a.bs

    def test_compileonattr_rel_backref_a(self):
        m = MetaData()
        t1 = Table('t1', m,
                   Column('id', Integer, primary_key=True),
                   Column('x', Integer))
        t2 = Table('t2', m,
                   Column('id', Integer, primary_key=True),
                   Column('t1_id', Integer, ForeignKey('t1.id')))

        class Base(object):
            def __init__(self, *args, **kwargs):
                pass

        for base in object, Base:
            class A(base): pass
            class B(base): pass
            mapper(A, t1, properties=dict(bs=relation(B, backref='a')))
            mapper(B, t2)

            b = B()
            assert b.a is None
            a = A()
            b.a = a

            session = create_session()
            session.add(b)
            assert a in session, "base is %s" % base

    def test_compileonattr_rel_backref_b(self):
        m = MetaData()
        t1 = Table('t1', m,
                   Column('id', Integer, primary_key=True),
                   Column('x', Integer))
        t2 = Table('t2', m,
                   Column('id', Integer, primary_key=True),
                   Column('t1_id', Integer, ForeignKey('t1.id')))

        class Base(object):
            def __init__(self): pass
        class Base_AKW(object):
            def __init__(self, *args, **kwargs): pass

        for base in object, Base, Base_AKW:
            class A(base): pass
            class B(base): pass
            mapper(A, t1)
            mapper(B, t2, properties=dict(a=relation(A, backref='bs')))

            a = A()
            b = B()
            b.a = a

            session = create_session()
            session.add(a)
            assert b in session, 'base: %s' % base


class FinderTest(_base.ORMTest):
    def test_standard(self):
        class A(object): pass

        attributes.register_class(A)

        eq_(type(attributes.manager_of_class(A)), attributes.ClassManager)

    def test_nativeext_interfaceexact(self):
        class A(object):
            __sa_instrumentation_manager__ = sa.orm.interfaces.InstrumentationManager

        attributes.register_class(A)
        ne_(type(attributes.manager_of_class(A)), attributes.ClassManager)

    def test_nativeext_submanager(self):
        class Mine(attributes.ClassManager): pass
        class A(object):
            __sa_instrumentation_manager__ = Mine

        attributes.register_class(A)
        eq_(type(attributes.manager_of_class(A)), Mine)

    @modifies_instrumentation_finders
    def test_customfinder_greedy(self):
        class Mine(attributes.ClassManager): pass
        class A(object): pass
        def find(cls):
            return Mine

        attributes.instrumentation_finders.insert(0, find)
        attributes.register_class(A)
        eq_(type(attributes.manager_of_class(A)), Mine)

    @modifies_instrumentation_finders
    def test_customfinder_pass(self):
        class A(object): pass
        def find(cls):
            return None

        attributes.instrumentation_finders.insert(0, find)
        attributes.register_class(A)
        eq_(type(attributes.manager_of_class(A)), attributes.ClassManager)


if __name__ == "__main__":
    testenv.main()