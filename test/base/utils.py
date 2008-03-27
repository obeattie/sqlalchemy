import testenv; testenv.configure_for_tests()
import threading, time, unittest
from sqlalchemy import util, sql, exceptions
from testlib import *
from testlib import sorted

class OrderedDictTest(TestBase):
    def test_odict(self):
        o = util.OrderedDict()
        o['a'] = 1
        o['b'] = 2
        o['snack'] = 'attack'
        o['c'] = 3

        self.assert_(o.keys() == ['a', 'b', 'snack', 'c'])
        self.assert_(o.values() == [1, 2, 'attack', 3])

        o.pop('snack')

        self.assert_(o.keys() == ['a', 'b', 'c'])
        self.assert_(o.values() == [1, 2, 3])

        o2 = util.OrderedDict(d=4)
        o2['e'] = 5

        self.assert_(o2.keys() == ['d', 'e'])
        self.assert_(o2.values() == [4, 5])

        o.update(o2)
        self.assert_(o.keys() == ['a', 'b', 'c', 'd', 'e'])
        self.assert_(o.values() == [1, 2, 3, 4, 5])

        o.setdefault('c', 'zzz')
        o.setdefault('f', 6)
        self.assert_(o.keys() == ['a', 'b', 'c', 'd', 'e', 'f'])
        self.assert_(o.values() == [1, 2, 3, 4, 5, 6])

class OrderedSetTest(TestBase):
    def test_mutators_against_iter(self):
        # testing a set modified against an iterator
        o = util.OrderedSet([3,2, 4, 5])

        self.assertEquals(o.difference(iter([3,4])),
                          util.OrderedSet([2,5]))
        self.assertEquals(o.intersection(iter([3,4, 6])),
                          util.OrderedSet([3, 4]))
        self.assertEquals(o.union(iter([3,4, 6])),
                          util.OrderedSet([2, 3, 4, 5, 6]))

class ColumnCollectionTest(TestBase):
    def test_in(self):
        cc = sql.ColumnCollection()
        cc.add(sql.column('col1'))
        cc.add(sql.column('col2'))
        cc.add(sql.column('col3'))
        assert 'col1' in cc
        assert 'col2' in cc

        try:
            cc['col1'] in cc
            assert False
        except exceptions.ArgumentError, e:
            assert str(e) == "__contains__ requires a string argument"

    def test_compare(self):
        cc1 = sql.ColumnCollection()
        cc2 = sql.ColumnCollection()
        cc3 = sql.ColumnCollection()
        c1 = sql.column('col1')
        c2 = c1.label('col2')
        c3 = sql.column('col3')
        cc1.add(c1)
        cc2.add(c2)
        cc3.add(c3)
        assert (cc1==cc2).compare(c1 == c2)
        assert not (cc1==cc3).compare(c2 == c3)

class ArgSingletonTest(unittest.TestCase):
    def test_cleanout(self):
        util.ArgSingleton.instances.clear()

        class MyClass(object):
            __metaclass__ = util.ArgSingleton
            def __init__(self, x, y):
                self.x = x
                self.y = y

        m1 = MyClass(3, 4)
        m2 = MyClass(1, 5)
        m3 = MyClass(3, 4)
        assert m1 is m3
        assert m2 is not m3
        assert len(util.ArgSingleton.instances) == 2

        m1 = m2 = m3 = None
        MyClass.dispose(MyClass)
        assert len(util.ArgSingleton.instances) == 0


class ImmutableSubclass(str):
    pass

class HashOverride(object):
    def __init__(self, value=None):
        self.value = value
    def __hash__(self):
        return hash(self.value)

class EqOverride(object):
    def __init__(self, value=None):
        self.value = value
    def __eq__(self, other):
        if isinstance(other, EqOverride):
            return self.value == other.value
        else:
            return False
    def __ne__(self, other):
        if isinstance(other, EqOverride):
            return self.value != other.value
        else:
            return True
class HashEqOverride(object):
    def __init__(self, value=None):
        self.value = value
    def __hash__(self):
        return hash(self.value)
    def __eq__(self, other):
        if isinstance(other, EqOverride):
            return self.value == other.value
        else:
            return False
    def __ne__(self, other):
        if isinstance(other, EqOverride):
            return self.value != other.value
        else:
            return True


class IdentitySetTest(unittest.TestCase):
    def assert_eq(self, identityset, expected_iterable):
        expected = sorted([id(o) for o in expected_iterable])
        found = sorted([id(o) for o in identityset])
        self.assertEquals(found, expected)

    def test_init(self):
        ids = util.IdentitySet([1,2,3,2,1])
        self.assert_eq(ids, [1,2,3])

        ids = util.IdentitySet(ids)
        self.assert_eq(ids, [1,2,3])

        ids = util.IdentitySet()
        self.assert_eq(ids, [])

        ids = util.IdentitySet([])
        self.assert_eq(ids, [])

        ids = util.IdentitySet(ids)
        self.assert_eq(ids, [])

    def test_add(self):
        for type_ in (object, ImmutableSubclass):
            data = [type_(), type_()]
            ids = util.IdentitySet()
            for i in range(2) + range(2):
                ids.add(data[i])
            self.assert_eq(ids, data)

        for type_ in (EqOverride, HashOverride, HashEqOverride):
            data = [type_(1), type_(1), type_(2)]
            ids = util.IdentitySet()
            for i in range(3) + range(3):
                ids.add(data[i])
            self.assert_eq(ids, data)

    def test_basic_sanity(self):
        IdentitySet = util.IdentitySet

        o1, o2, o3 = object(), object(), object()
        ids = IdentitySet([o1])
        ids.discard(o1)
        ids.discard(o1)
        ids.add(o1)
        ids.remove(o1)
        self.assertRaises(KeyError, ids.remove, o1)

        self.assert_(ids.copy() == ids)
        self.assert_(ids != None)
        self.assert_(not(ids == None))
        self.assert_(ids != IdentitySet([o1,o2,o3]))
        ids.clear()
        self.assert_(o1 not in ids)
        ids.add(o2)
        self.assert_(o2 in ids)
        self.assert_(ids.pop() == o2)
        ids.add(o1)
        self.assert_(len(ids) == 1)

        isuper = IdentitySet([o1,o2])
        self.assert_(ids < isuper)
        self.assert_(ids.issubset(isuper))
        self.assert_(isuper.issuperset(ids))
        self.assert_(isuper > ids)

        self.assert_(ids.union(isuper) == isuper)
        self.assert_(ids | isuper == isuper)
        self.assert_(isuper - ids == IdentitySet([o2]))
        self.assert_(isuper.difference(ids) == IdentitySet([o2]))
        self.assert_(ids.intersection(isuper) == IdentitySet([o1]))
        self.assert_(ids & isuper == IdentitySet([o1]))
        self.assert_(ids.symmetric_difference(isuper) == IdentitySet([o2]))
        self.assert_(ids ^ isuper == IdentitySet([o2]))

        ids.update(isuper)
        ids |= isuper
        ids.difference_update(isuper)
        ids -= isuper
        ids.intersection_update(isuper)
        ids &= isuper
        ids.symmetric_difference_update(isuper)
        ids ^= isuper

        ids.update('foobar')
        try:
            ids |= 'foobar'
            self.assert_(False)
        except TypeError:
            self.assert_(True)

        try:
            s = set([o1,o2])
            s |= ids
            self.assert_(False)
        except TypeError:
            self.assert_(True)

        self.assertRaises(TypeError, cmp, ids)
        self.assertRaises(TypeError, hash, ids)

    def test_difference(self):
        os1 = util.IdentitySet([1,2,3])
        os2 = util.IdentitySet([3,4,5])
        s1 = set([1,2,3])
        s2 = set([3,4,5])

        self.assertEquals(os1 - os2, util.IdentitySet([1, 2]))
        self.assertEquals(os2 - os1, util.IdentitySet([4, 5]))
        self.assertRaises(TypeError, lambda: os1 - s2)
        self.assertRaises(TypeError, lambda: os1 - [3, 4, 5])
        self.assertRaises(TypeError, lambda: s1 - os2)
        self.assertRaises(TypeError, lambda: s1 - [3, 4, 5])


class DictlikeIteritemsTest(unittest.TestCase):
    baseline = set([('a', 1), ('b', 2), ('c', 3)])

    def _ok(self, instance):
        iterator = util.dictlike_iteritems(instance)
        self.assertEquals(set(iterator), self.baseline)

    def _notok(self, instance):
        self.assertRaises(TypeError,
                          util.dictlike_iteritems,
                          instance)

    def test_dict(self):
        d = dict(a=1,b=2,c=3)
        self._ok(d)

    def test_subdict(self):
        class subdict(dict):
            pass
        d = subdict(a=1,b=2,c=3)
        self._ok(d)

    def test_UserDict(self):
        import UserDict
        d = UserDict.UserDict(a=1,b=2,c=3)
        self._ok(d)

    def test_object(self):
        self._notok(object())

    def test_duck_1(self):
        class duck1(object):
            def iteritems(duck):
                return iter(self.baseline)
        self._ok(duck1())

    def test_duck_2(self):
        class duck2(object):
            def items(duck):
                return list(self.baseline)
        self._ok(duck2())

    def test_duck_3(self):
        class duck3(object):
            def iterkeys(duck):
                return iter(['a', 'b', 'c'])
            def __getitem__(duck, key):
                return dict(a=1,b=2,c=3).get(key)
        self._ok(duck3())

    def test_duck_4(self):
        class duck4(object):
            def iterkeys(duck):
                return iter(['a', 'b', 'c'])
        self._notok(duck4())

    def test_duck_5(self):
        class duck5(object):
            def keys(duck):
                return ['a', 'b', 'c']
            def get(duck, key):
                return dict(a=1,b=2,c=3).get(key)
        self._ok(duck5())

    def test_duck_6(self):
        class duck6(object):
            def keys(duck):
                return ['a', 'b', 'c']
        self._notok(duck6())


class ArgInspectionTest(TestBase):
    def test_get_cls_kwargs(self):
        class A(object):
            def __init__(self, a):
                pass
        class A1(A):
            def __init__(self, a1):
                pass
        class A11(A1):
            def __init__(self, a11, **kw):
                pass
        class B(object):
            def __init__(self, b, **kw):
                pass
        class B1(B):
            def __init__(self, b1, **kw):
                pass
        class AB(A, B):
            def __init__(self, ab):
                pass
        class BA(B, A):
            def __init__(self, ba, **kwargs):
                pass
        class BA1(BA):
            pass
        class CAB(A, B):
            pass
        class CBA(B, A):
            pass
        class CAB1(A, B1):
            pass
        class CB1A(B1, A):
            pass
        class D(object):
            pass

        def test(cls, *expected):
            self.assertEquals(set(util.get_cls_kwargs(cls)), set(expected))

        test(A, 'a')
        test(A1, 'a1')
        test(A11, 'a11', 'a1')
        test(B, 'b')
        test(B1, 'b1', 'b')
        test(AB, 'ab')
        test(BA, 'ba', 'b', 'a')
        test(BA1, 'ba', 'b', 'a')
        test(CAB, 'a')
        test(CBA, 'b')
        test(CAB1, 'a')
        test(CB1A, 'b1', 'b')
        test(D)

    def test_get_func_kwargs(self):
        def f1(): pass
        def f2(foo): pass
        def f3(*foo): pass
        def f4(**foo): pass

        def test(fn, *expected):
            self.assertEquals(set(util.get_func_kwargs(fn)), set(expected))

        test(f1)
        test(f2, 'foo')
        test(f3)
        test(f4)

class SymbolTest(TestBase):
    def test_basic(self):
        sym1 = util.symbol('foo')
        assert sym1.name == 'foo'
        sym2 = util.symbol('foo')

        assert sym1 is sym2
        assert sym1 == sym2

        sym3 = util.symbol('bar')
        assert sym1 is not sym3
        assert sym1 != sym3

    def test_pickle(self):
        sym1 = util.symbol('foo')
        sym2 = util.symbol('foo')

        assert sym1 is sym2

        # default
        s = util.pickle.dumps(sym1)
        sym3 = util.pickle.loads(s)

        for protocol in 0, 1, 2:
            print protocol
            serial = util.pickle.dumps(sym1)
            rt = util.pickle.loads(serial)
            assert rt is sym1
            assert rt is sym2

class WeakIdentityMappingTest(TestBase):
    class Data(object):
        pass

    def _some_data(self, some=20):
        return [self.Data() for _ in xrange(some)]

    def _fixture(self, some=20):
        data = self._some_data()
        wim = util.WeakIdentityMapping()
        for idx, obj in enumerate(data):
            wim[obj] = idx
        return data, wim

    def test_delitem(self):
        data, wim = self._fixture()
        needle = data[-1]

        assert needle in wim
        assert id(needle) in wim.by_id
        assert wim[needle] == wim.by_id[id(needle)]

        del wim[needle]

        assert needle not in wim
        assert id(needle) not in wim.by_id
        assert len(wim) == (len(data) - 1)

        data.remove(needle)

        assert needle not in wim
        assert id(needle) not in wim.by_id
        assert len(wim) == len(data)

    def test_setitem(self):
        data, wim = self._fixture()

        o1, oid1 = data[-1], id(data[-1])

        assert o1 in wim
        assert oid1 in wim.by_id
        assert wim[o1] == wim.by_id[oid1]
        id_keys = set(wim.by_id.keys())

        wim[o1] = 1234
        assert o1 in wim
        assert oid1 in wim.by_id
        assert wim[o1] == wim.by_id[oid1]
        assert set(wim.by_id.keys()) == id_keys

        o2 = self.Data()
        oid2 = id(o2)

        wim[o2] = 5678
        assert o2 in wim
        assert oid2 in wim.by_id
        assert wim[o2] == wim.by_id[oid2]

    def test_pop(self):
        data, wim = self._fixture()
        needle = data[-1]

        needle = data.pop()
        assert needle in wim
        assert id(needle) in wim.by_id
        assert wim[needle] == wim.by_id[id(needle)]
        assert len(wim) == (len(data) + 1)

        wim.pop(needle)
        assert needle not in wim
        assert id(needle) not in wim.by_id
        assert len(wim) == len(data)

    def test_pop_default(self):
        data, wim = self._fixture()
        needle = data[-1]

        value = wim[needle]
        x = wim.pop(needle, 123)
        assert x != 123
        assert x == value
        assert needle not in wim
        assert id(needle) not in wim.by_id
        assert len(data) == (len(wim) + 1)

        n2 = self.Data()
        y = wim.pop(n2, 456)
        assert y == 456
        assert n2 not in wim
        assert id(n2) not in wim.by_id
        assert len(data) == (len(wim) + 1)

    def test_popitem(self):
        data, wim = self._fixture()
        (needle, idx) = wim.popitem()

        assert needle in data
        assert len(data) == (len(wim) + 1)
        assert id(needle) not in wim.by_id

    def test_setdefault(self):
        data, wim = self._fixture()

        o1 = self.Data()
        oid1 = id(o1)

        assert o1 not in wim

        res1 = wim.setdefault(o1, 123)
        assert o1 in wim
        assert oid1 in wim.by_id
        assert res1 == 123
        id_keys = set(wim.by_id.keys())

        res2 = wim.setdefault(o1, 456)
        assert o1 in wim
        assert oid1 in wim.by_id
        assert res2 == 123
        assert set(wim.by_id.keys()) == id_keys

        del wim[o1]
        assert o1 not in wim
        assert oid1 not in wim.by_id
        assert set(wim.by_id.keys()) != id_keys

        res3 = wim.setdefault(o1, 789)
        assert o1 in wim
        assert oid1 in wim.by_id
        assert res3 == 789
        assert set(wim.by_id.keys()) == id_keys

    def test_clear(self):
        data, wim = self._fixture()

        assert len(data) == len(wim) == len(wim.by_id)
        wim.clear()

        assert wim == {}
        assert wim.by_id == {}

    def test_update(self):
        data, wim = self._fixture()
        self.assertRaises(NotImplementedError, wim.update)

    def test_weak_clear(self):
        data, wim = self._fixture()

        assert len(data) == len(wim) == len(wim.by_id)

        del data[:]
        assert wim == {}
        assert wim.by_id == {}
        assert wim._weakrefs == {}

    def test_weak_single(self):
        data, wim = self._fixture()

        assert len(data) == len(wim) == len(wim.by_id)

        oid = id(data[0])
        del data[0]

        assert len(data) == len(wim) == len(wim.by_id)

        assert oid not in wim.by_id

    def test_weak_threadhop(self):
        data, wim = self._fixture()
        data = set(data)

        cv = threading.Condition()

        def empty(obj):
            cv.acquire()
            obj.clear()
            cv.notify()
            cv.release()

        th = threading.Thread(target=empty, args=(data,))

        cv.acquire()
        th.start()
        cv.wait()
        cv.release()

        assert wim == {}
        assert wim.by_id == {}
        assert wim._weakrefs == {}


class TestFormatArgspec(TestBase):
    def test_specs(self):
        def eq(fn, wanted, grouped=None):
            if grouped is None:
                parsed = util.format_argspec_plus(fn)
            else:
                parsed = util.format_argspec_plus(fn, grouped=grouped)
            self.assertEquals(parsed, wanted)

        eq(lambda: None,
           {'args': '()', 'self_arg': None,
            'apply_kw': '()', 'apply_pos': '()' })

        eq(lambda: None,
           {'args': '', 'self_arg': None,
            'apply_kw': '', 'apply_pos': '' },
           grouped=False)

        eq(lambda self: None,
           {'args': '(self)', 'self_arg': 'self',
            'apply_kw': '(self)', 'apply_pos': '(self)' })

        eq(lambda self: None,
           {'args': 'self', 'self_arg': 'self',
            'apply_kw': 'self', 'apply_pos': 'self' },
           grouped=False)

        eq(lambda *a: None,
           {'args': '(*a)', 'self_arg': None,
            'apply_kw': '(*a)', 'apply_pos': '(*a)' })

        eq(lambda **kw: None,
           {'args': '(**kw)', 'self_arg': None,
            'apply_kw': '(**kw)', 'apply_pos': '(**kw)' })

        eq(lambda *a, **kw: None,
           {'args': '(*a, **kw)', 'self_arg': None,
            'apply_kw': '(*a, **kw)', 'apply_pos': '(*a, **kw)' })

        eq(lambda a, *b: None,
           {'args': '(a, *b)', 'self_arg': 'a',
            'apply_kw': '(a, *b)', 'apply_pos': '(a, *b)' })

        eq(lambda a, **b: None,
           {'args': '(a, **b)', 'self_arg': 'a',
            'apply_kw': '(a, **b)', 'apply_pos': '(a, **b)' })

        eq(lambda a, *b, **c: None,
           {'args': '(a, *b, **c)', 'self_arg': 'a',
            'apply_kw': '(a, *b, **c)', 'apply_pos': '(a, *b, **c)' })

        eq(lambda a, b=1, **c: None,
           {'args': '(a, b=1, **c)', 'self_arg': 'a',
            'apply_kw': '(a, b=b, **c)', 'apply_pos': '(a, b, **c)' })

        eq(lambda a=1, b=2: None,
           {'args': '(a=1, b=2)', 'self_arg': 'a',
            'apply_kw': '(a=a, b=b)', 'apply_pos': '(a, b)' })

        eq(lambda a=1, b=2: None,
           {'args': 'a=1, b=2', 'self_arg': 'a',
            'apply_kw': 'a=a, b=b', 'apply_pos': 'a, b' },
           grouped=False)

    def test_init_grouped(self):
        object_spec = {
            'args': '(self)', 'self_arg': 'self',
            'apply_pos': '(self)', 'apply_kw': '(self)'}
        wrapper_spec = {
            'args': '(self, *args, **kwargs)', 'self_arg': 'self',
            'apply_pos': '(self, *args, **kwargs)',
            'apply_kw': '(self, *args, **kwargs)'}
        custom_spec = {
            'args': '(slef, a=123)', 'self_arg': 'slef', # yes, slef
            'apply_pos': '(slef, a)', 'apply_kw': '(slef, a=a)'}

        self._test_init(None, object_spec, wrapper_spec, custom_spec)
        self._test_init(True, object_spec, wrapper_spec, custom_spec)

    def test_init_bare(self):
        object_spec = {
            'args': 'self', 'self_arg': 'self',
            'apply_pos': 'self', 'apply_kw': 'self'}
        wrapper_spec = {
            'args': 'self, *args, **kwargs', 'self_arg': 'self',
            'apply_pos': 'self, *args, **kwargs',
            'apply_kw': 'self, *args, **kwargs'}
        custom_spec = {
            'args': 'slef, a=123', 'self_arg': 'slef', # yes, slef
            'apply_pos': 'slef, a', 'apply_kw': 'slef, a=a'}

        self._test_init(False, object_spec, wrapper_spec, custom_spec)

    def _test_init(self, grouped, object_spec, wrapper_spec, custom_spec):
        def eq(fn, wanted):
            if grouped is None:
                parsed = util.format_argspec_init(fn)
            else:
                parsed = util.format_argspec_init(fn, grouped=grouped)
            self.assertEquals(parsed, wanted)

        class O(object): pass

        eq(O.__init__, object_spec)

        class O(object):
            def __init__(self):
                pass

        eq(O.__init__, object_spec)

        class O(object):
            def __init__(slef, a=123):
                pass

        eq(O.__init__, custom_spec)

        class O(list): pass

        eq(O.__init__, wrapper_spec)

        class O(list):
            def __init__(self, *args, **kwargs):
                pass

        eq(O.__init__, wrapper_spec)

        class O(list):
            def __init__(self):
                pass

        eq(O.__init__, object_spec)

        class O(list):
            def __init__(slef, a=123):
                pass

        eq(O.__init__, custom_spec)

if __name__ == "__main__":
    testenv.main()
