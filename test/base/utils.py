import testbase
import unittest
from sqlalchemy import util, sql, exceptions
from testlib import *


class OrderedDictTest(PersistTest):
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

class ColumnCollectionTest(PersistTest):
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
        found = sorted(list(identityset))
        expected = sorted(expected_iterable)
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
        s = set([o1,o2])
        s |= ids
        self.assert_(isinstance(s, IdentitySet))

        self.assertRaises(TypeError, cmp, ids)
        self.assertRaises(TypeError, hash, ids)


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


if __name__ == "__main__":
    testbase.main()
