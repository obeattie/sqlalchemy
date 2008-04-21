import testenv; testenv.configure_for_tests()
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.orm import aliased
from sqlalchemy.orm import mapper
from testlib import *


class AliasedClassTest(TestBase):
    def point_map(self, cls):
        table = Table('point', MetaData(),
                    Column('id', Integer(), primary_key=True),
                    Column('x', Integer),
                    Column('y', Integer))
        mapper(cls, table)
        return table

    def test_simple(self):
        class Point(object):
            pass
        table = self.point_map(Point)

        alias = aliased(Point)

        assert alias.id
        assert alias.x
        assert alias.y

        assert Point.id.__clause_element__().table is table
        assert alias.id.__clause_element__().table is not table

    def test_notcallable(self):
        class Point(object):
            pass
        table = self.point_map(Point)
        alias = aliased(Point)

        self.assertRaises(TypeError, alias)

    def test_instancemethods(self):
        class Point(object):
            def zero(self):
                self.x, self.y = 0, 0

        table = self.point_map(Point)
        alias = aliased(Point)

        assert Point.zero
        assert not getattr(alias, 'zero')

    def test_classmethods(self):
        class Point(object):
            @classmethod
            def max_x(cls):
                return 100

        table = self.point_map(Point)
        alias = aliased(Point)

        assert Point.max_x
        assert alias.max_x
        assert Point.max_x() == alias.max_x()

    def test_simpleproperties(self):
        class Point(object):
            @property
            def max_x(self):
                return 100

        table = self.point_map(Point)
        alias = aliased(Point)

        assert Point.max_x
        assert Point.max_x != 100
        assert alias.max_x
        assert Point.max_x is alias.max_x

    def test_descriptors(self):
        class descriptor(object):
            """Tortured..."""
            def __init__(self, fn):
                self.fn = fn
            def __get__(self, obj, owner):
                if obj is not None:
                    return self.fn(obj, obj)
                else:
                    return self
            def method(self):
                return 'method'

        class Point(object):
            center = (0, 0)
            @descriptor
            def thing(self, arg):
                return arg.center

        table = self.point_map(Point)
        alias = aliased(Point)

        assert Point.thing != (0, 0)
        assert Point().thing == (0, 0)
        assert Point.thing.method() == 'method'

        assert alias.thing != (0, 0)
        assert alias.thing.method() == 'method'

    def test_hybrid_descriptors(self):
        from sqlalchemy import Column  # override testlib's override
        import new

        class MethodDescriptor(object):
            def __init__(self, func):
                self.func = func
            def __get__(self, instance, owner):
                if instance is None:
                    args = (self.func, owner, owner.__class__)
                else:
                    args = (self.func, instance, owner)
                return new.instancemethod(*args)

        class PropertyDescriptor(object):
            def __init__(self, fget, fset, fdel):
                self.fget = fget
                self.fset = fset
                self.fdel = fdel
            def __get__(self, instance, owner):
                if instance is None:
                    return self.fget(owner)
                else:
                    return self.fget(instance)
            def __set__(self, instance, value):
                self.fset(instance, value)
            def __delete__(self, instance):
                self.fdel(instance)
        hybrid = MethodDescriptor
        def hybrid_property(fget, fset=None, fdel=None):
            return PropertyDescriptor(fget, fset, fdel)

        def assert_table(expr, table):
            for child in expr.get_children():
                if isinstance(child, Column):
                    assert child.table is table

        class Point(object):
            def __init__(self, x, y):
                self.x, self.y = x, y
            @hybrid
            def left_of(self, other):
                return self.x < other.x

            double_x = hybrid_property(lambda self: self.x * 2)

        table = self.point_map(Point)
        alias = aliased(Point)
        alias_table = alias.x.__clause_element__().table
        assert table is not alias_table

        p1 = Point(-10, -10)
        p2 = Point(20, 20)

        assert p1.left_of(p2)
        assert p1.double_x == -20

        assert_table(Point.double_x, table)
        assert_table(alias.double_x, alias_table)

        assert_table(Point.left_of(p2), table)
        assert_table(alias.left_of(p2), alias_table)


if __name__ == '__main__':
    testenv.main()


    
    