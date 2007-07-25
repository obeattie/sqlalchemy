"""Contain the ``AssociationProxy`` class.

The ``AssociationProxy`` is a Python property object which provides
transparent proxied access to the endpoint of an association object.

See the example ``examples/association/proxied_association.py``.
"""

import weakref, itertools
import sqlalchemy.exceptions as exceptions
import sqlalchemy.orm as orm
import sqlalchemy.util as util

def association_proxy(targetcollection, attr, **kw):
    """Convenience function for use in mapped classes.  Implements a Python
    property representing a relation as a collection of simpler values.  The
    proxied property will mimic the collection type of the target (list, dict
    or set), or in the case of a one to one relation, a simple scalar value.

    targetcollection
      Name of the relation attribute we'll proxy to, usually created with
      'relation()' in a mapper setup.

    attr
      Attribute on the associated instances we'll proxy for.  For example,
      given a target collection of [obj1, obj2], a list created by this proxy
      property would look like
      [getattr(obj1, attr), getattr(obj2, attr)]

      If the relation is one-to-one or otherwise uselist=False, then simply:
      getattr(obj, attr)

    creator (optional)
      When new items are added to this proxied collection, new instances of
      the class collected by the target collection will be created.  For
      list and set collections, the target class constructor will be called
      with the 'value' for the new instance.  For dict types, two arguments
      are passed: key and value.

      If you want to construct instances differently, supply a 'creator'
      function that takes arguments as above and returns instances.

      For scalar relations, creator() will be called if the target is None.
      If the target is present, set operations are proxied to setattr() on the
      associated object.

      If you have an associated object with multiple attributes, you may set up
      multiple association proxies mapping to different attributes.  See the
      unit tests for examples, and for examples of how creator() functions can
      be used to construct the scalar relation on-demand in this situation.

    Passes along any other arguments to AssociationProxy
    """

    return AssociationProxy(targetcollection, attr, **kw)


class AssociationProxy(object):
    """A property object that automatically sets up `AssociationLists`
    on an object."""

    def __init__(self, targetcollection, attr, creator=None,
                 proxy_factory=None, proxy_bulk_set=None):
        """Arguments are:

          targetcollection
            Name of the collection we'll proxy to, usually created with
            'relation()' in a mapper setup.

          attr
            Attribute on the collected instances we'll proxy for.  For example,
            given a target collection of [obj1, obj2],
            a list created by this proxy property would look like
            [getattr(obj1, attr), getattr(obj2, attr)]

          creator
            Optional. When new items are added to this proxied collection, new
            instances of the class collected by the target collection will be
            created.  For list and set collections, the target class
            constructor will be called with the 'value' for the new instance.
            For dict types, two arguments are passed: key and value.

            If you want to construct instances differently, supply a 'creator'
            function that takes arguments as above and returns instances.

          proxy_factory
            Optional.  The type of collection to emulate is determined by
            sniffing the target collection.  If your collection type can't be
            determined by duck typing or you'd like to use a different collection
            implementation, you may supply a factory function to produce those
            collections.  Only applicable to non-scalar relations.

          proxy_bulk_set
            Optional, use with proxy_factory.  See the _set() method for
            details.
        """
        
        self.target_collection = targetcollection # backwards compat name...
        self.value_attr = attr
        self.creator = creator
        self.proxy_factory = proxy_factory
        self.proxy_bulk_set = proxy_bulk_set

        self.scalar = None
        self.owning_class = None
        self.key = '_%s_%s_%s' % (type(self).__name__,
                                  targetcollection, id(self))
        self.collection_class = None

    def _get_property(self):
        return orm.class_mapper(self.owning_class).get_property(self.target_collection)

    def _target_class(self):
        return self._get_property().mapper.class_
    target_class = property(_target_class)

    def _target_is_scalar(self):
        return not self._get_property().uselist

    def _lazy_collection(self, weakobjref):
        target = self.target_collection
        del self
        def lazy_collection():
            obj = weakobjref()
            if obj is None:
                raise exceptions.InvalidRequestError(
                    "stale association proxy, parent object has gone out of "
                    "scope")
            return getattr(obj, target)
        return lazy_collection
        
    def __get__(self, obj, class_):
        if obj is None:
            self.owning_class = class_
            return
        elif self.scalar is None:
            self.scalar = self._target_is_scalar()

        if self.scalar:
            return getattr(getattr(obj, self.target_collection), self.value_attr)
        else:
            try:
                return getattr(obj, self.key)
            except AttributeError:
                proxy = self._new(self._lazy_collection(weakref.ref(obj)))
                setattr(obj, self.key, proxy)
                return proxy

    def __set__(self, obj, values):
        if self.scalar is None:
            self.scalar = self._target_is_scalar()

        if self.scalar:
            creator = self.creator and self.creator or self.target_class
            target = getattr(obj, self.target_collection)
            if target is None:
                setattr(obj, self.target_collection, creator(values))
            else:
                setattr(target, self.value_attr, values)
        else:
            proxy = self.__get__(obj, None)
            proxy.clear()
            self._set(proxy, values)

    def __delete__(self, obj):
        delattr(obj, self.key)

    def _new(self, lazy_collection):
        creator = self.creator and self.creator or self.target_class
        self.collection_class = util.duck_type_collection(lazy_collection())

        if self.proxy_factory:
            return self.proxy_factory(lazy_collection, creator, self.value_attr)

        value_attr = self.value_attr
        getter = lambda o: getattr(o, value_attr)
        setter = lambda o, v: setattr(o, value_attr, v)
        
        if self.collection_class is list:
            return _AssociationList(lazy_collection, creator, getter, setter)
        elif self.collection_class is dict:
            kv_setter = lambda o, k, v: setattr(o, value_attr, v)
            return _AssociationDict(lazy_collection, creator, getter, kv_setter)
        elif self.collection_class is util.Set:
            return _AssociationSet(lazy_collection, creator, getter, setter)
        else:
            raise exceptions.ArgumentError(
                'could not guess which interface to use for '
                'collection_class "%s" backing "%s"; specify a '
                'proxy_factory and proxy_bulk_set manually' %
                (self.collection_class.__name__, self.target_collection))

    def _set(self, proxy, values):
        if self.proxy_bulk_set:
            self.proxy_bulk_set(proxy, values)
        elif self.collection_class is list:
            proxy.extend(values)
        elif self.collection_class is dict:
            proxy.update(values)
        elif self.collection_class is util.Set:
            proxy.update(values)
        else:
            raise exceptions.ArgumentError(
               'no proxy_bulk_set supplied for custom '
               'collection_class implementation')

class _AssociationList(object):
    """Generic proxying list which proxies list operations to a another list,
    converting association objects to and from a simplified value.
    """

    def __init__(self, lazy_collection, creator, getter, setter):
        """
        lazy_collection
          A callable returning a list-based collection of entities (usually
          an object attribute managed by a SQLAlchemy relation())
          
        creator
          A function that creates new target entities.  Given one parameter:
          value.  The assertion is assumed:
            obj = creator(somevalue)
            assert getter(obj) == somevalue

        getter
          A function.  Given an associated object, return the 'value'.

        setter
          A function.  Given an associated object and a value, store
          that value on the object.
        """

        self.lazy_collection = lazy_collection
        self.creator = creator
        self.getter = getter
        self.setter = setter

    col = property(lambda self: self.lazy_collection())

    # For compatibility with 0.3.1 through 0.3.7- pass kw through to creator.
    # (see append() below)
    def _create(self, value, **kw):
        return self.creator(value, **kw)

    def _get(self, object):
        return self.getter(object)

    def _set(self, object, value):
        return self.setter(object, value)

    def __len__(self):
        return len(self.col)

    def __nonzero__(self):
        if self.col:
            return True
        else:
            return False

    def __getitem__(self, index):
        return self._get(self.col[index])
    
    def __setitem__(self, index, value):
        if not isinstance(index, slice):
            self._set(self.col[index], value)
        else:
            if index.stop is None:
                stop = len(self)
            elif index.stop < 0:
                stop = len(self) + index.stop
            else:
                stop = index.stop
            step = index.step or 1

            rng = range(index.start or 0, stop, step)
            if step == 1:
                for i in rng:
                    del self[index.start]
                i = index.start
                for item in value:
                    self.insert(i, item)
                    i += 1
            else:
                if len(value) != len(rng):
                    raise ValueError(
                        "attempt to assign sequence of size %s to "
                        "extended slice of size %s" % (len(value),
                                                       len(rng)))
                for i, item in zip(rng, value):
                    self._set(self.col[i], item)

    def __delitem__(self, index):
        del self.col[index]

    def __contains__(self, value):
        for member in self.col:
            if self._get(member) == value:
                return True
        return False

    def __getslice__(self, start, end):
        return [self._get(member) for member in self.col[start:end]]

    def __setslice__(self, start, end, values):
        members = [self._create(v) for v in values]
        self.col[start:end] = members

    def __delslice__(self, start, end):
        del self.col[start:end]

    def __iter__(self):
        """Iterate over proxied values.

        For the actual domain objects, iterate over .col instead or
        just use the underlying collection directly from its property
        on the parent.
        """

        for member in self.col:
            yield self._get(member)
        raise StopIteration

    # For compatibility with 0.3.1 through 0.3.7- pass kw through to creator
    # on append() only.  (Can't on __setitem__, __contains__, etc., obviously.)
    def append(self, value, **kw):
        item = self._create(value, **kw)
        self.col.append(item)

    def count(self, value):
        return sum([1 for _ in
                    itertools.ifilter(lambda v: v == value, iter(self))])

    def extend(self, values):
        for v in values:
            self.append(v)

    def insert(self, index, value):
        self.col[index:index] = [self._create(value)]

    def pop(self, index=-1):
        return self.getter(self.col.pop(index))

    def remove(self, value):
        for i, val in enumerate(self):
            if val == value:
                del self.col[i]
                return
        raise ValueError("value not in list")

    def reverse(self):
        """Not supported, use reversed(mylist)"""

        raise NotImplementedError

    def sort(self):
        """Not supported, use sorted(mylist)"""

        raise NotImplementedError

    def clear(self):
        del self.col[0:len(self.col)]

    def __eq__(self, other): return list(self) == other
    def __ne__(self, other): return list(self) != other
    def __lt__(self, other): return list(self) < other
    def __le__(self, other): return list(self) <= other
    def __gt__(self, other): return list(self) > other
    def __ge__(self, other): return list(self) >= other
    def __cmp__(self, other): return cmp(list(self), other)

    def copy(self):
        return list(self)

    def __repr__(self):
        return repr(list(self))

    def hash(self):
        raise TypeError("%s objects are unhashable" % type(self).__name__)

_NotProvided = object()
class _AssociationDict(object):
    """Generic proxying list which proxies dict operations to a another dict,
    converting association objects to and from a simplified value.
    """

    def __init__(self, lazy_collection, creator, getter, setter):
        """
        lazy_collection
          A callable returning a dict-based collection of entities (usually
          an object attribute managed by a SQLAlchemy relation())
          
        creator
          A function that creates new target entities.  Given two parameters:
          key and value.  The assertion is assumed:
            obj = creator(somekey, somevalue)
            assert getter(somekey) == somevalue

        getter
          A function.  Given an associated object and a key, return the 'value'.

        setter
          A function.  Given an associated object, a key and a value, store
          that value on the object.
        """

        self.lazy_collection = lazy_collection
        self.creator = creator
        self.getter = getter
        self.setter = setter

    col = property(lambda self: self.lazy_collection())

    def _create(self, key, value):
        return self.creator(key, value)

    def _get(self, object):
        return self.getter(object)

    def _set(self, object, key, value):
        return self.setter(object, key, value)

    def __len__(self):
        return len(self.col)

    def __nonzero__(self):
        if self.col:
            return True
        else:
            return False

    def __getitem__(self, key):
        return self._get(self.col[key])
    
    def __setitem__(self, key, value):
        if key in self.col:
            self._set(self.col[key], key, value)
        else:
            self.col[key] = self._create(key, value)

    def __delitem__(self, key):
        del self.col[key]

    def __contains__(self, key):
        return key in self.col
    has_key = __contains__

    def __iter__(self):
        return self.col.iterkeys()

    def clear(self):
        self.col.clear()

    def __eq__(self, other): return dict(self) == other
    def __ne__(self, other): return dict(self) != other
    def __lt__(self, other): return dict(self) < other
    def __le__(self, other): return dict(self) <= other
    def __gt__(self, other): return dict(self) > other
    def __ge__(self, other): return dict(self) >= other
    def __cmp__(self, other): return cmp(dict(self), other)

    def __repr__(self):
        return repr(dict(self.items()))

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def setdefault(self, key, default=None):
        if key not in self.col:
            self.col[key] = self._create(key, default)
            return default
        else:
            return self[key]

    def keys(self):
        return self.col.keys()
    def iterkeys(self):
        return self.col.iterkeys()

    def values(self):
        return [ self._get(member) for member in self.col.values() ]
    def itervalues(self):
        for key in self.col:
            yield self._get(self.col[key])
        raise StopIteration

    def items(self):
        return [(k, self._get(self.col[k])) for k in self]
    def iteritems(self):
        for key in self.col:
            yield (key, self._get(self.col[key]))
        raise StopIteration

    def pop(self, key, default=_NotProvided):
        if default is _NotProvided:
            member = self.col.pop(key)
        else:
            member = self.col.pop(key, default)
        return self._get(member)

    def popitem(self):
        item = self.col.popitem()
        return (item[0], self._get(item[1]))
    
    def update(self, *a, **kw):
        if len(a) > 1:
            raise TypeError('update expected at most 1 arguments, got %i' %
                            len(a))
        elif len(a) == 1:
            seq_or_map = a[0]
            for item in seq_or_map:
                if isinstance(item, tuple):
                    self[item[0]] = item[1]
                else:
                    self[item] = seq_or_map[item]

        for key, value in kw:
            self[key] = value

    def copy(self):
        return dict(self.items())

    def hash(self):
        raise TypeError("%s objects are unhashable" % type(self).__name__)

class _AssociationSet(object):
    """Generic proxying list which proxies set operations to a another set,
    converting association objects to and from a simplified value.
    """

    def __init__(self, lazy_collection, creator, getter, setter):
        """
        collection
          A callable returning a set-based collection of entities (usually an
          object attribute managed by a SQLAlchemy relation())
          
        creator
          A function that creates new target entities.  Given one parameter:
          value.  The assertion is assumed:
            obj = creator(somevalue)
            assert getter(obj) == somevalue

        getter
          A function.  Given an associated object, return the 'value'.

        setter
          A function.  Given an associated object and a value, store
          that value on the object.
        """

        self.lazy_collection = lazy_collection
        self.creator = creator
        self.getter = getter
        self.setter = setter

    col = property(lambda self: self.lazy_collection())

    def _create(self, value):
        return self.creator(value)

    def _get(self, object):
        return self.getter(object)

    def _set(self, object, value):
        return self.setter(object, value)

    def __len__(self):
        return len(self.col)

    def __nonzero__(self):
        if self.col:
            return True
        else:
            return False

    def __contains__(self, value):
        for member in self.col:
            if self._get(member) == value:
                return True
        return False

    def __iter__(self):
        """Iterate over proxied values.  For the actual domain objects,
        iterate over .col instead or just use the underlying collection
        directly from its property on the parent."""
        for member in self.col:
            yield self._get(member)
        raise StopIteration

    def add(self, value):
        if value not in self:
            self.col.add(self._create(value))

    # for discard and remove, choosing a more expensive check strategy rather
    # than call self.creator()
    def discard(self, value):
        for member in self.col:
            if self._get(member) == value:
                self.col.discard(member)
                break

    def remove(self, value):
        for member in self.col:
            if self._get(member) == value:
                self.col.discard(member)
                return
        raise KeyError(value)

    def pop(self):
        if not self.col:
            raise KeyError('pop from an empty set')
        member = self.col.pop()
        return self._get(member)

    def update(self, other):
        for value in other:
            self.add(value)

    __ior__ = update

    def _set(self):
        return util.Set(iter(self))

    def union(self, other):
        return util.Set(self).union(other)

    __or__ = union

    def difference(self, other):
        return util.Set(self).difference(other)

    __sub__ = difference

    def difference_update(self, other):
        for value in other:
            self.discard(value)

    __isub__ = difference_update

    def intersection(self, other):
        return util.Set(self).intersection(other)

    __and__ = intersection

    def intersection_update(self, other):
        want, have = self.intersection(other), util.Set(self)

        remove, add = have - want, want - have

        for value in remove:
            self.remove(value)
        for value in add:
            self.add(value)

    __iand__ = intersection_update

    def symmetric_difference(self, other):
        return util.Set(self).symmetric_difference(other)

    __xor__ = symmetric_difference

    def symmetric_difference_update(self, other):
        want, have = self.symmetric_difference(other), util.Set(self)

        remove, add = have - want, want - have

        for value in remove:
            self.remove(value)
        for value in add:
            self.add(value)

    __ixor__ = symmetric_difference_update

    def issubset(self, other):
        return util.Set(self).issubset(other)
    
    def issuperset(self, other):
        return util.Set(self).issuperset(other)
                
    def clear(self):
        self.col.clear()

    def copy(self):
        return util.Set(self)

    def __eq__(self, other): return util.Set(self) == other
    def __ne__(self, other): return util.Set(self) != other
    def __lt__(self, other): return util.Set(self) < other
    def __le__(self, other): return util.Set(self) <= other
    def __gt__(self, other): return util.Set(self) > other
    def __ge__(self, other): return util.Set(self) >= other

    def __repr__(self):
        return repr(util.Set(self))

    def hash(self):
        raise TypeError("%s objects are unhashable" % type(self).__name__)
