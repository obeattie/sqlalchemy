# util.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import inspect, itertools, sets, sys, warnings, weakref
import __builtin__
types = __import__('types')

from sqlalchemy import exceptions

try:
    import thread, threading
except ImportError:
    import dummy_thread as thread
    import dummy_threading as threading

try:
    Set = set
    set_types = set, sets.Set
except NameError:
    set_types = sets.Set,
    # layer some of __builtin__.set's binop behavior onto sets.Set
    class Set(sets.Set):
        def _binary_sanity_check(self, other):
            pass

        def issubset(self, iterable):
            other = type(self)(iterable)
            return sets.Set.issubset(self, other)
        def __le__(self, other):
            sets.Set._binary_sanity_check(self, other)
            return sets.Set.__le__(self, other)
        def issuperset(self, iterable):
            other = type(self)(iterable)
            return sets.Set.issuperset(self, other)
        def __ge__(self, other):
            sets.Set._binary_sanity_check(self, other)
            return sets.Set.__ge__(self, other)

        # lt and gt still require a BaseSet
        def __lt__(self, other):
            sets.Set._binary_sanity_check(self, other)
            return sets.Set.__lt__(self, other)
        def __gt__(self, other):
            sets.Set._binary_sanity_check(self, other)
            return sets.Set.__gt__(self, other)

        def __ior__(self, other):
            if not isinstance(other, sets.BaseSet):
                return NotImplemented
            return sets.Set.__ior__(self, other)
        def __iand__(self, other):
            if not isinstance(other, sets.BaseSet):
                return NotImplemented
            return sets.Set.__iand__(self, other)
        def __ixor__(self, other):
            if not isinstance(other, sets.BaseSet):
                return NotImplemented
            return sets.Set.__ixor__(self, other)
        def __isub__(self, other):
            if not isinstance(other, sets.BaseSet):
                return NotImplemented
            return sets.Set.__isub__(self, other)

try:
    import cPickle as pickle
except ImportError:
    import pickle

try:
    reversed = __builtin__.reversed
except AttributeError:
    def reversed(seq):
        i = len(seq) -1
        while  i >= 0:
            yield seq[i]
            i -= 1
        raise StopIteration()

try:
    # Try the standard decimal for > 2.3 or the compatibility module
    # for 2.3, if installed.
    from decimal import Decimal
    decimal_type = Decimal
except ImportError:
    def Decimal(arg):
        if Decimal.warn:
            warn("True Decimal types not available on this Python, "
                "falling back to floats.")
            Decimal.warn = False
        return float(arg)
    Decimal.warn = True
    decimal_type = float

try:
    from operator import attrgetter
except:
    def attrgetter(attribute):
        return lambda value: getattr(value, attribute)

if sys.version_info >= (2, 5):
    class PopulateDict(dict):
        """a dict which populates missing values via a creation function.

        note the creation function takes a key, unlike collections.defaultdict.
        """

        def __init__(self, creator):
            self.creator = creator
        def __missing__(self, key):
            self[key] = val = self.creator(key)
            return val
else:
    class PopulateDict(dict):
        """a dict which populates missing values via a creation function."""

        def __init__(self, creator):
            self.creator = creator
        def __getitem__(self, key):
            try:
                return dict.__getitem__(self, key)
            except KeyError:
                self[key] = value = self.creator(key)
                return value

try:
    from collections import defaultdict
except ImportError:
    class defaultdict(dict):
        def __init__(self, default_factory=None, *a, **kw):
            if (default_factory is not None and
                not hasattr(default_factory, '__call__')):
                raise TypeError('first argument must be callable')
            dict.__init__(self, *a, **kw)
            self.default_factory = default_factory
        def __getitem__(self, key):
            try:
                return dict.__getitem__(self, key)
            except KeyError:
                return self.__missing__(key)
        def __missing__(self, key):
            if self.default_factory is None:
                raise KeyError(key)
            self[key] = value = self.default_factory()
            return value
        def __reduce__(self):
            if self.default_factory is None:
                args = tuple()
            else:
                args = self.default_factory,
            return type(self), args, None, None, self.iteritems()
        def copy(self):
            return self.__copy__()
        def __copy__(self):
            return type(self)(self.default_factory, self)
        def __deepcopy__(self, memo):
            import copy
            return type(self)(self.default_factory,
                              copy.deepcopy(self.items()))
        def __repr__(self):
            return 'defaultdict(%s, %s)' % (self.default_factory,
                                            dict.__repr__(self))

def to_list(x, default=None):
    if x is None:
        return default
    if not isinstance(x, (list, tuple)):
        return [x]
    else:
        return x

def to_set(x):
    if x is None:
        return Set()
    if not isinstance(x, Set):
        return Set(to_list(x))
    else:
        return x

def to_ascii(x):
    """Convert Unicode or a string with unknown encoding into ASCII."""

    if isinstance(x, str):
        return x.encode('string_escape')
    elif isinstance(x, unicode):
        return x.encode('unicode_escape')
    else:
        raise TypeError

def flatten_iterator(x):
    """Given an iterator of which further sub-elements may also be
    iterators, flatten the sub-elements into a single iterator.
    """

    for elem in x:
        if hasattr(elem, '__iter__'):
            for y in flatten_iterator(elem):
                yield y
        else:
            yield elem

class ArgSingleton(type):
    instances = weakref.WeakValueDictionary()

    def dispose(cls):
        for key in list(ArgSingleton.instances):
            if key[0] is cls:
                del ArgSingleton.instances[key]
    dispose = staticmethod(dispose)

    def __call__(self, *args):
        hashkey = (self, args)
        try:
            return ArgSingleton.instances[hashkey]
        except KeyError:
            instance = type.__call__(self, *args)
            ArgSingleton.instances[hashkey] = instance
            return instance

def get_cls_kwargs(cls):
    """Return the full set of inherited kwargs for the given `cls`.

    Probes a class's __init__ method, collecting all named arguments.  If the
    __init__ defines a **kwargs catch-all, then the constructor is presumed to
    pass along unrecognized keywords to it's base classes, and the collection
    process is repeated recursively on each of the bases.
    """

    for c in cls.__mro__:
        if '__init__' in c.__dict__:
            stack = Set([c])
            break
    else:
        return []

    args = Set()
    while stack:
        class_ = stack.pop()
        ctr = class_.__dict__.get('__init__', False)
        if not ctr or not isinstance(ctr, types.FunctionType):
            continue
        names, _, has_kw, _ = inspect.getargspec(ctr)
        args.update(names)
        if has_kw:
            stack.update(class_.__bases__)
    args.discard('self')
    return list(args)

def get_func_kwargs(func):
    """Return the full set of legal kwargs for the given `func`."""
    return inspect.getargspec(func)[0]

# from paste.deploy.converters
def asbool(obj):
    if isinstance(obj, (str, unicode)):
        obj = obj.strip().lower()
        if obj in ['true', 'yes', 'on', 'y', 't', '1']:
            return True
        elif obj in ['false', 'no', 'off', 'n', 'f', '0']:
            return False
        else:
            raise ValueError("String is not true/false: %r" % obj)
    return bool(obj)

def coerce_kw_type(kw, key, type_, flexi_bool=True):
    """If 'key' is present in dict 'kw', coerce its value to type 'type_' if
    necessary.  If 'flexi_bool' is True, the string '0' is considered false
    when coercing to boolean.
    """

    if key in kw and type(kw[key]) is not type_ and kw[key] is not None:
        if type_ is bool and flexi_bool:
            kw[key] = asbool(kw[key])
        else:
            kw[key] = type_(kw[key])

def duck_type_collection(specimen, default=None):
    """Given an instance or class, guess if it is or is acting as one of
    the basic collection types: list, set and dict.  If the __emulates__
    property is present, return that preferentially.
    """

    if hasattr(specimen, '__emulates__'):
        # canonicalize set vs sets.Set to a standard: util.Set
        if (specimen.__emulates__ is not None and
            issubclass(specimen.__emulates__, set_types)):
            return Set
        else:
            return specimen.__emulates__

    isa = isinstance(specimen, type) and issubclass or isinstance
    if isa(specimen, list): return list
    if isa(specimen, set_types): return Set
    if isa(specimen, dict): return dict

    if hasattr(specimen, 'append'):
        return list
    elif hasattr(specimen, 'add'):
        return Set
    elif hasattr(specimen, 'set'):
        return dict
    else:
        return default

def dictlike_iteritems(dictlike):
    """Return a (key, value) iterator for almost any dict-like object."""

    if hasattr(dictlike, 'iteritems'):
        return dictlike.iteritems()
    elif hasattr(dictlike, 'items'):
        return iter(dictlike.items())

    getter = getattr(dictlike, '__getitem__', getattr(dictlike, 'get', None))
    if getter is None:
        raise TypeError(
            "Object '%r' is not dict-like" % dictlike)

    if hasattr(dictlike, 'iterkeys'):
        def iterator():
            for key in dictlike.iterkeys():
                yield key, getter(key)
        return iterator()
    elif hasattr(dictlike, 'keys'):
        return iter([(key, getter(key)) for key in dictlike.keys()])
    else:
        raise TypeError(
            "Object '%r' is not dict-like" % dictlike)

def assert_arg_type(arg, argtype, name):
    if isinstance(arg, argtype):
        return arg
    else:
        if isinstance(argtype, tuple):
            raise exceptions.ArgumentError("Argument '%s' is expected to be one of type %s, got '%s'" % (name, ' or '.join(["'%s'" % str(a) for a in argtype]), str(type(arg))))
        else:
            raise exceptions.ArgumentError("Argument '%s' is expected to be of type '%s', got '%s'" % (name, str(argtype), str(type(arg))))

def warn_exception(func, *args, **kwargs):
    """executes the given function, catches all exceptions and converts to a warning."""
    try:
        return func(*args, **kwargs)
    except:
        warn("%s('%s') ignored" % sys.exc_info()[0:2])

class SimpleProperty(object):
    """A *default* property accessor."""

    def __init__(self, key):
        self.key = key

    def __set__(self, obj, value):
        setattr(obj, self.key, value)

    def __delete__(self, obj):
        delattr(obj, self.key)

    def __get__(self, obj, owner):
        if obj is None:
            return self
        else:
            return getattr(obj, self.key)

class NotImplProperty(object):
  """a property that raises ``NotImplementedError``."""

  def __init__(self, doc):
      self.__doc__ = doc

  def __set__(self, obj, value):
      raise NotImplementedError()

  def __delete__(self, obj):
      raise NotImplementedError()

  def __get__(self, obj, owner):
      if obj is None:
          return self
      else:
          raise NotImplementedError()

class OrderedProperties(object):
    """An object that maintains the order in which attributes are set upon it.

    Also provides an iterator and a very basic getitem/setitem
    interface to those attributes.

    (Not really a dict, since it iterates over values, not keys.  Not really
    a list, either, since each value must have a key associated; hence there is
    no append or extend.)
    """

    def __init__(self):
        self.__dict__['_data'] = OrderedDict()

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return self._data.itervalues()

    def __add__(self, other):
        return list(self) + list(other)

    def __setitem__(self, key, object):
        self._data[key] = object

    def __getitem__(self, key):
        return self._data[key]

    def __delitem__(self, key):
        del self._data[key]

    def __setattr__(self, key, object):
        self._data[key] = object

    def __getstate__(self):
        return {'_data': self.__dict__['_data']}

    def __setstate__(self, state):
        self.__dict__['_data'] = state['_data']

    def __getattr__(self, key):
        try:
            return self._data[key]
        except KeyError:
            raise AttributeError(key)

    def __contains__(self, key):
        return key in self._data

    def get(self, key, default=None):
        if key in self:
            return self[key]
        else:
            return default

    def keys(self):
        return self._data.keys()

    def has_key(self, key):
        return self._data.has_key(key)

    def clear(self):
        self._data.clear()

class OrderedDict(dict):
    """A Dictionary that returns keys/values/items in the order they were added."""

    def __init__(self, ____sequence=None, **kwargs):
        self._list = []
        if ____sequence is None:
            if kwargs:
                self.update(**kwargs)
        else:
            self.update(____sequence, **kwargs)

    def clear(self):
        self._list = []
        dict.clear(self)

    def update(self, ____sequence=None, **kwargs):
        if ____sequence is not None:
            if hasattr(____sequence, 'keys'):
                for key in ____sequence.keys():
                    self.__setitem__(key, ____sequence[key])
            else:
                for key, value in ____sequence:
                    self[key] = value
        if kwargs:
            self.update(kwargs)

    def setdefault(self, key, value):
        if key not in self:
            self.__setitem__(key, value)
            return value
        else:
            return self.__getitem__(key)

    def __iter__(self):
        return iter(self._list)

    def values(self):
        return [self[key] for key in self._list]

    def itervalues(self):
        return iter(self.values())

    def keys(self):
        return list(self._list)

    def iterkeys(self):
        return iter(self.keys())

    def items(self):
        return [(key, self[key]) for key in self.keys()]

    def iteritems(self):
        return iter(self.items())

    def __setitem__(self, key, object):
        if key not in self:
            self._list.append(key)
        dict.__setitem__(self, key, object)

    def __delitem__(self, key):
        dict.__delitem__(self, key)
        self._list.remove(key)

    def pop(self, key):
        value = dict.pop(self, key)
        self._list.remove(key)
        return value

    def popitem(self):
        item = dict.popitem(self)
        self._list.remove(item[0])
        return item

try:
    from threading import local as ThreadLocal
except ImportError:
    try:
        from dummy_threading import local as ThreadLocal
    except ImportError:
        class ThreadLocal(object):
            """An object in which attribute access occurs only within the context of the current thread."""

            def __init__(self):
                self.__dict__['_tdict'] = {}

            def __delattr__(self, key):
                try:
                    del self._tdict[(thread.get_ident(), key)]
                except KeyError:
                    raise AttributeError(key)

            def __getattr__(self, key):
                try:
                    return self._tdict[(thread.get_ident(), key)]
                except KeyError:
                    raise AttributeError(key)

            def __setattr__(self, key, value):
                self._tdict[(thread.get_ident(), key)] = value

class OrderedSet(Set):
    def __init__(self, d=None):
        Set.__init__(self)
        self._list = []
        if d is not None:
            self.update(d)

    def add(self, key):
        if key not in self:
            self._list.append(key)
        Set.add(self, key)

    def remove(self, element):
        Set.remove(self, element)
        self._list.remove(element)

    def discard(self, element):
        try:
            Set.remove(self, element)
        except KeyError:
            pass
        else:
            self._list.remove(element)

    def clear(self):
        Set.clear(self)
        self._list = []

    def __getitem__(self, key):
        return self._list[key]

    def __iter__(self):
        return iter(self._list)

    def __repr__(self):
      return '%s(%r)' % (self.__class__.__name__, self._list)

    __str__ = __repr__

    def update(self, iterable):
      add = self.add
      for i in iterable:
          add(i)
      return self

    __ior__ = update

    def union(self, other):
      result = self.__class__(self)
      result.update(other)
      return result

    __or__ = union

    def intersection(self, other):
        other = Set(other)
        return self.__class__([a for a in self if a in other])

    __and__ = intersection

    def symmetric_difference(self, other):
        other = Set(other)
        result = self.__class__([a for a in self if a not in other])
        result.update([a for a in other if a not in self])
        return result

    __xor__ = symmetric_difference

    def difference(self, other):
        other = Set(other)
        return self.__class__([a for a in self if a not in other])

    __sub__ = difference

    def intersection_update(self, other):
        other = Set(other)
        Set.intersection_update(self, other)
        self._list = [ a for a in self._list if a in other]
        return self

    __iand__ = intersection_update

    def symmetric_difference_update(self, other):
      Set.symmetric_difference_update(self, other)
      self._list =  [ a for a in self._list if a in self]
      self._list += [ a for a in other._list if a in self]
      return self

    __ixor__ = symmetric_difference_update

    def difference_update(self, other):
        Set.difference_update(self, other)
        self._list = [ a for a in self._list if a in self]
        return self

    __isub__ = difference_update

    if hasattr(Set, '__getstate__'):
        def __getstate__(self):
            base = Set.__getstate__(self)
            return base, self._list

        def __setstate__(self, state):
            Set.__setstate__(self, state[0])
            self._list = state[1]

class IdentitySet(object):
    """A set that considers only object id() for uniqueness.

    This strategy has edge cases for builtin types- it's possible to have
    two 'foo' strings in one of these sets, for example.  Use sparingly.
    """

    _working_set = Set

    def __init__(self, iterable=None):
        self._members = _IterableUpdatableDict()
        if iterable:
            for o in iterable:
                self.add(o)

    def add(self, value):
        self._members[id(value)] = value

    def __contains__(self, value):
        return id(value) in self._members

    def remove(self, value):
        del self._members[id(value)]

    def discard(self, value):
        try:
            self.remove(value)
        except KeyError:
            pass

    def pop(self):
        try:
            pair = self._members.popitem()
            return pair[1]
        except KeyError:
            raise KeyError('pop from an empty set')

    def clear(self):
        self._members.clear()

    def __cmp__(self, other):
        raise TypeError('cannot compare sets using cmp()')

    def __eq__(self, other):
        if isinstance(other, IdentitySet):
            return self._members == other._members
        else:
            return False

    def __ne__(self, other):
        if isinstance(other, IdentitySet):
            return self._members != other._members
        else:
            return True

    def issubset(self, iterable):
        other = type(self)(iterable)

        if len(self) > len(other):
            return False
        for m in itertools.ifilterfalse(other._members.has_key,
                                        self._members.iterkeys()):
            return False
        return True

    def __le__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        return self.issubset(other)

    def __lt__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        return len(self) < len(other) and self.issubset(other)

    def issuperset(self, iterable):
        other = type(self)(iterable)

        if len(self) < len(other):
            return False

        for m in itertools.ifilterfalse(self._members.has_key,
                                        other._members.iterkeys()):
            return False
        return True

    def __ge__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        return self.issuperset(other)

    def __gt__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        return len(self) > len(other) and self.issuperset(other)

    def union(self, iterable):
        result = type(self)()
        # testlib.pragma exempt:__hash__
        result._members.update(
            self._working_set(self._members.iteritems()).union(_iter_id(iterable)))
        return result

    def __or__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        return self.union(other)

    def update(self, iterable):
        self._members = self.union(iterable)._members

    def __ior__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        self.update(other)
        return self

    def difference(self, iterable):
        result = type(self)()
        # testlib.pragma exempt:__hash__
        result._members.update(
            self._working_set(self._members.iteritems()).difference(_iter_id(iterable)))
        return result

    def __sub__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        return self.difference(other)

    def difference_update(self, iterable):
        self._members = self.difference(iterable)._members

    def __isub__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        self.difference_update(other)
        return self

    def intersection(self, iterable):
        result = type(self)()
        # testlib.pragma exempt:__hash__
        result._members.update(
            self._working_set(self._members.iteritems()).intersection(_iter_id(iterable)))
        return result

    def __and__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        return self.intersection(other)

    def intersection_update(self, iterable):
        self._members = self.intersection(iterable)._members

    def __iand__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        self.intersection_update(other)
        return self

    def symmetric_difference(self, iterable):
        result = type(self)()
        # testlib.pragma exempt:__hash__
        result._members.update(
            self._working_set(self._members.iteritems()).symmetric_difference(_iter_id(iterable)))
        return result

    def __xor__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        return self.symmetric_difference(other)

    def symmetric_difference_update(self, iterable):
        self._members = self.symmetric_difference(iterable)._members

    def __ixor__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        self.symmetric_difference(other)
        return self

    def copy(self):
        return type(self)(self._members.itervalues())

    __copy__ = copy

    def __len__(self):
        return len(self._members)

    def __iter__(self):
        return self._members.itervalues()

    def __hash__(self):
        raise TypeError('set objects are unhashable')

    def __repr__(self):
        return '%s(%r)' % (type(self).__name__, self._members.values())

if sys.version_info >= (2, 4):
    _IterableUpdatableDict = dict
else:
    class _IterableUpdatableDict(dict):
        """A dict that can update(iterable) like Python 2.4+'s dict."""
        def update(self, __iterable=None, **kw):
            if __iterable is not None:
                if not isinstance(__iterable, dict):
                    __iterable = dict(__iterable)
                dict.update(self, __iterable)
            if kw:
                dict.update(self, **kw)

def _iter_id(iterable):
    """Generator: ((id(o), o) for o in iterable)."""
    for item in iterable:
        yield id(item), item


class OrderedIdentitySet(IdentitySet):
    class _working_set(OrderedSet):
        # a testing pragma: exempt the OIDS working set from the test suite's
        # "never call the user's __hash__" assertions.  this is a big hammer,
        # but it's safe here: IDS operates on (id, instance) tuples in the
        # working set.
        __sa_hash_exempt__ = True

    def __init__(self, iterable=None):
        IdentitySet.__init__(self)
        self._members = OrderedDict()
        if iterable:
            for o in iterable:
                self.add(o)


class UniqueAppender(object):
    """Only adds items to a collection once.

    Additional appends() of the same object are ignored.  Membership is
    determined by identity (``is a``) not equality (``==``).
    """

    def __init__(self, data, via=None):
        self.data = data
        self._unique = IdentitySet()
        if via:
            self._data_appender = getattr(data, via)
        elif hasattr(data, 'append'):
            self._data_appender = data.append
        elif hasattr(data, 'add'):
            # TODO: we think its a set here.  bypass unneeded uniquing logic ?
            self._data_appender = data.add

    def append(self, item):
        if item not in self._unique:
            self._data_appender(item)
            self._unique.add(item)

    def __iter__(self):
        return iter(self.data)

class ScopedRegistry(object):
    """A Registry that can store one or multiple instances of a single
    class on a per-thread scoped basis, or on a customized scope.

    createfunc
      a callable that returns a new object to be placed in the registry

    scopefunc
      a callable that will return a key to store/retrieve an object,
      defaults to ``thread.get_ident`` for thread-local objects.  Use
      a value like ``lambda: True`` for application scope.
    """

    def __init__(self, createfunc, scopefunc=None):
        self.createfunc = createfunc
        if scopefunc is None:
            self.scopefunc = thread.get_ident
        else:
            self.scopefunc = scopefunc
        self.registry = {}

    def __call__(self):
        key = self._get_key()
        try:
            return self.registry[key]
        except KeyError:
            return self.registry.setdefault(key, self.createfunc())

    def has(self):
        return self._get_key() in self.registry

    def set(self, obj):
        self.registry[self._get_key()] = obj

    def clear(self):
        try:
            del self.registry[self._get_key()]
        except KeyError:
            pass

    def _get_key(self):
        return self.scopefunc()

class symbol(object):
    """A constant symbol.

    >>> symbol('foo') is symbol('foo')
    True
    >>> symbol('foo')
    <symbol 'foo>

    A slight refinement of the MAGICCOOKIE=object() pattern.  The primary
    advantage of symbol() is its repr().  They are also singletons.
    """

    symbols = {}
    _lock = threading.Lock()

    def __new__(cls, name):
        try:
            symbol._lock.acquire()
            sym = cls.symbols.get(name)
            if sym is None:
                cls.symbols[name] = sym = object.__new__(cls, name)
            return sym
        finally:
            symbol._lock.release()

    def __init__(self, name):
        """Construct a new named symbol.

        Repeated calls of symbol('name') will all return the same instance.
        """

        assert isinstance(name, str)
        self.name = name
    def __repr__(self):
        return "<symbol '%s>" % self.name

def warn(msg):
    if isinstance(msg, basestring):
        warnings.warn(msg, exceptions.SAWarning, stacklevel=3)
    else:
        warnings.warn(msg, stacklevel=3)

def warn_deprecated(msg):
    warnings.warn(msg, exceptions.SADeprecationWarning, stacklevel=3)

def deprecated(func, message=None, add_deprecation_to_docstring=True):
    """Decorates a function and issues a deprecation warning on use.

    message
      If provided, issue message in the warning.  A sensible default
      is used if not provided.

    add_deprecation_to_docstring
      Default True.  If False, the wrapped function's __doc__ is left
      as-is.  If True, the 'message' is prepended to the docs if
      provided, or sensible default if message is omitted.
    """

    if message is not None:
        warning = message % dict(func=func.__name__)
    else:
        warning = "Call to deprecated function %s" % func.__name__

    def func_with_warning(*args, **kwargs):
        warnings.warn(exceptions.SADeprecationWarning(warning),
                      stacklevel=2)
        return func(*args, **kwargs)

    doc = func.__doc__ is not None and func.__doc__ or ''

    if add_deprecation_to_docstring:
        header = message is not None and warning or 'Deprecated.'
        doc = '\n'.join((header.rstrip(), doc))

    func_with_warning.__doc__ = doc
    func_with_warning.__dict__.update(func.__dict__)
    try:
        func_with_warning.__name__ = func.__name__
    except TypeError:
        pass

    return func_with_warning
