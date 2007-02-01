# util.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import weakref, UserList, time, string, inspect, sys, sets
try:
    import thread, threading
except ImportError:
    import dummy_thread as thread
    import dummy_threading as threading

from sqlalchemy.exceptions import *
import __builtin__

try:
    Set = set
except:
    Set = sets.Set
    
def to_list(x):
    if x is None:
        return None
    if not isinstance(x, list) and not isinstance(x, tuple):
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

def flatten_iterator(x):
    """given an iterator of which further sub-elements may also be iterators,
    flatten the sub-elements into a single iterator."""
    for elem in x:
        if hasattr(elem, '__iter__'):
            for y in flatten_iterator(elem):
                yield y
        else:
            yield elem
            
def reversed(seq):
    try:
        return __builtin__.reversed(seq)
    except:
        def rev():
            i = len(seq) -1
            while  i >= 0:
                yield seq[i]
                i -= 1
            raise StopIteration()
        return rev()

class ArgSingleton(type):
    instances = {}
    def dispose_static(self, *args):
        hashkey = (self, args)
        #if hashkey in ArgSingleton.instances:
        del ArgSingleton.instances[hashkey]
    def __call__(self, *args):
        hashkey = (self, args)
        try:
            return ArgSingleton.instances[hashkey]
        except KeyError:
            instance = type.__call__(self, *args)
            ArgSingleton.instances[hashkey] = instance
            return instance

def get_cls_kwargs(cls):
    """return the full set of legal kwargs for the given cls"""
    kw = []
    for c in cls.__mro__:
        cons = c.__init__
        if hasattr(cons, 'func_code'):
            for vn in cons.func_code.co_varnames:
                if vn != 'self':
                    kw.append(vn)
    return kw
                        
class SimpleProperty(object):
    """a "default" property accessor."""
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

class OrderedProperties(object):
    """
    An object that maintains the order in which attributes are set upon it.
    also provides an iterator and a very basic getitem/setitem interface to those attributes.
    
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
    _data = property(lambda s:s.__dict__['_data'])
    def __getattr__(self, key):
        try:
            return self._data[key]
        except KeyError:
            raise AttributeError(key)
    def __contains__(self, key):
        return key in self._data
    def get(self, key, default=None):
        if self.has_key(key):
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
    """A Dictionary that returns keys/values/items in the order they were added"""
    def __init__(self, d=None, **kwargs):
        self._list = []
        self.update(d, **kwargs)
    def keys(self):
        return list(self._list)
    def clear(self):
        self._list = []
        dict.clear(self)
    def update(self, d=None, **kwargs):
        # d can be a dict or sequence of keys/values
        if d:
            if hasattr(d, 'iteritems'):
                seq = d.iteritems()
            else:
                seq = d
            for key, value in seq:
                self.__setitem__(key, value)
        if kwargs:
            self.update(kwargs)
    def setdefault(self, key, value):
        if not self.has_key(key):
            self.__setitem__(key, value)
            return value
        else:
            return self.__getitem__(key)
    def values(self):
        return [self[key] for key in self._list]
    def __iter__(self):
        return iter(self._list)
    def itervalues(self):
        return iter([self[key] for key in self._list])
    def iterkeys(self): 
        return self.__iter__()
    def iteritems(self):
        return iter([(key, self[key]) for key in self.keys()])
    def __delitem__(self, key):
        try:
            del self._list[self._list.index(key)]
        except ValueError:
            raise KeyError(key)
        dict.__delitem__(self, key)
    def __setitem__(self, key, object):
        if not self.has_key(key):
            self._list.append(key)
        dict.__setitem__(self, key, object)
    def __getitem__(self, key):
        return dict.__getitem__(self, key)

class ThreadLocal(object):
    """an object in which attribute access occurs only within the context of the current thread"""
    def __init__(self):
        self.__dict__['_tdict'] = {}
    def __delattr__(self, key):
        try:
            del self._tdict["%d_%s" % (thread.get_ident(), key)]
        except KeyError:
            raise AttributeError(key)
    def __getattr__(self, key):
        try:
            return self._tdict["%d_%s" % (thread.get_ident(), key)]
        except KeyError:
            raise AttributeError(key)
    def __setattr__(self, key, value):
        self._tdict["%d_%s" % (thread.get_ident(), key)] = value

class DictDecorator(dict):
    """a Dictionary that delegates items not found to a second wrapped dictionary."""
    def __init__(self, decorate):
        self.decorate = decorate
    def __getitem__(self, key):
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            return self.decorate[key]
    def __repr__(self):
        return dict.__repr__(self) + repr(self.decorate)

class OrderedSet(sets.Set):
    def __init__(self, iterable=None):
        """Construct a set from an optional iterable."""
        self._data = OrderedDict()
        if iterable is not None: 
          self._update(iterable)

class UniqueAppender(object):
    def __init__(self, data):
        self.data = data
        if hasattr(data, 'append'):
            self._data_appender = data.append
        elif hasattr(data, 'add'):
            self._data_appender = data.add
        self.set = Set()
    def append(self, item):
        if item not in self.set:
            self.set.add(item)
            self._data_appender(item)
        
class ScopedRegistry(object):
    """a Registry that can store one or multiple instances of a single class 
    on a per-thread scoped basis, or on a customized scope
    
    createfunc - a callable that returns a new object to be placed in the registry
    scopefunc - a callable that will return a key to store/retrieve an object,
    defaults to thread.get_ident for thread-local objects.  use a value like
    lambda: True for application scope.
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
    def set(self, obj):
        self.registry[self._get_key()] = obj
    def clear(self):
        try:
            del self.registry[self._get_key()]
        except KeyError:
            pass
    def _get_key(self):
        return self.scopefunc()


