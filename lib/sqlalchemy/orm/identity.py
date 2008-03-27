import UserDict
import weakref
import threading

from sqlalchemy.orm import attributes

class IdentityMap(dict):
    def add(self, state):
        raise NotImplementedError()
    
    def remove(self, state):
        raise NotImplementedError()

    def has_key(self, key):
        return key in self
        
    def popitem(self):
        raise NotImplementedError("IdentityMap uses remove() to remove data")

    def pop(self, key, *args):
        raise NotImplementedError("IdentityMap uses remove() to remove data")

    def setdefault(self, key, default=None):
        raise NotImplementedError("IdentityMap uses add() to insert data")

    def copy(self):
        raise NotImplementedError()

    def __setitem__(self, key, value):
        raise NotImplementedError("IdentityMap uses add() to insert data")

    def __delitem__(self, key):
        raise NotImplementedError("IdentityMap uses remove() to remove data")
        
class WeakInstanceDict(IdentityMap):

    def __init__(self, *args, **kw):
        self._wr = weakref.ref(self)
        # RLock because the mutex is used by a cleanup
        # handler, which can be called at any time (including within an already mutexed block)
        self._mutex = threading.RLock()

    def __getitem__(self, key):
        state = dict.__getitem__(self, key)
        o = state.obj()
        if o is None:
            o = state._check_resurrect(self)
        if o is None:
            raise KeyError, key
        return o

    def __contains__(self, key):
        try:
            state = dict.__getitem__(self, key)
            o = state.obj()
            if o is None:
                o = state._check_resurrect(self)
        except KeyError:
            return False
        return o is not None
    
    def contains_state(self, state):
        return dict.get(self, state.key) is state
        
    def add(self, state):
        if state.key in self:
            self._mutex.acquire()
            try:
                if state.key in self:
                    dict.__getitem__(self, state.key).instance_dict = None
            finally:
                self._mutex.release()
        dict.__setitem__(self, state.key, state)
        state.instance_dict = self._wr

    def remove(self, state):
        state.instance_dict = None
        dict.__delitem__(self, state.key)
    
    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default
            
    def items(self):
        return list(self.iteritems())

    def iteritems(self):
        for state in dict.itervalues(self):
            value = state.obj()
            if value is not None:
                yield state.key, value

    def itervalues(self):
        for state in dict.itervalues(self):
            instance = state.obj()
            if instance is not None:
                yield instance

    def values(self):
        return list(self.itervalues())

    def all_states(self):
        return dict.values(self)

class StrongInstanceDict(IdentityMap):
    def all_states(self):
        return [attributes.state_getter(o) for o in self.values()]
    
    def contains_state(self, state):
        return state.key in self and attributes.state_getter(self[state.key]) is state
    
    def add(self, state):
        dict.__setitem__(self, state.key, state.obj())
    
    def remove(self, state):
        dict.__delitem__(self, state.key)
        
        