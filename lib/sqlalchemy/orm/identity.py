import weakref
import threading

from sqlalchemy.orm import attributes

class IdentityMap(dict):
    def __init__(self):
        self._mutable_attrs = weakref.WeakKeyDictionary()
        self._modified = False
        
    def add(self, state):
        raise NotImplementedError()
    
    def remove(self, state):
        raise NotImplementedError()
    
    def update(self, dict):
        raise NotImplementedError("IdentityMap uses add() to insert data")
    
    def clear(self):
        raise NotImplementedError("IdentityMap uses remove() to remove data")
        
    def _manage_incoming_state(self, state):
        if state.modified:
            self._modified = True
        if state.manager.has_mutable_scalars:
            self._mutable_attrs[state] = True
    
    def modified(self):
        """return True if any InstanceStates present have been marked as 'modified'."""
        
        if not self._modified:
            for state in self._mutable_attrs:
                if state.modified:
                    return True
            else:
                return False
        else:
            return True
            
    def _set_modified(self, value):
        self._modified = value
    
    modified = property(modified, _set_modified)
    
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

    def __init__(self):
        IdentityMap.__init__(self)
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
        self._manage_incoming_state(state)

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
    
    def prune(self):
        return 0
        
class StrongInstanceDict(IdentityMap):
    def all_states(self):
        return [attributes.state_getter(o) for o in self.values()]
    
    def contains_state(self, state):
        return state.key in self and attributes.state_getter(self[state.key]) is state
    
    def add(self, state):
        dict.__setitem__(self, state.key, state.obj())
        self._manage_incoming_state(state)
    
    def remove(self, state):
        dict.__delitem__(self, state.key)

    def prune(self):
        """prune unreferenced, non-dirty states."""
        
        ref_count = len(self)
        dirty = [s.obj() for s in self.all_states() if s.modified]
        keepers = weakref.WeakValueDictionary(self)
        dict.clear(self)
        dict.update(self, keepers)
        self._modified = bool(dirty)
        return ref_count - len(self)
        
        