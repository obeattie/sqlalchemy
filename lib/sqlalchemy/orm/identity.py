import UserDict
import weakref
import threading

class WeakInstanceDict(UserDict.UserDict):
    """similar to WeakValueDictionary, but wired towards 'state' objects."""

    def __init__(self, *args, **kw):
        self._wr = weakref.ref(self)
        # RLock because the mutex is used by a cleanup
        # handler, which can be called at any time (including within an already mutexed block)
        self._mutex = threading.RLock()
        UserDict.UserDict.__init__(self, *args, **kw)

    def __getitem__(self, key):
        state = self.data[key]
        o = state.obj()
        if o is None:
            o = state._check_resurrect(self)
        if o is None:
            raise KeyError, key
        return o

    def __contains__(self, key):
        try:
            state = self.data[key]
            o = state.obj()
            if o is None:
                o = state._check_resurrect(self)
        except KeyError:
            return False
        return o is not None

    def has_key(self, key):
        return key in self

    def __repr__(self):
        return "<InstanceDict at %s>" % id(self)

    def __setitem__(self, key, value):
        if key in self.data:
            self._mutex.acquire()
            try:
                if key in self.data:
                    self.data[key].instance_dict = None
            finally:
                self._mutex.release()
        self.data[key] = value._state
        value._state.instance_dict = self._wr

    def __delitem__(self, key):
        state = self.data[key]
        state.instance_dict = None
        del self.data[key]

    def get(self, key, default=None):
        try:
            state = self.data[key]
        except KeyError:
            return default
        else:
            o = state.obj()
            if o is None:
                # This should only happen
                return default
            else:
                return o

    def items(self):
        L = []
        for key, state in self.data.items():
            o = state.obj()
            if o is not None:
                L.append((key, o))
        return L

    def iteritems(self):
        for state in self.data.itervalues():
            value = state.obj()
            if value is not None:
                yield value._instance_key, value

    def iterkeys(self):
        return self.data.iterkeys()

    def __iter__(self):
        return self.data.iterkeys()

    def __len__(self):
        return len(self.values())

    def itervalues(self):
        for state in self.data.itervalues():
            instance = state.obj()
            if instance is not None:
                yield instance

    def values(self):
        L = []
        for state in self.data.values():
            o = state.obj()
            if o is not None:
                L.append(o)
        return L

    def popitem(self):
        raise NotImplementedError()

    def pop(self, key, *args):
        raise NotImplementedError()

    def setdefault(self, key, default=None):
        raise NotImplementedError()

    def update(self, dict=None, **kwargs):
        raise NotImplementedError()

    def copy(self):
        raise NotImplementedError()

    def all_states(self):
        return self.data.values()

class StrongInstanceDict(dict):
    def all_states(self):
        return [o._state for o in self.values()]
