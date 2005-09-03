# util.py
# Copyright (C) 2005 Michael Bayer mike_mp@zzzcomputing.com
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

__ALL__ = ['OrderedProperties', 'OrderedDict']
import thread, weakref

class OrderedProperties(object):

    def __init__(self):
        self.__dict__['_list'] = []
    
    def keys(self):
        return self._list
        
    def __iter__(self):
        return iter([self[x] for x in self._list])
    
    def __setitem__(self, key, object):
        setattr(self, key, object)
        
    def __getitem__(self, key):
        return getattr(self, key)
        
    def __setattr__(self, key, object):
        if not hasattr(self, key):
            self._list.append(key)
    
        self.__dict__[key] = object
    

class OrderedDict(dict):
    """A Dictionary that keeps its own internal ordering"""
    def __init__(self, values = None):
        self.list = []
        if values is not None:
            for val in values:
                self.update(val)

    def keys(self):
        return self.list
    
    def update(self, dict):
        for key in dict.keys():
            self.__setitem__(key, dict[key])
    
    def setdefault(self, key, value):
        if not self.has_key(key):
            self.__setitem__(key, value)
            return value
        else:
            return self.__getitem__(key)
            
    def values(self):
        return map(lambda key: self[key], self.list)
        
    def __iter__(self):
        return iter(self.list)

    def itervalues(self):
        return iter([self[key] for key in self.list])
        
    def iterkeys(self):return self.__iter__()
    
    def iteritems(self):
        return iter([(key, self[key]) for key in self.keys()])
        
    def __setitem__(self, key, object):
        if not self.has_key(key):
            self.list.append(key)

        dict.__setitem__(self, key, object)
        
    def __getitem__(self, key):
        return dict.__getitem__(self, key)


class ThreadLocal(object):
    def __init__(self):
        object.__setattr__(self, 'tdict', {})

    def __getattribute__(self, key):
        try:
            return object.__getattribute__(self, 'tdict')["%d_%s" % (thread.get_ident(), key)]
        except KeyError:
            raise AttributeError(key)
        
    def __setattr__(self, key, value):
        object.__getattribute__(self, 'tdict')["%d_%s" % (thread.get_ident(), key)] = value
        

class Set(object):
    def __init__(self, iter = None):
        self.map  = {}
        if iter is not None:
            for i in iter:
                self.append(i)

    def __iter__(self):
        return iter(self.map.values())
        
    def append(self, item):
        self.map[item] = item
        
    def __delitem__(self, key):
        del self.map[key]
        
    def __getitem__(self, key):
        return self.map[key]
        
        
class ScopedRegistry(object):
    def __init__(self, createfunc):
        self.createfunc = createfunc
        self.application = createfunc()
        self.threadlocal = {}
        self.scopes = {
                    'application' : {'call' : self._call_application, 'clear' : self._clear_application}, 
                    'thread' : {'call' : self._call_thread, 'clear':self._clear_thread}
                    }

    def __call__(self, scope):
        return self.scopes[scope]['call']()

    def clear(self, scope):
        return self.scopes[scope]['clear']()
        
    def _call_thread(self):
        try:
            return self.threadlocal[thread.get_ident()]
        except KeyError:
            return self.threadlocal.setdefault(thread.get_ident(), self.createfunc())

    def _clear_thread(self):
        try:
            del self.threadlocal[thread.get_ident()]
        except KeyError:
            pass

    def _call_application(self):
        return self.application

    def _clear_application(self):
        self.application = createfunc()
                
            