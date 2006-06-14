# attributes.py - manages object attributes
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import util
from exceptions import *
import weakref
from UserList import UserList

class SmartProperty(object):
    """a property object that instruments attribute access on object instances.  All methods correspond to 
    a single attribute on a particular class."""
    def __init__(self, manager, key, uselist, callable_, typecallable, trackparent=False, extension=None, **kwargs):
        self.manager = manager
        self.key = key
        self.uselist = uselist
        self.callable_ = callable_
        self.typecallable= typecallable
        self.trackparent = trackparent
        self.extension = extension
        self.kwargs = kwargs

    def __set__(self, obj, value):
        self.set(None, obj, value)

    def __delete__(self, obj):
        self.delete(None, obj)

    def __get__(self, obj, owner):
        if obj is None:
            return self
        return self.get(obj)

    def do_list_value_changed(self, obj, item, isdelete):
        pass
    def do_value_changed(self, obj, oldvalue, newvalue):
        pass

    def hasparent(self, item):
        """returns True if the given item is attached to a parent object 
        via the attribute represented by this SmartProperty."""
        return item._state.get(('hasparent', self))
        
    def sethasparent(self, item, value):
        if item is not None:
            item._state[('hasparent', self)] = value

    def get_history(self, obj, passive=False):
        """returns a new History object for the given object for this 
        SmartProperty's attribute."""
        return History(self, obj, passive=passive)

    def set_callable(self, obj, callable_):
        if callable_ is None:
            self.initialize(obj)
        else:
            obj._state[('callable', self)] = callable_

    def reset(self, obj):
        """removes any per-instance callable functions corresponding to this SmartProperty's attribute
        from the given object, and removes this SmartProperty's
        attribute from the given object's dictionary."""
        try:
            del obj._state[('callable', self)]
        except KeyError:
            pass
        self.clear(obj)
    
    def clear(self, obj):
        """removes this SmartProperty's attribute from the given object's dictionary."""
        try:
            del obj.__dict__[self.key]
        except KeyError:
            pass
            
    def get_callable(self, obj):
        if obj._state.has_key(('callable', self)):
            return obj._state[('callable', self)]
        elif self.callable_ is not None:
            return self.callable_(obj)
        else:
            return None
            
    def blank_list(self):
        if self.typecallable is not None:
            return self.typecallable()
        else:
            return []

    def _adapt_list(self, data):
        if self.typecallable is not None:
            t = self.typecallable()
            if data is not None:
                [t.append(x) for x in data]
            return t
        else:
            return data
            
    def initialize(self, obj):
        if self.uselist:
            l = ListInstrument(self, obj, self.blank_list())
            obj.__dict__[self.key] = l
            return l
        else:
            obj.__dict__[self.key] = None
            return None
            
    def get(self, obj, passive=False, raiseerr=True):
        """retrieves a value from the given object.  if a callable is assembled
        on this object's attribute, and passive is False, the callable will be executed
        and the resulting value will be set as the new value for this attribute."""
        try:
            return obj.__dict__[self.key]
        except KeyError:
            if obj._state.has_key('trigger'):
                trig = obj._state['trigger']
                del obj._state['trigger']
                trig()
                try:
                    return obj.__dict__[self.key]
                except KeyError:
                    pass
                    
            if self.uselist:
                callable_ = self.get_callable(obj)
                if callable_ is not None:
                    if passive:
                        return None
                    l = ListInstrument(self, obj, self._adapt_list(callable_()), init=False)
                    orig = obj._state.get('original', None)
                    if orig is not None:
                        orig.commit_attribute(self, obj, l)
                else:
                    l = ListInstrument(self, obj, self.blank_list(), init=False)
                obj.__dict__[self.key] = l
                return l
            else:
                callable_ = self.get_callable(obj)
                if callable_ is not None:
                    if passive:
                        return None
                    obj.__dict__[self.key] = self._adapt_list(callable_())
                    orig = obj._state.get('original', None)
                    if orig is not None:
                        orig.commit_attribute(self, obj)
                    return obj.__dict__[self.key]
                else:
                    if raiseerr:
                        raise AttributeError(self.key)
                    else:
                        return None
        
    def set(self, event, obj, value):
        """sets a value on the given object."""
        if event is not self:
            state = obj._state
            if state.has_key('trigger'):
                trig = state['trigger']
                del state['trigger']
                trig()
            if self.uselist:
                value = ListInstrument(self, obj, value)
            old = self.get(obj, raiseerr=False)
            obj.__dict__[self.key] = value
            state['modified'] = True
            if not self.uselist:
                if self.trackparent:
                    if value is not None:
                        self.sethasparent(value, True)
                    if old is not None:
                        self.sethasparent(old, False)
                self.do_value_changed(obj, old, value)
                if self.extension is not None:
                    self.extension.set(event or self, obj, value, old)
            
    def delete(self, event, obj):
        """deletes a value from the given object."""
        if event is not self:
            try:
                old = obj.__dict__[self.key]
                del obj.__dict__[self.key]
            except KeyError:
                raise AttributeError(self.key)
            obj._state['modified'] = True
            if self.trackparent:
                if old is not None:
                    self.sethasparent(old, False)
            self.do_value_changed(obj, old, None)
            if self.extension is not None:
                self.extension.delete(event or self, obj, old)

    def append(self, event, obj, value):
        """appends an element to a list based element or sets a scalar based element to the given value.
        Used by GenericBackrefExtension to "append" an item independent of list/scalar semantics."""
        if self.uselist:
            if event is not self:
                self.get(obj).append_with_event(value, event)
        else:
            self.set(event, obj, value)

    def remove(self, event, obj, value):
        """removes an element from a list based element or sets a scalar based element to None.
        Used by GenericBackrefExtension to "remove" an item independent of list/scalar semantics."""
        if self.uselist:
            if event is not self:
                self.get(obj).remove_with_event(value, event)
        else:
            self.set(event, obj, None)

    def append_event(self, event, obj, value):
        """called by ListInstrument when an item is appended"""
        obj._state['modified'] = True
        if self.trackparent:
            self.sethasparent(value, True)
        self.do_list_value_changed(obj, value, False)
        if self.extension is not None:
            self.extension.append(event or self, obj, value)
    
    def remove_event(self, event, obj, value):
        """called by ListInstrument when an item is removed"""
        obj._state['modified'] = True
        if self.trackparent:
            self.sethasparent(value, False)
        self.do_list_value_changed(obj, value, True)
        if self.extension is not None:
            self.extension.delete(event or self, obj, value)
                
class ListInstrument(object):
    """instruments a list-based attribute.  append and remove operations will fire off events to the 
    SmartProperty that manages the object's attribute."""
    def __init__(self, attr, obj, data, init=True):
        self.attr = attr
        self.__obj = weakref.ref(obj)
        self.key = attr.key
        self.data = data or attr.blank_list()
        if init:
            for x in self.data:
                self.__setrecord(x)
    def __getstate__(self):
        return {'key':self.key, 'obj':self.obj, 'data':self.data, 'attr':self.attr}
    def __setstate__(self, d):
        self.key = d['key']
        self.__obj = weakref.ref(d['obj'])
        self.data = d['data']
        self.attr = d['attr']
        
    obj = property(lambda s:s.__obj())

    def unchanged_items(self):
        """deprecated"""
        return self.attr.get_history(self.obj).unchanged_items
    def added_items(self):
        """deprecated"""
        return self.attr.get_history(self.obj).added_items
    def deleted_items(self):
        """deprecated"""
        return self.attr.get_history(self.obj).deleted_items

    def __iter__(self):
        return iter(self.data)
    def __repr__(self):
        return repr(self.data)
        
    def __getattr__(self, attr):
        """proxies unknown HistoryArraySet methods and attributes to the underlying
        data array.  this allows custom list classes to be used."""
        return getattr(self.data, attr)

    def __setrecord(self, item, event=None):
        self.attr.append_event(event, self.obj, item)
        return True

    def __delrecord(self, item, event=None):
        self.attr.remove_event(event, self.obj, item)
        return True
        
    def append_with_event(self, item, event):
        self.__setrecord(item, event)
        self.data.append(item)
        
    def append_without_event(self, item):
        self.data.append(item)
        
    def remove_with_event(self, item, event):
        self.__delrecord(item, event)
        self.data.remove(item)
            
    def append(self, item, _mapper_nohistory=False):
        """fires off dependent events, and appends the given item to the underlying list.
        _mapper_nohistory is a backwards compatibility hack; call append_without_event instead."""
        if _mapper_nohistory:
            self.append_without_event(item)
        else:
            self.__setrecord(item)
            self.data.append(item)
        
    def clear(self):
        if isinstance(self.data, dict):
            self.data.clear()
        else:
            self.data[:] = self.attr.blank_list()
            
    def __getitem__(self, i):
        return self.data[i]
    def __setitem__(self, i, item):
        if isinstance(i, slice):
            self.__setslice__(i.start, i.stop, item)
        else:
            self.__setrecord(item)
            self.data[i] = item
    def __delitem__(self, i):
        if isinstance(i, slice):
            self.__delslice__(i.start, i.stop)
        else:
            self.__delrecord(self.data[i], None)
            del self.data[i]

    def __lt__(self, other): return self.data <  self.__cast(other)
    def __le__(self, other): return self.data <= self.__cast(other)
    def __eq__(self, other): return self.data == self.__cast(other)
    def __ne__(self, other): return self.data != self.__cast(other)
    def __gt__(self, other): return self.data >  self.__cast(other)
    def __ge__(self, other): return self.data >= self.__cast(other)
    def __cast(self, other):
       if isinstance(other, ListInstrument): return other.data
       else: return other
    def __cmp__(self, other):
       return cmp(self.data, self.__cast(other))
    def __contains__(self, item): return item in self.data
    def __len__(self): return len(self.data)
    def __setslice__(self, i, j, other):
        i = max(i, 0); j = max(j, 0)
        [self.__delrecord(x) for x in self.data[i:]]
        g = [a for a in list(other) if self.__setrecord(a)]
        self.data[i:] = g
    def __delslice__(self, i, j):
        i = max(i, 0); j = max(j, 0)
        for a in self.data[i:j]:
            self.__delrecord(a)
        del self.data[i:j]
    def insert(self, i, item): 
        if self.__setrecord(item):
            self.data.insert(i, item)
    def pop(self, i=-1):
        item = self.data[i]
        self.__delrecord(item)
        return self.data.pop(i)
    def remove(self, item): 
        self.__delrecord(item)
        self.data.remove(item)
    def extend(self, item_list):
        for item in item_list:
            self.append(item)            
    def __add__(self, other):
        raise NotImplementedError()
    def __radd__(self, other):
        raise NotImplementedError()
    def __iadd__(self, other):
        raise NotImplementedError()

class AttributeExtension(object):
    """an abstract class which specifies an "onadd" or "ondelete" operation
    to be attached to an object property."""
    def append(self, event, obj, child):
        pass
    def delete(self, event, obj, child):
        pass
    def set(self, event, obj, child, oldchild):
        pass
        
class GenericBackrefExtension(AttributeExtension):
    """an attachment to a ScalarAttribute or ListAttribute which receives change events,
    and upon such an event synchronizes a two-way relationship.  A typical two-way
    relationship is a parent object containing a list of child objects, where each
    child object references the parent.  The other are two objects which contain 
    scalar references to each other."""
    def __init__(self, key):
        self.key = key
    def set(self, event, obj, child, oldchild):
        if oldchild is child:
            return
        if oldchild is not None:
            getattr(oldchild.__class__, self.key).remove(event, oldchild, obj)
        if child is not None:
            getattr(child.__class__, self.key).append(event, child, obj)
    def append(self, event, obj, child):
        getattr(child.__class__, self.key).append(event, child, obj)
    def delete(self, event, obj, child):
        getattr(child.__class__, self.key).remove(event, child, obj)

class Original(object):
    """stores the original state of an object when the commit() method on the attribute manager
    is called."""
    def __init__(self, manager, obj):
        self.data = {}
        for attr in manager.managed_attributes(obj.__class__):
            self.commit_attribute(attr, obj)

    def commit_attribute(self, attr, obj, value=False):
            if attr.uselist:
                if value is not False:
                    self.data[attr.key] = [x for x in value]
                elif obj.__dict__.has_key(attr.key):
                    self.data[attr.key] = [x for x in obj.__dict__[attr.key]]
            else:
                if value is not False:
                    self.data[attr.key] = value
                elif obj.__dict__.has_key(attr.key):
                    self.data[attr.key] = obj.__dict__[attr.key]
                        
    def rollback(self, manager, obj):
        for attr in manager.managed_attributes(obj.__class__):
            if self.data.has_key(attr.key):
                if attr.uselist:
                    obj.__dict__[attr.key][:] = self.data[attr.key]
                else:
                    obj.__dict__[attr.key] = self.data[attr.key]
            else:
                del obj.__dict__[attr.key]
                
    def __repr__(self):
        return "Original: %s" % repr(self.data)

class History(object):
    """calculates the "history" of a particular attribute on a particular instance."""
    def __init__(self, attr, obj, passive=False):
        self.attr = attr
        orig = obj._state.get('original', None)
        if orig is not None:
            original = orig.data.get(attr.key)
        else:
            original = None

        current = attr.get(obj, passive=passive, raiseerr=False)
        if attr.uselist:
            self._current = current
        else:
            self._current = [current]

        if attr.uselist:
            s = util.Set(original or [])
            self._added_items = []
            self._unchanged_items = []
            self._deleted_items = []
            if current:
                for a in current:
                    if a in s:
                        self._unchanged_items.append(a)
                    else:
                        self._added_items.append(a)
            for a in s:
                if a not in self._unchanged_items:
                    self._deleted_items.append(a)    
        else:
            if current is original:
                self._unchanged_items = [current]
                self._added_items = []
                self._deleted_items = []
            else:
                self._added_items = [current]
                if original is not None:
                    self._deleted_items = [original]
                else:
                    self._deleted_items = []
                self._unchanged_items = []
    def __iter__(self):
        return iter(self._current)
    def added_items(self):
        return self._added_items
    def unchanged_items(self):
        return self._unchanged_items
    def deleted_items(self):
        return self._deleted_items
    def hasparent(self, obj):
        """deprecated.  this should be called directly from the appropriate SmartProperty object."""
        return self.attr.hasparent(obj)
        
class AttributeManager(object):
    """maintains a set of per-attribute history container objects for a set of objects."""


    def rollback(self, *obj):
        """retrieves the committed history for each object in the given list, and rolls back the attributes
        each instance to their original value."""
        for o in obj:
            orig = o._state.get('original')
            if orig is not None:
                orig.rollback(self, o)
            else:
                self.__clear(o)
    
    def __clear(self, obj):
        for attr in self.managed_attributes(obj.__class__):
            try:
                del obj.__dict__[attr.key]
            except KeyError:
                pass
                
    def commit(self, *obj):
        for o in obj:
            o._state['original'] = Original(self, o)
            o._state['modified'] = False
            
    def managed_attributes(self, class_):
        """returns an iterator of all SmartProperty objects associated with the given class."""
        if not isinstance(class_, type):
            raise repr(class_) + " is not a type"
        for value in class_.__dict__.values():
            if isinstance(value, SmartProperty):
                yield value
                
    def is_modified(self, object):
        return object._state.get('modified', False)
        
    def remove(self, obj):
        """called when an object is totally being removed from memory"""
        # currently a no-op since the state of the object is attached to the object itself
        pass

    def init_attr(self, obj):
        """sets up the _managed_attributes dictionary on an object.  this happens anyway 
        when a particular attribute is first accessed on the object regardless
        of this method being called, however calling this first will result in an elimination of 
        AttributeError/KeyErrors that are thrown when get_unexec_history is called for the first
        time for a particular key."""
        obj._attr_state = {}

    def get_history(self, obj, key, **kwargs):
        return getattr(obj.__class__, key).get_history(obj, **kwargs)

    def get_as_list(self, obj, key, passive=False):
        """returns an attribute of the given name from the given object.  if the attribute
        is a scalar, returns it as a single-item list, otherwise returns the list based attribute.
        if the 'passive' keyword argument is True,
        and the attribute's value is to be produced by an unexecuted callable, the callable is not
        executed and a blank list is returned. """
        attr = getattr(obj.__class__, key)
        x = attr.get(obj, passive=passive)
        if x is None:
            return []
        elif attr.uselist:
            return x
        else:
            return [x]
            
    def trigger_history(self, obj, callable):
        """clears all managed object attributes and places the given callable
        as an attribute-wide "trigger", which will execute upon the next attribute access, after
        which the trigger is removed."""
        self.__clear(obj)
        try:
            del obj._state['original']
        except KeyError:
            pass
        obj._state['trigger'] = callable

    def untrigger_history(self, obj):
        """removes a trigger function set by trigger_history.  does not restore the previous state of the object."""
        del obj._state['trigger']
        
    def has_trigger(self, obj):
        """returns True if the given object has a trigger function set by trigger_history()."""
        return obj._state.has_key('trigger')
            
    def reset_history(self, obj, key):
        """removes any per-instance callable functions corresponding to given attribute key
        from the given object, and removes this attribute from the given object's dictionary."""
        attr = getattr(obj.__class__, key)
        attr.reset(obj)
        
    def reset_class_managed(self, class_):
        """removes all SmartProperty property objects from the given class."""
        for attr in self.managed_attributes(class_):
            delattr(class_, attr.key)

    def is_class_managed(self, class_, key):
        """returns True if the given key correponds to an instrumented property on the given class."""
        return hasattr(class_, key) and isinstance(getattr(class_, key), SmartProperty)

    def create_history(self, obj, key, uselist, callable_=None, **kwargs):
        getattr(obj.__class__, key).set_callable(obj, callable_)
        
    def create_prop(self, class_, key, uselist, callable_, typecallable, **kwargs):
        """creates a scalar property object, defaulting to SmartProperty, which 
        will communicate change events back to this AttributeManager."""
        return SmartProperty(self, key, uselist, callable_, typecallable, **kwargs)
    
    def register_attribute(self, class_, key, uselist, callable_=None, **kwargs):
        if not hasattr(class_, '_state'):
            def _get_state(self):
                try:
                    return self._attr_state
                except AttributeError:
                    self._attr_state = {}
                    return self._attr_state
            class_._state = property(_get_state)
            
        typecallable = getattr(class_, key, None)
        # TODO: look at existing properties on the class, and adapt them to the SmartProperty
        if isinstance(typecallable, SmartProperty):
            typecallable = None
        setattr(class_, key, self.create_prop(class_, key, uselist, callable_, typecallable=typecallable, **kwargs))

