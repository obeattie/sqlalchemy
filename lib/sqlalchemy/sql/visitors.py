from sqlalchemy import util

class _VisitorMeta(type):
    def __init__(cls, classname, bases, dict_):
        cls._name_registry = [name for name in dict_ if name.startswith('visit_')]
        for b in bases:
            if isinstance(b, _VisitorMeta):
                cls._name_registry += b._name_registry
        return type.__init__(cls, classname, bases, dict_)
    
    def __call__(cls, *args, **kwargs):
        obj = type.__call__(cls, *args, **kwargs)
        obj._registry = dict([(name[6:], getattr(obj, name)) for name in cls._name_registry])
        return obj
        
    def __setattr__(cls, key, value):
        if key.startswith('visit_'):
            cls._name_registry.append(key)
        type.__setattr__(cls, key, value)

class ClauseVisitor(object):
    # for some reason, when VisitorMeta is uncommented here, specifically the "obj._registry[x] = <somemethod>"
    # causes test/sql/testtypes.py UnicodeTest to hang in a heisenberg-like fashion (i.e. error
    # disappears when SQL logging is turned on) for Postgres on OSX.  has to do with the dict
    # being attached to the instance *and* having methods from the instance inside of it.
    
#    __metaclass__ = _VisitorMeta
    __traverse_options__ = {}
    
    def traverse_single(self, obj):
        for v in self._iterate_visitors:
            meth = getattr(v, "visit_%s" % obj.__visit_name__, None)
            #meth = v._registry.get(obj.__visit_name__, None)
            if meth:
                return meth(obj)
    
    def iterate(self, obj):
        """traverse the given expression structure, returning an iterator of all elements."""

        return iterate(obj, self.__traverse_options__)
        
    def traverse(self, obj):
        """traverse and visit the given expression structure."""

        visitors = {}
#        for v in reversed(list(self._iterate_visitors)):
#            visitors.update(v._registry)

        for name in dir(self):
            if name.startswith('visit_'):
                visitors[name[6:]] = getattr(self, name)
            
        return traverse(obj, self.__traverse_options__, visitors)

    def _iterate_visitors(self):
        """iterate through this visitor and each 'chained' visitor."""
        
        v = self
        while v:
            yield v
            v = getattr(v, '_next', None)
    _iterate_visitors = property(_iterate_visitors)

    def chain(self, visitor):
        """'chain' an additional ClauseVisitor onto this ClauseVisitor.
        
        the chained visitor will receive all visit events after this one.
        """
        tail = list(self._iterate_visitors)[-1]
        tail._next = visitor
        return self

class CloningVisitor(ClauseVisitor):
    def copy_and_process(self, list_):
        """Apply cloned traversal to the given list of elements, and return the new list."""

        return [self.traverse(x) for x in list_]

    def traverse(self, obj):
        """traverse and visit the given expression structure."""

        visitors = {}
#        for v in reversed(list(self._iterate_visitors)):
#            visitors.update(v._registry)

        for name in dir(self):
            if name.startswith('visit_'):
                visitors[name[6:]] = getattr(self, name)
            
        return cloned_traverse(obj, self.__traverse_options__, visitors)

class ReplacingCloningVisitor(CloningVisitor):
    def replace(self, elem):
        """receive pre-copied elements during a cloning traversal.
        
        If the method returns a new element, the element is used 
        instead of creating a simple copy of the element.  Traversal 
        will halt on the newly returned element if it is re-encountered.
        """
        return None

    def traverse(self, obj):
        """traverse and visit the given expression structure."""

        def replace(elem):
            for v in self._iterate_visitors:
                e = v.replace(elem)
                if e:
                    return e
        return replacement_traverse(obj, self.__traverse_options__, replace)

def iterate(obj, opts):
    """traverse the given expression structure, returning an iterator."""

    stack = [obj]
    traversal = util.deque()
    while stack:
        t = stack.pop()
        yield t
        for c in t.get_children(**opts):
            stack.append(c)

def iterate_depthfirst(obj, opts):
    """traverse the given expression structure, returning an iterator.
    
    traversal is configured to be depth-first.
    
    """
    stack = [obj]
    traversal = util.deque()
    while stack:
        t = stack.pop()
        traversal.appendleft(t)
        for c in t.get_children(**opts):
            stack.append(c)
    return iter(traversal)

def traverse(obj, opts, visitors):
    """traverse and visit the given expression structure."""

    for target in iterate(obj, opts):
        meth = visitors.get(target.__visit_name__, None)
        if meth:
            meth(target)
    return obj

def cloned_traverse(obj, opts, visitors):
    cloned = {}

    def clone(element):
        if element not in cloned:
            cloned[element] = element._clone()
        return cloned[element]

    obj = clone(obj)
    stack = [obj]

    while stack:
        t = stack.pop()
        if t in cloned:
            continue
        t._copy_internals(clone=clone)

        meth = visitors.get(t.__visit_name__, None)
        if meth:
            meth(t)

        for c in t.get_children(**opts):
            stack.append(c)
    return obj

def replacement_traverse(obj, opts, replace):
    cloned = {}
    stop_on = util.Set(opts.get('stop_on', []))

    def clone(element):
        newelem = replace(element)
        if newelem:
            stop_on.add(newelem)
            return newelem

        if element not in cloned:
            cloned[element] = element._clone()
        return cloned[element]

    obj = clone(obj)
    stack = [obj]
    while stack:
        t = stack.pop()
        if t in stop_on:
            continue
        t._copy_internals(clone=clone)
        for c in t.get_children(**opts):
            stack.append(c)
    return obj
