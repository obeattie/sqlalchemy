from sqlalchemy import util

class ClauseVisitor(object):
    """Traverses and visits ``ClauseElement`` structures."""

    __traverse_options__ = {}
    
    def traverse_single(self, obj, **kwargs):
        """visit a single element, without traversing its child elements."""
        
        for v in self._iterate_visitors:
            meth = getattr(v, "visit_%s" % obj.__visit_name__, None)
            if meth:
                return meth(obj, **kwargs)
    
    def iterate(self, obj):
        """traverse the given expression structure, returning an iterator of all elements."""
        
        opts = self.__traverse_options__
        stack = [obj]
        traversal = util.deque()
        while stack:
            t = stack.pop()
            traversal.appendleft(t)
            for c in t.get_children(**opts):
                stack.append(c)
        return iter(traversal)
        
    def traverse(self, obj):
        """traverse and visit the given expression structure."""

        ts = self.traverse_single
        for target in self.iterate(obj):
            ts(target)
        return obj

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
        opts = self.__traverse_options__
        cloned = dict([[k, k] for k in opts.get('stop_on', [])])
        ts = self.traverse_single

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
            ts(t)
            for c in t.get_children(**opts):
                stack.append(c)
        return obj


class ReplacingCloningVisitor(CloningVisitor):
    def before_clone(self, elem):
        """receive pre-copied elements during a cloning traversal.
        
        If the method returns a new element, the element is used 
        instead of creating a simple copy of the element.  Traversal 
        will halt on the newly returned element if it is re-encountered.
        """
        return None

    def traverse(self, obj):
        # TODO: the lazy clause visiting in test/orm/relationships.py/RelationTest2 
        # depends explicitly on the separate "stop_on" list being used in addition to
        # the cloned dictionary.  Add unit tests to test/sql/generative.py to test this
        opts = self.__traverse_options__
        cloned = {}
        stop_on = util.Set(opts.get('stop_on', []))
        ts = self.traverse_single

        def clone(element):
            for v in self._iterate_visitors:
                newelem = v.before_clone(element)
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
            ts(t)
            for c in t.get_children(**opts):
                stack.append(c)
        return obj

def traverse(clause, **kwargs):
    """traverse the given clause, applying visit functions passed in as keyword arguments."""
    
    clone = kwargs.pop('clone', False)
    if clone:
        if 'before_clone' in kwargs:
            base = ReplacingCloningVisitor
        else:
            base = CloningVisitor
    else:
        base = ClauseVisitor

    class Vis(base):
        __traverse_options__ = kwargs.pop('traverse_options', {})

        def traverse_single(self, obj):
            k = "visit_%s" % obj.__visit_name__ 
            if k in kwargs:
                return kwargs[k](obj)
    vis = Vis()
    if 'before_clone' in kwargs:
        setattr(vis, 'before_clone', kwargs['before_clone'])
    return vis.traverse(clause)

def iterate(clause, **traverse_options):
    """traverse the given expression structure, returning an iterator of all elements."""
    
    stack = [clause]
    traversal = util.deque()
    while stack:
        t = stack.pop()
        traversal.appendleft(t)
        for c in t.get_children(**traverse_options):
            stack.append(c)
    return iter(traversal)
