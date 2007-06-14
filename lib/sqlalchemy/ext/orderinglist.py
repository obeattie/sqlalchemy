"""
A custom list implementation for mapped relations that syncs position in a
Python list with a position attribute on the mapped objects.
"""

__all__ = [ 'ordering_list' ]


def ordering_list(attr, count_from=None, **kw):
    """
    Prepares an OrderingList factory for use as an argument to a
    Mapper relation's 'collection_class' option.  Arguments are:

    attr
      Name of the mapped attribute to use for storage and retrieval of
      ordering information

    count_from (optional)
      Set up an integer-based ordering, starting at 'count_from'.  For example,
      ordering_list('pos', count_from=1) would create a 1-based list in SQL,
      storing the value in the 'pos' column.  Ignored if ordering_func is
      supplied.
      
    Passes along any keyword arguments to OrderingList constructor.
    """

    kw = _unsugar_count_from(count_from=count_from, **kw)
    return lambda: OrderingList(attr, **kw)

# Ordering utility functions
def count_from_0(index, collection):
    """Numbering function: consecutive integers starting at 0."""

    return index

def count_from_1(index, collection):
    """Numbering function: consecutive integers starting at 1."""

    return index + 1

def count_from_n_factory(start):
    """Numbering function: consecutive integers starting at arbitrary start."""

    def f(index, collection):
        return index + start
    try:
        f.__name__ = 'count_from_%i' % start
    except TypeError:
        pass
    return f

def _unsugar_count_from(**kw):
    """Keyword argument filter, prepares a simple ordering_func from
    a 'count_from' argument, otherwise passes ordering_func on unchanged."""
    
    count_from = kw.pop('count_from', None)
    if kw.get('ordering_func', None) is None and count_from is not None:
        if count_from == 0:
            kw['ordering_func'] = count_from_0
        elif count_from == 1:
            kw['ordering_func'] = count_from_1
        else:
            kw['ordering_func'] = count_from_n_factory(count_from)
    return kw

class OrderingList(list):
    def __init__(self, ordering_attr=None, ordering_func=None,
                 reorder_on_append=False):
        """
        A 'collection_class' list implementation that syncs position in a
        Python list with a position attribute on the mapped objects.

        This implementation counts on the list starting in the proper
        order, so be SURE to put an order_by on your relation.
        Arguments are:

        ordering_attr
          Name of the attribute that stores the object's order in the relation.

        ordering_func
          Optional.  A function that maps the position in the Python list to a
          value to store in the ordering_attr.  Values returned are usually
          (but need not be!) integers.

          ordering_funcs are called with two positional parameters: index of
          the element in the list, and the list itself.
          
          If omitted, list indexes are used for the attribute values.  Two
          basic pre-built numbering functions are provided: 'count_from_0' and
          'count_from_1'.  For more exotic examples like stepped numbering,
          alphabetical and Fibonacci numbering, see the unit tests.

        reorder_on_append
          Default false.  When appending an object with an existing (non-None)
          ordering value, that value will be left untouched unless
          reorder_on_append is true.  This is an optimization to avoid a
          variety of dangerous unexpected database writes.

          SQLAlchemy will add instances to the list via append() when your
          object loads.  If for some reason the result set from the database
          skips a step in the ordering (say, row '1' is missing but you get
          '2', '3', and '4'), reorder_on_append=True would immediately
          renumber the items to '1', '2', '3'.  If you have multiple sessions
          making changes, any of whom happen to load this collection even in
          passing, all of the sessions would try to 'clean up' the numbering
          in their commits, possibly causing all but one to fail with a
          concurrent modification error.  Spooky action at a distance.

          Recommend leaving this with the default of False, and just call
          ._reorder() if you're doing append() operations with previously
          ordered instances or doing housekeeping after manual sql operations.
        """

        self.ordering_attr = ordering_attr
        if ordering_func is None:
            ordering_func = count_from_0
        self.ordering_func = ordering_func
        self.reorder_on_append = reorder_on_append

    # More complex serialization schemes (multi column, e.g.) are possible by
    # subclassing and reimplementing these two methods.
    def _get_order_value(self, entity):
        return getattr(entity, self.ordering_attr)

    def _set_order_value(self, entity, value):
        setattr(entity, self.ordering_attr, value)

    def _reorder(self):
        """Sweep through the list and ensure that each object has accurate
        ordering information set."""
        
        for index, entity in enumerate(self):
            self._order_entity(index, entity, True)

    def _order_entity(self, index, entity, reorder=True):
        have = self._get_order_value(entity)

        # Don't disturb existing ordering if reorder is False
        if have is not None and not reorder:
            return

        should_be = self.ordering_func(index, self)
        if have <> should_be:
            self._set_order_value(entity, should_be)

    def append(self, entity):
        super(OrderingList, self).append(entity)
        self._order_entity(len(self) - 1, entity, self.reorder_on_append)

    def _raw_append(self, entity):
        """Append without any ordering behavior."""

        super(OrderingList, self).append(entity)

    def insert(self, index, entity):
        self[index:index] = [entity]

    def remove(self, entity):
        super(OrderingList, self).remove(entity)
        self._reorder()

    def pop(self, index=-1):
        entity = super(OrderingList, self).pop(index)
        self._reorder()
        return entity
        
    def __setitem__(self, index, entity):
        if isinstance(index, slice):
            for i in range(index.start or 0, index.stop or 0, index.step or 1):
                self.__setitem__(i, entity[i])
        else:
            self._order_entity(index, entity, True)
            super(OrderingList, self).__setitem__(index, entity)
            
    def __delitem__(self, index):
        super(OrderingList, self).__delitem__(index)
        self._reorder()

    def __setslice__(self, start, end, values):
        super(OrderingList, self).__setslice__(start, end, values)
        self._reorder()

    def __delslice__(self, start, end):
        super(OrderingList, self).__delslice__(start, end)
        self._reorder()
