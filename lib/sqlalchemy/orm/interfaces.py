
class MapperProperty(object):
    """manages the relationship of a Mapper to a single class attribute, as well
    as that attribute as it appears on individual instances of the class, including 
    attribute instrumentation, attribute access, loading behavior, and dependency calculations."""
    def setup(self, querycontext, **kwargs):
        """called when a statement is being constructed.  """
        pass
    def execute(self, selectcontext, instance, row, identitykey, isnew):
        """called when the mapper receives a row.  instance is the parent instance
        corresponding to the row. """
        raise NotImplementedError()
    def cascade_iterator(self, type, object, recursive=None):
        return []
    def cascade_callable(self, type, object, callable_, recursive=None):
        return []
    def copy(self):
        raise NotImplementedError()
    def get_criterion(self, query, key, value):
        """Returns a WHERE clause suitable for this MapperProperty corresponding to the 
        given key/value pair, where the key is a column or object property name, and value
        is a value to be matched.  This is only picked up by PropertyLoaders.
            
        this is called by a Query's join_by method to formulate a set of key/value pairs into 
        a WHERE criterion that spans multiple tables if needed."""
        return None
    def set_parent(self, parent):
        self.parent = parent
    def init(self, key, parent):
        """called after all mappers are compiled to assemble relationships between 
        mappers, establish instrumented class attributes"""
        self.key = key
        self.localparent = parent
        if not hasattr(self, 'inherits'):
            self.inherits = None
        self.do_init()
    def adapt_to_inherited(self, key, newparent):
        """adapt this MapperProperty to a new parent, assuming the new parent is an inheriting
        descendant of the old parent.  """
        p = self.copy()
        newparent._compile_property(key, p, init=False)
        p.localparent = newparent
        p.parent = self.parent
        p.inherits = getattr(self, 'inherits', self)
    def do_init(self):
        """template method for subclasses"""
        pass
    def register_deleted(self, object, uow):
        """called when the instance is being deleted"""
        pass
    def register_dependencies(self, *args, **kwargs):
        """called by the Mapper in response to the UnitOfWork calling the Mapper's
        register_dependencies operation.  Should register with the UnitOfWork all 
        inter-mapper dependencies as well as dependency processors (see UOW docs for more details)"""
        pass
    def is_primary(self):
        """a return value of True indicates we are the primary MapperProperty for this loader's
        attribute on our mapper's class.  It means we can set the object's attribute behavior
        at the class level.  otherwise we have to set attribute behavior on a per-instance level."""
        return self.inherits is None and self.parent._is_primary_mapper()
        
class MapperOption(object):
    """describes a modification to an OperationContext."""
    def process_context(self, context):
        pass

class OperationContext(object):
    """serves as a context during a query construction or instance loading operation.
    accepts MapperOption objects which may modify its state before proceeding."""
    def __init__(self, options):
        self.options = options
        self.attributes = {}
        for opt in options:
            opt.process_context(self)

class StrategizedOption(MapperOption):
    """a MapperOption that affects which LoaderStrategy will be used for an operation
    by a StrategizedProperty."""
    def __init__(self, key):
        self.key = key
    def get_strategy_class(self):
        raise NotImplementedError()
    def process_context(self, context):
        context.attributes[(LoaderStrategy, self.key)] = self.get_strategy_class()
                
class StrategizedProperty(MapperProperty):
    def _get_strategy(self, context):
        cls = context.attributes.get((LoaderStrategy, self.key), self.strategy.__class__)
        try:
            return self._optional_strategies[cls]
        except KeyError:
            strategy = cls(self)
            strategy.init()
            self._optional_strategies[cls] = strategy
            return strategy
    def setup(self, querycontext, **kwargs):
        self._get_strategy(querycontext).setup_query(querycontext, **kwargs)
    def execute(self, selectcontext, instance, row, identitykey, isnew):
        self._get_strategy(selectcontext).process_row(selectcontext, instance, row, identitykey, isnew)
    def do_init(self):
        self._optional_strategies = {}
        self.strategy.init()
        self.strategy.init_class_attribute()

class LoaderStrategy(object):
    """describes the loading behavior of a StrategizedProperty object.  The LoaderStrategy
    interacts with the querying process in three ways:
      * it controls the configuration of the InstrumentedAttribute placed on a class to 
      handle the behavior of the attribute.  this may involve setting up class-level callable
      functions to fire off a select operation when the attribute is first accessed (i.e. a lazy load)
      * it processes the QueryContext at statement construction time, where it can modify the SQL statement
      that is being produced.  simple column attributes may add their represented column to the list of
      selected columns, "eager loading" properties may add LEFT OUTER JOIN clauses to the statement.
      * it processes the SelectionContext at row-processing time.  This may involve setting instance-level
      lazyloader functions on newly constructed instances, or may involve recursively appending child items 
      to a list in response to additionally eager-loaded objects in the query.
    """
    def __init__(self, parent):
        self.parent_property = parent
    def init(self):
        self.parent = self.parent_property.parent
        self.key = self.parent_property.key
    def init_class_attribute(self):
        pass
    def setup_query(self, context, **kwargs):
        pass
    def process_row(self, selectcontext, instance, row, identitykey, isnew):
        pass

