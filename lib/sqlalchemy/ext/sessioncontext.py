from sqlalchemy.util import ScopedRegistry
from sqlalchemy.orm.mapper import MapperExtension, EXT_PASS
from sqlalchemy.orm import create_session

__all__ = ['SessionContext', 'SessionContextExt']

class SessionContext(object):
    """A simple wrapper for ``ScopedRegistry`` that provides a
    `current` property which can be used to get, set, or remove the
    session in the current scope.

    By default this object provides thread-local scoping, which is the
    default scope provided by sqlalchemy.util.ScopedRegistry.

    Usage::

      engine = create_engine(...)
      def session_factory():
          return Session(bind_to=engine)
      context = SessionContext(session_factory)

      s = context.current # get thread-local session
      context.current = Session(bind_to=other_engine) # set current session
      del context.current # discard the thread-local session (a new one will
                          # be created on the next call to context.current)
    """

    def __init__(self, session_factory=None, scopefunc=None):
        if session_factory is None:
            session_factory = create_session
        self.registry = ScopedRegistry(session_factory, scopefunc)
        super(SessionContext, self).__init__()

    def get_current(self):
        return self.registry()

    def set_current(self, session):
        self.registry.set(session)

    def del_current(self):
        self.registry.clear()

    current = property(get_current, set_current, del_current,
                       """Property used to get/set/del the session in the current scope.""")

    def _get_mapper_extension(self):
        try:
            return self._extension
        except AttributeError:
            self._extension = ext = SessionContextExt(self)
            return ext

    mapper_extension = property(_get_mapper_extension,
                                doc="""Get a mapper extension that implements `get_session` using this context.""")


class SessionContextExt(MapperExtension):
    """A mapper extension that provides sessions to a mapper using ``SessionContext``."""

    def __init__(self, context):
        MapperExtension.__init__(self)
        self.context = context

    def get_session(self):
        return self.context.current

    def init_instance(self, mapper, class_, instance, args, kwargs):
        session = kwargs.pop('_sa_session', self.context.current)
        session._save_impl(instance, entity_name=kwargs.pop('_sa_entity_name', None))
        return EXT_PASS

    def init_failed(self, mapper, class_, instance, args, kwargs):
        object_session(instance).expunge(instance)
        return EXT_PASS
        
    def dispose_class(self, mapper, class_):
        if hasattr(class_, '__init__') and hasattr(class_.__init__, '_oldinit'):
            if class_.__init__._oldinit is not None:
                class_.__init__ = class_.__init__._oldinit
            else:
                delattr(class_, '__init__')
                
            