from sqlalchemy.util import ScopedRegistry

class SessionContext(object):
    """A simple wrapper for ScopedRegistry that provides a "current" property
    which can be used to get, set, or remove the session in the current scope.

    By default this object provides thread-local scoping, which is the default
    scope provided by sqlalchemy.util.ScopedRegistry.

    Usage:
        engine = create_engine(...)
        def session_factory():
            return Session(bind_to=engine)
        context = SessionContext(session_factory)

        s = context.current # get thread-local session
        context.current = Session(bind_to=other_engine) # set current session
        del context.current # discard the thread-local session (a new one will
                            # be created on the next call to context.current)
    """
    def __init__(self, session_factory, scopefunc=None):
        self.registry = ScopedRegistry(session_factory, scopefunc)
        super(SessionContext, self).__init__()

    def get_current(self):
        return self.registry()
    def set_current(self, session):
        self.registry.set(session)
    def del_current(self):
        self.registry.clear()
    current = property(get_current, set_current, del_current,
        """Property used to get/set/del the session in the current scope""")

    def create_metaclass(session_context):
        """return a metaclass to be used by objects that wish to be bound to a
        thread-local session upon instantiatoin.

        Note non-standard use of session_context rather than self as the name
        of the first arguement of this method.

        Usage:
            context = SessionContext(...)
            class MyClass(object):
                __metaclass__ = context.metaclass
                ...
        """
        try:
            return session_context._metaclass
        except AttributeError:
            class metaclass(type):
                def __init__(cls, name, bases, dct):
                    old_init = getattr(cls, "__init__")
                    def __init__(self, *args, **kwargs):
                        session_context.current.save(self)
                        old_init(self, *args, **kwargs)
                    setattr(cls, "__init__", __init__)
                    super(metaclass, cls).__init__(name, bases, dct)
            session_context._metaclass = metaclass
            return metaclass
    metaclass = property(create_metaclass)

    def create_baseclass(session_context):
        """return a baseclass to be used by objects that wish to be bound to a
        thread-local session upon instantiatoin.

        Note non-standard use of session_context rather than self as the name
        of the first arguement of this method.

        Usage:
            context = SessionContext(...)
            class MyClass(context.baseclass):
                ...
        """
        try:
            return session_context._baseclass
        except AttributeError:
            class baseclass(object):
                def __init__(self, *args, **kwargs):
                    session_context.current.save(self)
                    super(baseclass, self).__init__(*args, **kwargs)
            session_context._baseclass = baseclass
            return baseclass
    baseclass = property(create_baseclass)


def test():

    def run_test(class_, context):
        obj = class_()
        assert context.current == get_session(obj)

        # keep a reference so the old session doesn't get gc'd
        old_session = context.current

        context.current = create_session()
        assert context.current != get_session(obj)
        assert old_session == get_session(obj)

        del context.current
        assert context.current != get_session(obj)
        assert old_session == get_session(obj)

        obj2 = class_()
        assert context.current == get_session(obj2)

    # test metaclass
    context = SessionContext(create_session)
    class MyClass(object): __metaclass__ = context.metaclass
    run_test(MyClass, context)

    # test baseclass
    context = SessionContext(create_session)
    class MyClass(context.baseclass): pass
    run_test(MyClass, context)

if __name__ == "__main__":
    test()
    print "All tests passed!"
