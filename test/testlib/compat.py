import types

__all__ = '_function_named',


def _function_named(fn, newname):
    try:
        fn.__name__ = newname
    except:
        fn = types.FunctionType(fn.__code__, fn.__globals__, newname,
                          fn.__defaults__, fn.__closure__)
    return fn

