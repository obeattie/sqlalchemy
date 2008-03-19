from sqlalchemy.orm import attributes
class Foo(object):pass
attributes.register_class(Foo)
attributes.register_attribute(Foo, 'x', uselist=False, useobject=False)

# -----------------

f = Foo()
assert attributes.get_history(f._foostate, 'x') == ([], [], [])
f.x = 5
assert attributes.get_history(f._foostate, 'x') == ([5], [], [])
f._foostate.rollback()
assert attributes.get_history(f._foostate, 'x') == ([], [], [])


assert f.x is None, f.x
assert attributes.get_history(f._foostate, 'x') == ([], [None], []) # this is idiosyncratic of scalar attributes

# -----------------

f = Foo()
f.x = 5
f._foostate.commit_all()
assert attributes.get_history(f._foostate, 'x') == ([], [5], [])

f._foostate.rollback()
assert attributes.get_history(f._foostate, 'x') == ([], [5], []), attributes.get_history(f._foostate, 'x')

# -----------------

f = Foo()
f.x = 5
f._foostate.commit_all()
assert attributes.get_history(f._foostate, 'x') == ([], [5], [])

f.x = 9
assert attributes.get_history(f._foostate, 'x') == ([9], [], [5])
f._foostate.set_savepoint()
assert attributes.get_history(f._foostate, 'x') == ([9], [], [5])

f.x = 12
assert f._foostate.committed_state['x'] == 9
assert attributes.get_history(f._foostate, 'x') == ([12], [], [5])

f._foostate.rollback()
assert attributes.get_history(f._foostate, 'x') == ([9], [], [5]), attributes.get_history(f._foostate, 'x')

f._foostate.rollback()
assert attributes.get_history(f._foostate, 'x') == ([], [5], [])

# -----------------

f = Foo()
f.x = 5
f._foostate.commit_all()
assert attributes.get_history(f._foostate, 'x') == ([], [5], [])

f.x = 9
assert attributes.get_history(f._foostate, 'x') == ([9], [], [5])
f._foostate.set_savepoint()
assert attributes.get_history(f._foostate, 'x') == ([9], [], [5])

f.x = 12
assert attributes.get_history(f._foostate, 'x') == ([12], [], [5])

f._foostate.rollback()
assert attributes.get_history(f._foostate, 'x') == ([9], [], [5])

f._foostate.commit_all()
assert attributes.get_history(f._foostate, 'x') == ([], [9], [])

# -----------------
f = Foo()
f.x = 5
f._foostate.commit_all()
assert attributes.get_history(f._foostate, 'x') == ([], [5], [])

f.x = 9
assert attributes.get_history(f._foostate, 'x') == ([9], [], [5])
f._foostate.set_savepoint()
assert attributes.get_history(f._foostate, 'x') == ([9], [], [5])
assert f.x == 9

f.x = 12
assert attributes.get_history(f._foostate, 'x') == ([12], [], [5])

f._foostate.remove_savepoint()
assert not f._foostate.savepoints
f._foostate.rollback()
assert attributes.get_history(f._foostate, 'x') == ([], [5], []), attributes.get_history(f._foostate, 'x')
assert f.x == 5

# -----------------

f = Foo()
f.x = 5
f._foostate.commit_all()
assert attributes.get_history(f._foostate, 'x') == ([], [5], [])

f.x = 9
assert attributes.get_history(f._foostate, 'x') == ([9], [], [5])
f._foostate.set_savepoint()
assert attributes.get_history(f._foostate, 'x') == ([9], [], [5])

f.x = 12
assert attributes.get_history(f._foostate, 'x') == ([12], [], [5])

f._foostate.remove_savepoint()

f._foostate.commit_all()
assert attributes.get_history(f._foostate, 'x') == ([], [12], [])

# -----------------

f = Foo()
f.x = 5
f._foostate.commit_all()
assert attributes.get_history(f._foostate, 'x') == ([], [5], [])
f._foostate.set_savepoint()
assert attributes.get_history(f._foostate, 'x') == ([], [5], [])
f.x = 12
assert attributes.get_history(f._foostate, 'x') == ([12], [], [5]), attributes.get_history(f._foostate, 'x')
f._foostate.rollback()
assert attributes.get_history(f._foostate, 'x') == ([], [5], [])
assert f.x == 5

# -----------------

f = Foo()
assert attributes.get_history(f._foostate, 'x') == ([], [], [])
f._foostate.set_savepoint()
assert attributes.get_history(f._foostate, 'x') == ([], [], [])
f.x = 12
assert attributes.get_history(f._foostate, 'x') == ([12], [], [])
f._foostate.rollback()
