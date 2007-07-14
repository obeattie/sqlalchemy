"""
Introduction
============

SqlSoup provides a convenient way to access database tables without
having to declare table or mapper classes ahead of time.

Suppose we have a database with users, books, and loans tables
(corresponding to the PyWebOff dataset, if you're curious).  For
testing purposes, we'll create this db as follows::

    >>> from sqlalchemy import create_engine
    >>> e = create_engine('sqlite:///:memory:')
    >>> for sql in _testsql: e.execute(sql) #doctest: +ELLIPSIS
    <...

Creating a SqlSoup gateway is just like creating an SQLAlchemy
engine::

    >>> from sqlalchemy.ext.sqlsoup import SqlSoup
    >>> db = SqlSoup('sqlite:///:memory:')

or, you can re-use an existing metadata::

    >>> db = SqlSoup(MetaData(e))

You can optionally specify a schema within the database for your
SqlSoup::

    # >>> db.schema = myschemaname


Loading objects
===============

Loading objects is as easy as this::

    >>> users = db.users.select()
    >>> users.sort()
    >>> users
    [MappedUsers(name='Joe Student',email='student@example.edu',password='student',classname=None,admin=0), MappedUsers(name='Bhargan Basepair',email='basepair@example.edu',password='basepair',classname=None,admin=1)]

Of course, letting the database do the sort is better (".c" is short for ".columns")::

    >>> db.users.select(order_by=[db.users.c.name])
    [MappedUsers(name='Bhargan Basepair',email='basepair@example.edu',password='basepair',classname=None,admin=1), MappedUsers(name='Joe Student',email='student@example.edu',password='student',classname=None,admin=0)]

Field access is intuitive::

    >>> users[0].email
    u'student@example.edu'

Of course, you don't want to load all users very often.  Let's add a
WHERE clause.  Let's also switch the order_by to DESC while we're at
it::

    >>> from sqlalchemy import or_, and_, desc
    >>> where = or_(db.users.c.name=='Bhargan Basepair', db.users.c.email=='student@example.edu')
    >>> db.users.select(where, order_by=[desc(db.users.c.name)])
    [MappedUsers(name='Joe Student',email='student@example.edu',password='student',classname=None,admin=0), MappedUsers(name='Bhargan Basepair',email='basepair@example.edu',password='basepair',classname=None,admin=1)]

You can also use the select...by methods if you're querying on a
single column.  This allows using keyword arguments as column names::

    >>> db.users.selectone_by(name='Bhargan Basepair')
    MappedUsers(name='Bhargan Basepair',email='basepair@example.edu',password='basepair',classname=None,admin=1)

Since name is the primary key, this is equivalent to

    >>> db.users.get('Bhargan Basepair')
    MappedUsers(name='Bhargan Basepair',email='basepair@example.edu',password='basepair',classname=None,admin=1)


Select variants
---------------

All the SQLAlchemy Query select variants are available.  Here's a
quick summary of these methods:

- ``get(PK)``: load a single object identified by its primary key
  (either a scalar, or a tuple)

- ``select(Clause, **kwargs)``: perform a select restricted by the
  `Clause` argument; returns a list of objects.  The most common clause
  argument takes the form ``db.tablename.c.columname == value``.  The
  most common optional argument is `order_by`.

- ``select_by(**params)``: select methods ending with ``_by`` allow
  using bare column names (``columname=value``). This feels more
  natural to most Python programmers; the downside is you can't
  specify ``order_by`` or other select options.

- ``selectfirst``, ``selectfirst_by``: returns only the first object
  found; equivalent to ``select(...)[0]`` or ``select_by(...)[0]``,
  except None is returned if no rows are selected.

- ``selectone``, ``selectone_by``: like ``selectfirst`` or
  ``selectfirst_by``, but raises if less or more than one object is
  selected.

- ``count``, ``count_by``: returns an integer count of the rows
  selected.

See the SQLAlchemy documentation for details, `datamapping query`__
for general info and examples, `sql construction`__ for details on
constructing ``WHERE`` clauses.

__ http://www.sqlalchemy.org/docs/datamapping.myt#datamapping_query
__ http://www.sqlalchemy.org/docs/sqlconstruction.myt


Modifying objects
=================

Modifying objects is intuitive::

    >>> user = _
    >>> user.email = 'basepair+nospam@example.edu'
    >>> db.flush()

(SqlSoup leverages the sophisticated SQLAlchemy unit-of-work code, so
multiple updates to a single object will be turned into a single
``UPDATE`` statement when you flush.)

To finish covering the basics, let's insert a new loan, then delete
it::

    >>> book_id = db.books.selectfirst(db.books.c.title=='Regional Variation in Moss').id
    >>> db.loans.insert(book_id=book_id, user_name=user.name)
    MappedLoans(book_id=2,user_name='Bhargan Basepair',loan_date=None)
    >>> db.flush()

    >>> loan = db.loans.selectone_by(book_id=2, user_name='Bhargan Basepair')
    >>> db.delete(loan)
    >>> db.flush()

You can also delete rows that have not been loaded as objects. Let's
do our insert/delete cycle once more, this time using the loans
table's delete method. (For SQLAlchemy experts: note that no flush()
call is required since this delete acts at the SQL level, not at the
Mapper level.) The same where-clause construction rules apply here as
to the select methods.

::

    >>> db.loans.insert(book_id=book_id, user_name=user.name)
    MappedLoans(book_id=2,user_name='Bhargan Basepair',loan_date=None)
    >>> db.flush()
    >>> db.loans.delete(db.loans.c.book_id==2)

You can similarly update multiple rows at once. This will change the
book_id to 1 in all loans whose book_id is 2::

    >>> db.loans.update(db.loans.c.book_id==2, book_id=1)
    >>> db.loans.select_by(db.loans.c.book_id==1)
    [MappedLoans(book_id=1,user_name='Joe Student',loan_date=datetime.datetime(2006, 7, 12, 0, 0))]


Joins
=====

Occasionally, you will want to pull out a lot of data from related
tables all at once.  In this situation, it is far more efficient to
have the database perform the necessary join.  (Here we do not have *a
lot of data* but hopefully the concept is still clear.)  SQLAlchemy is
smart enough to recognize that loans has a foreign key to users, and
uses that as the join condition automatically.

::

    >>> join1 = db.join(db.users, db.loans, isouter=True)
    >>> join1.select_by(name='Joe Student')
    [MappedJoin(name='Joe Student',email='student@example.edu',password='student',classname=None,admin=0,book_id=1,user_name='Joe Student',loan_date=datetime.datetime(2006, 7, 12, 0, 0))]

If you're unfortunate enough to be using MySQL with the default MyISAM
storage engine, you'll have to specify the join condition manually,
since MyISAM does not store foreign keys.  Here's the same join again,
with the join condition explicitly specified::

    >>> db.join(db.users, db.loans, db.users.c.name==db.loans.c.user_name, isouter=True)
    <class 'sqlalchemy.ext.sqlsoup.MappedJoin'>

You can compose arbitrarily complex joins by combining Join objects
with tables or other joins.  Here we combine our first join with the
books table::

    >>> join2 = db.join(join1, db.books)
    >>> join2.select()
    [MappedJoin(name='Joe Student',email='student@example.edu',password='student',classname=None,admin=0,book_id=1,user_name='Joe Student',loan_date=datetime.datetime(2006, 7, 12, 0, 0),id=1,title='Mustards I Have Known',published_year='1989',authors='Jones')]

If you join tables that have an identical column name, wrap your join
with `with_labels`, to disambiguate columns with their table name::

    >>> db.with_labels(join1).c.keys()
    [u'users_name', u'users_email', u'users_password', u'users_classname', u'users_admin', u'loans_book_id', u'loans_user_name', u'loans_loan_date']

You can also join directly to a labeled object::

    >>> labeled_loans = db.with_labels(db.loans)
    >>> db.join(db.users, labeled_loans, isouter=True).c.keys()
    [u'name', u'email', u'password', u'classname', u'admin', u'loans_book_id', u'loans_user_name', u'loans_loan_date']


Advanced Use
============

Accessing the Session
---------------------

SqlSoup uses a SessionContext to provide thread-local sessions.  You
can get a reference to the current one like this::

    >>> from sqlalchemy.ext.sqlsoup import objectstore
    >>> session = objectstore.current

Now you have access to all the standard session-based SA features,
such as transactions.  (SqlSoup's ``flush()`` is normally
transactionalized, but you can perform manual transaction management
if you need a transaction to span multiple flushes.)


Mapping arbitrary Selectables
-----------------------------

SqlSoup can map any SQLAlchemy ``Selectable`` with the map
method. Let's map a ``Select`` object that uses an aggregate function;
we'll use the SQLAlchemy ``Table`` that SqlSoup introspected as the
basis. (Since we're not mapping to a simple table or join, we need to
tell SQLAlchemy how to find the *primary key* which just needs to be
unique within the select, and not necessarily correspond to a *real*
PK in the database.)

::

    >>> from sqlalchemy import select, func
    >>> b = db.books._table
    >>> s = select([b.c.published_year, func.count('*').label('n')], from_obj=[b], group_by=[b.c.published_year])
    >>> s = s.alias('years_with_count')
    >>> years_with_count = db.map(s, primary_key=[s.c.published_year])
    >>> years_with_count.select_by(published_year='1989')
    [MappedBooks(published_year='1989',n=1)]

Obviously if we just wanted to get a list of counts associated with
book years once, raw SQL is going to be less work. The advantage of
mapping a Select is reusability, both standalone and in Joins. (And if
you go to full SQLAlchemy, you can perform mappings like this directly
to your object models.)

An easy way to save mapped selectables like this is to just hang them on
your db object::

    >>> db.years_with_count = years_with_count

Python is flexible like that!


Raw SQL
-------

SqlSoup works fine with SQLAlchemy's `text block support`__.

__ http://www.sqlalchemy.org/docs/documentation.myt#sql_textual

You can also access the SqlSoup's `engine` attribute to compose SQL
directly.  The engine's ``execute`` method corresponds to the one of a
DBAPI cursor, and returns a ``ResultProxy`` that has ``fetch`` methods
you would also see on a cursor::

    >>> rp = db.bind.execute('select name, email from users order by name')
    >>> for name, email in rp.fetchall(): print name, email
    Bhargan Basepair basepair+nospam@example.edu
    Joe Student student@example.edu

You can also pass this engine object to other SQLAlchemy constructs.


Extra tests
===========

Boring tests here.  Nothing of real expository value.

::

    >>> db.users.select(db.users.c.classname==None, order_by=[db.users.c.name])
    [MappedUsers(name='Bhargan Basepair',email='basepair+nospam@example.edu',password='basepair',classname=None,admin=1), MappedUsers(name='Joe Student',email='student@example.edu',password='student',classname=None,admin=0)]

    >>> db.nopk
    Traceback (most recent call last):
    ...
    PKNotFoundError: table 'nopk' does not have a primary key defined [columns: i]

    >>> db.nosuchtable
    Traceback (most recent call last):
    ...
    NoSuchTableError: nosuchtable

    >>> years_with_count.insert(published_year='2007', n=1)
    Traceback (most recent call last):
    ...
    InvalidRequestError: SQLSoup can only modify mapped Tables (found: Alias)

    [tests clear()]
    >>> db.loans.count()
    1
    >>> _ = db.loans.insert(book_id=1, user_name='Bhargan Basepair')
    >>> db.clear()
    >>> db.flush()
    >>> db.loans.count()
    1
"""

from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.sessioncontext import SessionContext
from sqlalchemy.ext.assignmapper import assign_mapper
from sqlalchemy.exceptions import *


_testsql = """
CREATE TABLE books (
    id                   integer PRIMARY KEY, -- auto-increments in sqlite
    title                text NOT NULL,
    published_year       char(4) NOT NULL,
    authors              text NOT NULL
);

CREATE TABLE users (
    name                 varchar(32) PRIMARY KEY,
    email                varchar(128) NOT NULL,
    password             varchar(128) NOT NULL,
    classname            text,
    admin                int NOT NULL -- 0 = false
);

CREATE TABLE loans (
    book_id              int PRIMARY KEY REFERENCES books(id),
    user_name            varchar(32) references users(name)
        ON DELETE SET NULL ON UPDATE CASCADE,
    loan_date            datetime DEFAULT current_timestamp
);

insert into users(name, email, password, admin)
values('Bhargan Basepair', 'basepair@example.edu', 'basepair', 1);
insert into users(name, email, password, admin)
values('Joe Student', 'student@example.edu', 'student', 0);

insert into books(title, published_year, authors)
values('Mustards I Have Known', '1989', 'Jones');
insert into books(title, published_year, authors)
values('Regional Variation in Moss', '1971', 'Flim and Flam');

insert into loans(book_id, user_name, loan_date)
values (
    (select min(id) from books),
    (select name from users where name like 'Joe%'),
    '2006-07-12 0:0:0')
;

CREATE TABLE nopk (
    i                    int
);
""".split(';')

__all__ = ['PKNotFoundError', 'SqlSoup']

#
# thread local SessionContext
#
class Objectstore(SessionContext):
    def __getattr__(self, key):
        return getattr(self.current, key)
    def get_session(self):
        return self.current

objectstore = Objectstore(create_session)

class PKNotFoundError(SQLAlchemyError): pass

# metaclass is necessary to expose class methods with getattr, e.g.
# we want to pass db.users.select through to users._mapper.select
def _ddl_error(cls):
    msg = 'SQLSoup can only modify mapped Tables (found: %s)' \
          % cls._table.__class__.__name__
    raise InvalidRequestError(msg)

class SelectableClassType(type):
    def insert(cls, **kwargs):
        _ddl_error(cls)

    def delete(cls, *args, **kwargs):
        _ddl_error(cls)

    def update(cls, whereclause=None, values=None, **kwargs):
        _ddl_error(cls)

    def _selectable(cls):
        return cls._table

    def __getattr__(cls, attr):
        if attr == '_query':
            # called during mapper init
            raise AttributeError()
        return getattr(cls._query, attr)

class TableClassType(SelectableClassType):
    def insert(cls, **kwargs):
        o = cls()
        o.__dict__.update(kwargs)
        return o

    def delete(cls, *args, **kwargs):
        cls._table.delete(*args, **kwargs).execute()

    def update(cls, whereclause=None, values=None, **kwargs):
        cls._table.update(whereclause, values).execute(**kwargs)

def _is_outer_join(selectable):
    if not isinstance(selectable, sql.Join):
        return False
    if selectable.isouter:
        return True
    return _is_outer_join(selectable.left) or _is_outer_join(selectable.right)

def _selectable_name(selectable):
    if isinstance(selectable, sql.Alias):
        return _selectable_name(selectable.selectable)
    elif isinstance(selectable, sql.Select):
        return ''.join([_selectable_name(s) for s in selectable.froms])
    elif isinstance(selectable, schema.Table):
        return selectable.name.capitalize()
    else:
        x = selectable.__class__.__name__
        if x[0] == '_':
            x = x[1:]
        return x

def class_for_table(selectable, **mapper_kwargs):
    if not hasattr(selectable, '_selectable') \
    or selectable._selectable() != selectable:
        raise ArgumentError('class_for_table requires a selectable as its argument')
    mapname = 'Mapped' + _selectable_name(selectable)
    if isinstance(selectable, Table):
        klass = TableClassType(mapname, (object,), {})
    else:
        klass = SelectableClassType(mapname, (object,), {})

    def __cmp__(self, o):
        L = self.__class__.c.keys()
        L.sort()
        t1 = [getattr(self, k) for k in L]
        try:
            t2 = [getattr(o, k) for k in L]
        except AttributeError:
            raise TypeError('unable to compare with %s' % o.__class__)
        return cmp(t1, t2)

    def __repr__(self):
        import locale
        encoding = locale.getdefaultlocale()[1] or 'ascii'
        L = []
        for k in self.__class__.c.keys():
            value = getattr(self, k, '')
            if isinstance(value, unicode):
                value = value.encode(encoding)
            L.append("%s=%r" % (k, value))
        return '%s(%s)' % (self.__class__.__name__, ','.join(L))

    for m in ['__cmp__', '__repr__']:
        setattr(klass, m, eval(m))
    klass._table = selectable
    mappr = mapper(klass,
                   selectable,
                   extension=objectstore.mapper_extension,
                   allow_null_pks=_is_outer_join(selectable),
                   **mapper_kwargs)
    klass._query = Query(mappr)
    return klass

class SqlSoup:
    def __init__(self, *args, **kwargs):
        """Initialize a new ``SqlSoup``.

        `args` may either be an ``SQLEngine`` or a set of arguments
        suitable for passing to ``create_engine``.
        """

        # meh, sometimes having method overloading instead of kwargs would be easier
        if isinstance(args[0], MetaData):
            args = list(args)
            metadata = args.pop(0)
            if args or kwargs:
                raise ArgumentError('Extra arguments not allowed when metadata is given')
        else:
            metadata = MetaData(*args, **kwargs)
        self._metadata = metadata
        self._cache = {}
        self.schema = None

    def engine(self):
        return self._metadata.bind

    engine = property(engine)
    bind = engine

    def delete(self, *args, **kwargs):
        objectstore.delete(*args, **kwargs)

    def flush(self):
        objectstore.get_session().flush()

    def clear(self):
        objectstore.clear()

    def map(self, selectable, **kwargs):
        try:
            t = self._cache[selectable]
        except KeyError:
            t = class_for_table(selectable, **kwargs)
            self._cache[selectable] = t
        return t

    def with_labels(self, item):
        # TODO give meaningful aliases
        return self.map(item._selectable().select(use_labels=True).alias('foo'))

    def join(self, *args, **kwargs):
        j = join(*args, **kwargs)
        return self.map(j)

    def __getattr__(self, attr):
        try:
            t = self._cache[attr]
        except KeyError:
            table = Table(attr, self._metadata, autoload=True, schema=self.schema)
            if not table.primary_key.columns:
                raise PKNotFoundError('table %r does not have a primary key defined [columns: %s]' % (attr, ','.join(table.c.keys())))
            if table.columns:
                t = class_for_table(table)
            else:
                t = None
            self._cache[attr] = t
        return t

if __name__ == '__main__':
    import doctest
    doctest.testmod()
