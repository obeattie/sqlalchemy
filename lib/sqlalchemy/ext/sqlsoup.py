"""
Introduction
============

SqlSoup provides a convenient way to access database tables without having
to declare table or mapper classes ahead of time.

Suppose we have a database with users, books, and loans tables
(corresponding to the PyWebOff dataset, if you're curious).
For testing purposes, we'll create this db as follows:
    >>> from sqlalchemy import create_engine
    >>> e = create_engine('sqlite:///:memory:')
    >>> for sql in _testsql: e.execute(sql) #doctest: +ELLIPSIS
    <...

Creating a SqlSoup gateway is just like creating an SqlAlchemy engine:
    >>> from sqlalchemy.ext.sqlsoup import SqlSoup
    >>> db = SqlSoup('sqlite:///:memory:')

or, you can re-use an existing metadata:
    >>> db = SqlSoup(BoundMetaData(e))

You can optionally specify a schema within the database for your SqlSoup:
    # >>> db.schema = myschemaname


Loading objects
===============

Loading objects is as easy as this:
    >>> users = db.users.select()
    >>> users.sort()
    >>> users
    [MappedUsers(name='Joe Student',email='student@example.edu',password='student',classname=None,admin=0), MappedUsers(name='Bhargan Basepair',email='basepair@example.edu',password='basepair',classname=None,admin=1)]

Of course, letting the database do the sort is better (".c" is short for ".columns"):
    >>> db.users.select(order_by=[db.users.c.name])
    [MappedUsers(name='Bhargan Basepair',email='basepair@example.edu',password='basepair',classname=None,admin=1), MappedUsers(name='Joe Student',email='student@example.edu',password='student',classname=None,admin=0)]

Field access is intuitive:
    >>> users[0].email
    u'student@example.edu'

Of course, you don't want to load all users very often.  The common case is to
select by a key or other field:
    >>> db.users.selectone_by(name='Bhargan Basepair')
    MappedUsers(name='Bhargan Basepair',email='basepair@example.edu',password='basepair',classname=None,admin=1)

All the SqlAlchemy Query select variants (select, select_by, selectone, selectone_by, selectfirst, selectfirst_by)
are available.  See the SqlAlchemy documentation for details:
http://www.sqlalchemy.org/docs/datamapping.myt#datamapping_query for general info and examples,
and http://www.sqlalchemy.org/docs/sqlconstruction.myt for details on construction WHERE clauses.



Modifying objects
=================

Modifying objects is intuitive:
    >>> user = _
    >>> user.email = 'basepair+nospam@example.edu'
    >>> db.flush()

(SqlSoup leverages the sophisticated SqlAlchemy unit-of-work code, so
multiple updates to a single object will be turned into a single UPDATE
statement when you flush.)


To finish covering the basics, let's insert a new loan, then delete it:
    >>> book_id = db.books.selectfirst(db.books.c.title=='Regional Variation in Moss').id
    >>> db.loans.insert(book_id=book_id, user_name=user.name)
    MappedLoans(book_id=2,user_name='Bhargan Basepair',loan_date=None)
    >>> db.flush()

    >>> loan = db.loans.selectone_by(book_id=2, user_name='Bhargan Basepair')
    >>> db.delete(loan)
    >>> db.flush()

You can also delete rows that have not been loaded as objects.  Let's do our insert/delete cycle once more,
this time using the loans table's delete method.  (For SQLAlchemy experts:
note that no flush() call is required since this
delete acts at the SQL level, not at the Mapper level.)  The same where-clause construction rules
apply here as to the select methods.
    >>> db.loans.insert(book_id=book_id, user_name=user.name)
    MappedLoans(book_id=2,user_name='Bhargan Basepair',loan_date=None)
    >>> db.flush()
    >>> db.loans.delete(db.loans.c.book_id==2)

You can similarly update multiple rows at once.  This will change the book_id to 1 in all loans whose book_id is 2:

    >>> db.loans.update(db.loans.c.book_id==2, book_id=1)
    >>> db.loans.select_by(db.loans.c.book_id==1)
    [MappedLoans(book_id=1,user_name='Joe Student',loan_date=datetime.datetime(2006, 7, 12, 0, 0))]

	
Joins
=====

Occasionally, you will want to pull out a lot of data from related tables all at
once.  In this situation, it is far
more efficient to have the database perform the necessary join.  (Here
we do not have "a lot of data," but hopefully the concept is still clear.)
SQLAlchemy is smart enough to recognize that loans has a foreign key
to users, and uses that as the join condition automatically.
    >>> join1 = db.join(db.users, db.loans, isouter=True)
    >>> join1.select_by(name='Joe Student')
    [MappedJoin(name='Joe Student',email='student@example.edu',password='student',classname=None,admin=0,book_id=1,user_name='Joe Student',loan_date=datetime.datetime(2006, 7, 12, 0, 0))]

If you join tables that have an identical column name, pass use_lables to your
select:
    >>> db.with_labels(join1).select()
    [MappedUsersLoansJoin(users_name='Joe Student',users_email='student@example.edu',users_password='student',users_classname=None,users_admin=0,loans_book_id=1,loans_user_name='Joe Student',loans_loan_date=datetime.datetime(2006, 7, 12, 0, 0))]

You can compose arbitrarily complex joins by combining Join objects with
tables or other joins.
    >>> join2 = db.join(join1, db.books)
    >>> join2.select()
    [MappedJoin(name='Joe Student',email='student@example.edu',password='student',classname=None,admin=0,book_id=1,user_name='Joe Student',loan_date=datetime.datetime(2006, 7, 12, 0, 0),id=1,title='Mustards I Have Known',published_year='1989',authors='Jones')]


Advanced Use
============

You can access the SqlSoup's engine attribute to compose SQL directly.
The engine's <b>execute</b> method corresponds
to the one of a DBAPI cursor, and returns a ResultProxy that has <b>fetch</b> methods
you would also see on a cursor.

    >>> rp = db.engine.execute('select name, email from users order by name')
    >>> for name, email in rp.fetchall(): print name, email
    Bhargan Basepair basepair+nospam@example.edu
    Joe Student student@example.edu

You can also pass this engine object to other SQLAlchemy constructs.
"""

from sqlalchemy import *
from sqlalchemy.ext.sessioncontext import SessionContext
from sqlalchemy.ext.assignmapper import assign_mapper
from sqlalchemy.exceptions import *


_testsql = """
CREATE TABLE books (
    id                   integer PRIMARY KEY, -- auto-SERIAL in sqlite
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
    loan_date            date DEFAULT current_timestamp
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
""".split(';')

__all__ = ['NoSuchTableError', 'SqlSoup']

#
# thread local SessionContext
#
class Objectstore(SessionContext):
    def __getattr__(self, key):
        return getattr(self.current, key)
    def get_session(self):
        return self.current

objectstore = Objectstore(create_session)

class NoSuchTableError(SQLAlchemyError): pass

# metaclass is necessary to expose class methods with getattr, e.g.
# we want to pass db.users.select through to users._mapper.select
class TableClassType(type):
    def insert(cls, **kwargs):
        o = cls()
        o.__dict__.update(kwargs)
        return o
    def _selectable(cls):
        return cls._table
    def delete(cls, *args, **kwargs):
        cls._table.delete(*args, **kwargs).execute()
    def update(cls, whereclause=None, values=None, **kwargs):
        cls._table.update(whereclause, values).execute(**kwargs)
    def __getattr__(cls, attr):
        if attr == '_mapper':
            # called during mapper init
            raise AttributeError()
        return getattr(cls._mapper, attr)
            

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
        return ''.join([_selectable_name(s) for s in selectable._froms])
    elif isinstance(selectable, schema.Table):
        return selectable.name.capitalize()
    else:
        return selectable.__class__.__name__

def class_for_table(selectable):
    if not hasattr(selectable, '_selectable') \
    or selectable._selectable() != selectable:
        raise 'class_for_table requires a selectable as its argument'
    mapname = 'Mapped' + _selectable_name(selectable)
    klass = TableClassType(mapname, (object,), {})
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
        encoding = locale.getdefaultlocale()[1]
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
    klass._mapper = mapper(klass,
                           selectable,
                           extension=objectstore.mapper_extension,
                           allow_null_pks=_is_outer_join(selectable))
    return klass

class SqlSoup:
    def __init__(self, *args, **kwargs):
        """
        args may either be an SQLEngine or a set of arguments suitable
        for passing to create_engine
        """
        # meh, sometimes having method overloading instead of kwargs would be easier
        if isinstance(args[0], MetaData):
            args = list(args)
            metadata = args.pop(0)
            if args or kwargs:
                raise ArgumentError('Extra arguments not allowed when metadata is given')
        else:
            metadata = BoundMetaData(*args, **kwargs)
        self._metadata = metadata
        self._cache = {}
        self.schema = None
    def engine(self):
        return self._metadata._engine
    engine = property(engine)
    def delete(self, *args, **kwargs):
        objectstore.delete(*args, **kwargs)
    def flush(self):
        objectstore.get_session().flush()
    def rollback(self):
        objectstore.clear()
    def _reset(self):
        # for debugging
        self._cache = {}
        self.rollback()
    def _map(self, selectable):
        try:
            t = self._cache[selectable]
        except KeyError:
            t = class_for_table(selectable)
            self._cache[selectable] = t
        return t
    def with_labels(self, item):
        # TODO give meaningful aliases
        return self._map(item._selectable().select(use_labels=True).alias('foo'))
    def join(self, *args, **kwargs):
        j = join(*args, **kwargs)
        return self._map(j)
    def __getattr__(self, attr):
        try:
            t = self._cache[attr]
        except KeyError:
            table = Table(attr, self._metadata, autoload=True, schema=self.schema)
            if table.columns:
                t = class_for_table(table)
            else:
                t = None
            self._cache[attr] = t
        if not t:
            raise NoSuchTableError('%s does not exist' % attr)
        return t

if __name__ == '__main__':
    import doctest
    doctest.testmod()
