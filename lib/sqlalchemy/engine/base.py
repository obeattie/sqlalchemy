from sqlalchemy import exceptions, sql, schema, util, types, logging
import StringIO, sys, re


class ConnectionProvider(object):
    """Define an interface that returns raw Connection objects (or compatible)."""

    def get_connection(self):
        """Return a Connection or compatible object from a DBAPI which also contains a close() method.

        It is not defined what context this connection belongs to.  It
        may be newly connected, returned from a pool, part of some
        other kind of context such as thread-local, or can be a fixed
        member of this object.
        """

        raise NotImplementedError()

    def dispose(self):
        """Release all resources corresponding to this ConnectionProvider.

        This includes any underlying connection pools.
        """

        raise NotImplementedError()


class Dialect(sql.AbstractDialect):
    """Define the behavior of a specific database/DBAPI.

    Any aspect of metadata definition, SQL query generation, execution,
    result-set handling, or anything else which varies between
    databases is defined under the general category of the Dialect.
    The Dialect acts as a factory for other database-specific object
    implementations including ExecutionContext, Compiled,
    DefaultGenerator, and TypeEngine.

    All Dialects implement the following attributes:

    positional
      True if the paramstyle for this Dialect is positional

    paramstyle
      The paramstyle to be used (some DBAPIs support multiple paramstyles)

    supports_autoclose_results
      Usually True; if False, indicates that rows returned by
      fetchone() might not be just plain tuples, and may be
      "live" proxy objects which still require the cursor to be open
      in order to be read (such as pyPgSQL which has active
      filehandles for BLOBs).  In that case, an auto-closing
      ResultProxy cannot automatically close itself after results are
      consumed.

    convert_unicode
      True if unicode conversion should be applied to all str types

    encoding
      type of encoding to use for unicode, usually defaults to 'utf-8'
    """

    def create_connect_args(self, opts):
        """Build DBAPI compatible connection arguments.

        Given a dictionary of key-valued connect parameters, returns a
        tuple consisting of a `*args`/`**kwargs` suitable to send directly
        to the dbapi's connect function.  The connect args will have
        any number of the following keynames: host, hostname,
        database, dbname, user, username, password, pw, passwd,
        filename.
        """

        raise NotImplementedError()

    def convert_compiled_params(self, parameters):
        """Build DBAPI execute arguments from a ClauseParameters.

        Given a sql.ClauseParameters object, returns an array or
        dictionary suitable to pass directly to this Dialect's DBAPI's
        execute method.
        """

        raise NotImplementedError()

    def type_descriptor(self, typeobj):
        """Trasform the type from generic to database-specific.

        Provides a database-specific TypeEngine object, given the
        generic object which comes from the types module.  Subclasses
        will usually use the adapt_type() method in the types module
        to make this job easy.
        """

        raise NotImplementedError()

    def oid_column_name(self, column):
        """Return the oid column name for this dialect, or None if the dialect cant/wont support OID/ROWID.

        The Column instance which represents OID for the query being
        compiled is passed, so that the dialect can inspect the column
        and its parent selectable to determine if OID/ROWID is not
        selected for a particular selectable (i.e. oracle doesnt
        support ROWID for UNION, GROUP BY, DISTINCT, etc.)
        """

        raise NotImplementedError()

    def max_identifier_length(self):
        """Return the maximum length of identifier names.
        
        Return None if no limit."""
        return None

    def supports_unicode_statements(self):
        """indicate whether the DBAPI can receive SQL statements as Python unicode strings"""
        raise NotImplementedError()
        
    def supports_sane_rowcount(self):
        """Indicate whether the dialect properly implements statements rowcount.

        Provided to indicate when MySQL is being used, which does not
        have standard behavior for the "rowcount" function on a statement handle.
        """

        raise NotImplementedError()

    def schemagenerator(self, engine, proxy, **params):
        """Return a ``schema.SchemaVisitor`` instance that can generate schemas.

        `schemagenerator()` is called via the `create()` method on Table,
        Index, and others.
        """

        raise NotImplementedError()

    def schemadropper(self, engine, proxy, **params):
        """Return a ``schema.SchemaVisitor`` instance that can drop schemas.

        `schemadropper()` is called via the `drop()` method on Table,
        Index, and others.
        """

        raise NotImplementedError()

    def defaultrunner(self, engine, proxy, **params):
        """Return a ``schema.SchemaVisitor`` instance that can execute defaults."""

        raise NotImplementedError()

    def compiler(self, statement, parameters):
        """Return a ``sql.ClauseVisitor`` able to transform a ``ClauseElement`` into a string.

        The returned object is usually a subclass of
        ansisql.ANSICompiler, and will produce a string representation
        of the given ClauseElement and `parameters` dictionary.

        `compiler()` is called within the context of the compile() method.
        """

        raise NotImplementedError()

    def reflecttable(self, connection, table):
        """Load table description from the database.

        Given a ``Connection`` and a ``Table`` object, reflect its
        columns and properties from the database.
        """

        raise NotImplementedError()

    def has_table(self, connection, table_name, schema=None):
        """Check the existence of a particular table in the database.

        Given a ``Connection`` object and a `table_name`, return True
        if the given table (possibly within the specified `schema`)
        exists in the database, False otherwise.
        """

        raise NotImplementedError()

    def has_sequence(self, connection, sequence_name):
        """Check the existence of a particular sequence in the database.

        Given a ``Connection`` object and a `sequence_name`, return
        True if the given sequence exists in the database, False
        otherwise.
        """

        raise NotImplementedError()

    def dbapi(self):
        """Establish a connection to the database.

        Subclasses override this method to provide the DBAPI module
        used to establish connections.
        """

        raise NotImplementedError()

    def get_default_schema_name(self, connection):
        """Return the currently selected schema given a connection"""

        raise NotImplementedError()

    def execution_context(self):
        """Return a new ExecutionContext object."""

        raise NotImplementedError()

    def do_begin(self, connection):
        """Provide an implementation of connection.begin()."""

        raise NotImplementedError()

    def do_rollback(self, connection):
        """Provide an implementation of connection.rollback()."""

        raise NotImplementedError()

    def do_commit(self, connection):
        """Provide an implementation of connection.commit()"""

        raise NotImplementedError()

    def do_executemany(self, cursor, statement, parameters):
        """Execute a single SQL statement looping over a sequence of parameters."""

        raise NotImplementedError()

    def do_execute(self, cursor, statement, parameters):
        """Execute a single SQL statement with given parameters."""

        raise NotImplementedError()

    def create_cursor(self, connection):
        """Return a new cursor generated from the given connection."""

        raise NotImplementedError()

    def create_result_proxy_args(self, connection, cursor):
        """Return a dictionary of arguments that should be passed to ResultProxy()."""

        raise NotImplementedError()

    def compile(self, clauseelement, parameters=None):
        """Compile the given ClauseElement using this Dialect.

        A convenience method which simply flips around the compile()
        call on ClauseElement.
        """

        return clauseelement.compile(dialect=self, parameters=parameters)


class ExecutionContext(object):
    """A messenger object for a Dialect that corresponds to a single execution.

    ExecutionContext should have a datamember "cursor" which is created
    at initialization time.
    
    The Dialect should provide an ExecutionContext via the
    create_execution_context() method.  The `pre_exec` and `post_exec`
    methods will be called for compiled statements, afterwhich it is
    expected that the various methods `last_inserted_ids`,
    `last_inserted_params`, etc.  will contain appropriate values, if
    applicable.
    """

    def pre_exec(self):
        """Called before an execution of a compiled statement.

        `proxy` is a callable that takes a string statement and a bind
        parameter list/dictionary.
        """

        raise NotImplementedError()

    def post_exec(self):
        """Called after the execution of a compiled statement.

        `proxy` is a callable that takes a string statement and a bind
        parameter list/dictionary.
        """

        raise NotImplementedError()

    def get_result_proxy(self):
        """return a ResultProxy corresponding to this ExecutionContext."""
        raise NotImplementedError()
        
    def get_rowcount(self):
        """Return the count of rows updated/deleted for an UPDATE/DELETE statement."""

        raise NotImplementedError()

    def supports_sane_rowcount(self):
        """Indicate if the "rowcount" DBAPI cursor function works properly.

        Currently, MySQLDB does not properly implement this function.
        """

        raise NotImplementedError()

    def last_inserted_ids(self):
        """Return the list of the primary key values for the last insert statement executed.

        This does not apply to straight textual clauses; only to
        ``sql.Insert`` objects compiled against a ``schema.Table`` object,
        which are executed via `statement.execute()`.  The order of
        items in the list is the same as that of the Table's
        'primary_key' attribute.

        In some cases, this method may invoke a query back to the
        database to retrieve the data, based on the "lastrowid" value
        in the cursor.
        """

        raise NotImplementedError()

    def last_inserted_params(self):
        """Return a dictionary of the full parameter dictionary for the last compiled INSERT statement.

        Includes any ColumnDefaults or Sequences that were pre-executed.
        """

        raise NotImplementedError()

    def last_updated_params(self):
        """Return a dictionary of the full parameter dictionary for the last compiled UPDATE statement.

        Includes any ColumnDefaults that were pre-executed.
        """

        raise NotImplementedError()

    def lastrow_has_defaults(self):
        """Return True if the last row INSERTED via a compiled insert statement contained PassiveDefaults.

        The presence of PassiveDefaults indicates that the database
        inserted data beyond that which we passed to the query
        programmatically.
        """

        raise NotImplementedError()


class Connectable(object):
    """Interface for an object that can provide an Engine and a Connection object which correponds to that Engine."""

    def contextual_connect(self):
        """Return a Connection object which may be part of an ongoing context."""

        raise NotImplementedError()

    def create(self, entity, **kwargs):
        """Create a table or index given an appropriate schema object."""

        raise NotImplementedError()

    def drop(self, entity, **kwargs):
        """Drop a table or index given an appropriate schema object."""

        raise NotImplementedError()

    def execute(self, object, *multiparams, **params):
        raise NotImplementedError()

    def _not_impl(self):
        raise NotImplementedError()

    engine = property(_not_impl, doc="The Engine which this Connectable is associated with.")

class Connection(Connectable):
    """Represent a single DBAPI connection returned from the underlying connection pool.

    Provides execution support for string-based SQL statements as well
    as ClauseElement, Compiled and DefaultGenerator objects.  Provides
    a begin method to return Transaction objects.

    The Connection object is **not** threadsafe.
    """

    def __init__(self, engine, connection=None, close_with_result=False):
        self.__engine = engine
        self.__connection = connection or engine.raw_connection()
        self.__transaction = None
        self.__close_with_result = close_with_result

    def _get_connection(self):
        try:
            return self.__connection
        except AttributeError:
            raise exceptions.InvalidRequestError("This Connection is closed")

    engine = property(lambda s:s.__engine, doc="The Engine with which this Connection is associated (read only)")
    connection = property(_get_connection, doc="The underlying DBAPI connection managed by this Connection.")
    should_close_with_result = property(lambda s:s.__close_with_result, doc="Indicates if this Connection should be closed when a corresponding ResultProxy is closed; this is essentially an auto-release mode.")

    def _create_transaction(self, parent):
        return Transaction(self, parent)

    def connect(self):
        """connect() is implemented to return self so that an incoming Engine or Connection object can be treated similarly."""
        return self

    def contextual_connect(self, **kwargs):
        """contextual_connect() is implemented to return self so that an incoming Engine or Connection object can be treated similarly."""
        return self

    def begin(self):
        if self.__transaction is None:
            self.__transaction = self._create_transaction(None)
            return self.__transaction
        else:
            return self._create_transaction(self.__transaction)

    def in_transaction(self):
        return self.__transaction is not None

    def _begin_impl(self):
        self.__engine.logger.info("BEGIN")
        self.__engine.dialect.do_begin(self.connection)

    def _rollback_impl(self):
        self.__engine.logger.info("ROLLBACK")
        self.__engine.dialect.do_rollback(self.connection)
        self.__connection.close_open_cursors()
        self.__transaction = None

    def _commit_impl(self):
        self.__engine.logger.info("COMMIT")
        self.__engine.dialect.do_commit(self.connection)
        self.__transaction = None

    def _autocommit(self, statement):
        """When no Transaction is present, this is called after executions to provide "autocommit" behavior."""
        # TODO: have the dialect determine if autocommit can be set on the connection directly without this
        # extra step
        if not self.in_transaction() and re.match(r'UPDATE|INSERT|CREATE|DELETE|DROP|ALTER', statement.lstrip().upper()):
            self._commit_impl()

    def _autorollback(self):
        if not self.in_transaction():
            self._rollback_impl()

    def close(self):
        try:
            c = self.__connection
        except AttributeError:
            return
        self.__connection.close()
        self.__connection = None
        del self.__connection

    def scalar(self, object, *multiparams, **params):
        return self.execute(object, *multiparams, **params).scalar()

    def execute(self, object, *multiparams, **params):
        for c in type(object).__mro__:
            if c in Connection.executors:
                return Connection.executors[c](self, object, *multiparams, **params)
        else:
            raise exceptions.InvalidRequestError("Unexecuteable object type: " + str(type(object)))

    def execute_default(self, default, **kwargs):
        return default.accept_visitor(self.__engine.dialect.defaultrunner(self.__engine, self.proxy, **kwargs))

    def execute_text(self, statement, *multiparams, **params):
        if len(multiparams) == 0:
            parameters = params
        elif len(multiparams) == 1 and (isinstance(multiparams[0], list) or isinstance(multiparams[0], dict)):
            parameters = multiparams[0]
        else:
            parameters = list(multiparams)
        cursor = self._execute_raw(statement, parameters)
        rpargs = self.__engine.dialect.create_result_proxy_args(self, cursor)
        return ResultProxy(self.__engine, self, cursor, **rpargs)

    def _params_to_listofdicts(self, *multiparams, **params):
        if len(multiparams) == 0:
            return [params]
        elif len(multiparams) == 1:
            if multiparams[0] == None:
                return [{}]
            elif isinstance (multiparams[0], list) or isinstance (multiparams[0], tuple):
                return multiparams[0]
            else:
                return [multiparams[0]]
        else:
            return multiparams
    
    def execute_function(self, func, *multiparams, **params):
        return self.execute_clauseelement(func.select(), *multiparams, **params)
        
    def execute_clauseelement(self, elem, *multiparams, **params):
        executemany = len(multiparams) > 0
        if executemany:
            param = multiparams[0]
        else:
            param = params
        return self.execute_compiled(elem.compile(engine=self.__engine, parameters=param), *multiparams, **params)

    def execute_compiled(self, compiled, *multiparams, **params):
        """Execute a sql.Compiled object."""
        if not compiled.can_execute:
            raise exceptions.ArgumentError("Not an executeable clause: %s" % (str(compiled)))
        parameters = [compiled.construct_params(m) for m in self._params_to_listofdicts(*multiparams, **params)]
        if len(parameters) == 1:
            parameters = parameters[0]
        context = self.__engine.dialect.create_execution_context(compiled=compiled, parameters=parameters, connection=self, engine=self.__engine)
        context.pre_exec()
        self.execute_compiled_impl(compiled, parameters, context)
        context.post_exec()
        return context.get_result_proxy()
    
    def _execute_compiled_impl(self, compiled, parameters, context):
        self._execute_raw(unicode(compiled), self.__engine.dialect.convert_compiled_params(parameters), context=context)
            
    def _execute_raw(self, statement, parameters=None, context=None, **kwargs):
        if not self.__engine.dialect.supports_unicode_statements():
            # encode to ascii, with full error handling
            statement = statement.encode('ascii')
        if context is None:
            context = self.__engine.dialect.create_execution_context(statement=statement, parameters=parameters, connection=self, engine=self.__engine)
        self.__engine.logger.info(statement)
        self.__engine.logger.info(repr(parameters))
        if parameters is not None and isinstance(parameters, list) and len(parameters) > 0 and (isinstance(parameters[0], list) or isinstance(parameters[0], dict)):
            self._executemany(context.cursor, statement, parameters, context=context)
        else:
            self._execute(context.cursor, statement, parameters, context=context)
        self._autocommit(statement)
        return context.cursor

    def _execute(self, c, statement, parameters, context=None):
        if parameters is None:
            if self.__engine.dialect.positional:
                parameters = ()
            else:
                parameters = {}
        try:
            self.__engine.dialect.do_execute(c, statement, parameters, context=context)
        except Exception, e:
            self._autorollback()
            #self._rollback_impl()
            if self.__close_with_result:
                self.close()
            raise exceptions.SQLError(statement, parameters, e)

    def _executemany(self, c, statement, parameters, context=None):
        try:
            self.__engine.dialect.do_executemany(c, statement, parameters, context=context)
        except Exception, e:
            self._autorollback()
            #self._rollback_impl()
            if self.__close_with_result:
                self.close()
            raise exceptions.SQLError(statement, parameters, e)




    # poor man's multimethod/generic function thingy
    executors = {
        sql._Function : execute_function,
        sql.ClauseElement : execute_clauseelement,
        sql.ClauseVisitor : execute_compiled,
        schema.SchemaItem:execute_default,
        str.__mro__[-2] : execute_text
    }

    def create(self, entity, **kwargs):
        """Create a table or index given an appropriate schema object."""

        return self.__engine.create(entity, connection=self, **kwargs)

    def drop(self, entity, **kwargs):
        """Drop a table or index given an appropriate schema object."""

        return self.__engine.drop(entity, connection=self, **kwargs)

    def reflecttable(self, table, **kwargs):
        """Reflect the columns in the given table from the database."""

        return self.__engine.reflecttable(table, connection=self, **kwargs)

    def default_schema_name(self):
        return self.__engine.dialect.get_default_schema_name(self)

    def run_callable(self, callable_):
        return callable_(self)


    def proxy(self, statement=None, parameters=None):
        """Execute the given statement string and parameter object.

        The parameter object is expected to be the result of a call to
        ``compiled.get_params()``.  This callable is a generic version
        of a connection/cursor-specific callable that is produced
        within the execute_compiled method, and is used for objects
        that require this style of proxy when outside of an
        execute_compiled method, primarily the DefaultRunner.
        """
        parameters = self.__engine.dialect.convert_compiled_params(parameters)
        return self._execute_raw(statement, parameters)

class Transaction(object):
    """Represent a Transaction in progress.

    The Transaction object is **not** threadsafe.
    """

    def __init__(self, connection, parent):
        self.__connection = connection
        self.__parent = parent or self
        self.__is_active = True
        if self.__parent is self:
            self.__connection._begin_impl()

    connection = property(lambda s:s.__connection, doc="The Connection object referenced by this Transaction")
    is_active = property(lambda s:s.__is_active)

    def rollback(self):
        if not self.__parent.__is_active:
            return
        if self.__parent is self:
            self.__connection._rollback_impl()
            self.__is_active = False
        else:
            self.__parent.rollback()

    def commit(self):
        if not self.__parent.__is_active:
            raise exceptions.InvalidRequestError("This transaction is inactive")
        if self.__parent is self:
            self.__connection._commit_impl()
            self.__is_active = False

class Engine(sql.Executor, Connectable):
    """
    Connects a ConnectionProvider, a Dialect and a CompilerFactory together to
    provide a default implementation of SchemaEngine.
    """

    def __init__(self, connection_provider, dialect, echo=None):
        self.connection_provider = connection_provider
        self.dialect=dialect
        self.echo = echo
        self.logger = logging.instance_logger(self)

    name = property(lambda s:sys.modules[s.dialect.__module__].descriptor()['name'])
    engine = property(lambda s:s)
    echo = logging.echo_property()

    def dispose(self):
        self.connection_provider.dispose()

    def create(self, entity, connection=None, **kwargs):
        """Create a table or index within this engine's database connection given a schema.Table object."""

        self._run_visitor(self.dialect.schemagenerator, entity, connection=connection, **kwargs)

    def drop(self, entity, connection=None, **kwargs):
        """Drop a table or index within this engine's database connection given a schema.Table object."""

        self._run_visitor(self.dialect.schemadropper, entity, connection=connection, **kwargs)

    def execute_default(self, default, **kwargs):
        connection = self.contextual_connect()
        try:
            return connection.execute_default(default, **kwargs)
        finally:
            connection.close()

    def _func(self):
        return sql._FunctionGenerator(self)

    func = property(_func)

    def text(self, text, *args, **kwargs):
        """Return a sql.text() object for performing literal queries."""

        return sql.text(text, engine=self, *args, **kwargs)

    def _run_visitor(self, visitorcallable, element, connection=None, **kwargs):
        if connection is None:
            conn = self.contextual_connect()
        else:
            conn = connection
        try:
            element.accept_visitor(visitorcallable(self, conn.proxy, connection=conn, **kwargs))
        finally:
            if connection is None:
                conn.close()

    def transaction(self, callable_, connection=None, *args, **kwargs):
        """Execute the given function within a transaction boundary.

        This is a shortcut for explicitly calling `begin()` and `commit()`
        and optionally `rollback()` when exceptions are raised.  The
        given `*args` and `**kwargs` will be passed to the function, as
        well as the Connection used in the transaction.
        """

        if connection is None:
            conn = self.contextual_connect()
        else:
            conn = connection
        try:
            trans = conn.begin()
            try:
                ret = callable_(conn, *args, **kwargs)
                trans.commit()
                return ret
            except:
                trans.rollback()
                raise
        finally:
            if connection is None:
                conn.close()

    def run_callable(self, callable_, connection=None, *args, **kwargs):
        if connection is None:
            conn = self.contextual_connect()
        else:
            conn = connection
        try:
            return callable_(conn, *args, **kwargs)
        finally:
            if connection is None:
                conn.close()

    def execute(self, statement, *multiparams, **params):
        connection = self.contextual_connect(close_with_result=True)
        return connection.execute(statement, *multiparams, **params)

    def scalar(self, statement, *multiparams, **params):
        return self.execute(statement, *multiparams, **params).scalar()

    def execute_compiled(self, compiled, *multiparams, **params):
        connection = self.contextual_connect(close_with_result=True)
        return connection.execute_compiled(compiled, *multiparams, **params)

    def compiler(self, statement, parameters, **kwargs):
        return self.dialect.compiler(statement, parameters, engine=self, **kwargs)

    def connect(self, **kwargs):
        """Return a newly allocated Connection object."""

        return Connection(self, **kwargs)

    def contextual_connect(self, close_with_result=False, **kwargs):
        """Return a Connection object which may be newly allocated, or may be part of some ongoing context.

        This Connection is meant to be used by the various "auto-connecting" operations.
        """

        return Connection(self, close_with_result=close_with_result, **kwargs)

    def reflecttable(self, table, connection=None):
        """Given a Table object, reflects its columns and properties from the database."""

        if connection is None:
            conn = self.contextual_connect()
        else:
            conn = connection
        try:
            self.dialect.reflecttable(conn, table)
        finally:
            if connection is None:
                conn.close()

    def has_table(self, table_name, schema=None):
        return self.run_callable(lambda c: self.dialect.has_table(c, table_name, schema=schema))

    def raw_connection(self):
        """Return a DBAPI connection."""

        return self.connection_provider.get_connection()

    def log(self, msg):
        """Log a message using this SQLEngine's logger stream."""

        self.logger.info(msg)

class ResultProxy(object):
    """Wraps a DBAPI cursor object to provide easier access to row columns.

    Individual columns may be accessed by their integer position,
    case-insensitive column name, or by ``schema.Column``
    object. e.g.::

      row = fetchone()

      col1 = row[0]    # access via integer position

      col2 = row['col2']   # access via name

      col3 = row[mytable.c.mycol] # access via Column object.

    ResultProxy also contains a map of TypeEngine objects and will
    invoke the appropriate ``convert_result_value()`` method before
    returning columns, as well as the ExecutionContext corresponding
    to the statement execution.  It provides several methods for which
    to obtain information from the underlying ExecutionContext.
    """

    class AmbiguousColumn(object):
        def __init__(self, key):
            self.key = key
        def dialect_impl(self, dialect):
            return self
        def convert_result_value(self, arg, engine):
            raise exceptions.InvalidRequestError("Ambiguous column name '%s' in result set! try 'use_labels' option on select statement." % (self.key))

    def __new__(cls, *args, **kwargs):
        if cls is ResultProxy and kwargs.has_key('should_prefetch') and kwargs['should_prefetch']:
            return PrefetchingResultProxy(*args, **kwargs)
        else:
            return object.__new__(cls, *args, **kwargs)

    def __init__(self, engine, connection, cursor, executioncontext=None, typemap=None, column_labels=None, should_prefetch=None):
        """ResultProxy objects are constructed via the execute() method on SQLEngine."""

        self.connection = connection
        self.dialect = engine.dialect
        self.cursor = cursor
        self.engine = engine
        self.closed = False
        self.column_labels = column_labels
        if executioncontext is not None:
            self.__executioncontext = executioncontext
            self.rowcount = executioncontext.get_rowcount(cursor)
        else:
            self.rowcount = cursor.rowcount
        self.__key_cache = {}
        self.__echo = engine.echo == 'debug'
        metadata = cursor.description
        self.props = {}
        self.keys = []
        i = 0
        
        if metadata is not None:
            for item in metadata:
                # sqlite possibly prepending table name to colnames so strip
                colname = item[0].split('.')[-1].lower()
                if typemap is not None:
                    rec = (typemap.get(colname, types.NULLTYPE), i)
                else:
                    rec = (types.NULLTYPE, i)
                if rec[0] is None:
                    raise DBAPIError("None for metadata " + colname)
                if self.props.setdefault(colname, rec) is not rec:
                    self.props[colname] = (ResultProxy.AmbiguousColumn(colname), 0)
                self.keys.append(colname)
                self.props[i] = rec
                i+=1

    def _executioncontext(self):
        try:
            return self.__executioncontext
        except AttributeError:
            raise exceptions.InvalidRequestError("This ResultProxy does not have an execution context with which to complete this operation.  Execution contexts are not generated for literal SQL execution.")
    executioncontext = property(_executioncontext)

    def close(self):
        """Close this ResultProxy, and the underlying DBAPI cursor corresponding to the execution.

        If this ResultProxy was generated from an implicit execution,
        the underlying Connection will also be closed (returns the
        underlying DBAPI connection to the connection pool.)

        This method is also called automatically when all result rows
        are exhausted.
        """

        if not self.closed:
            self.closed = True
            self.cursor.close()
            if self.connection.should_close_with_result and self.dialect.supports_autoclose_results:
                self.connection.close()

    def _convert_key(self, key):
        """Convert and cache a key.

        Given a key, which could be a ColumnElement, string, etc.,
        matches it to the appropriate key we got from the result set's
        metadata; then cache it locally for quick re-access.
        """

        try:
            return self.__key_cache[key]
        except KeyError:
            if isinstance(key, int) and key in self.props:
                rec = self.props[key]
            elif isinstance(key, basestring) and key.lower() in self.props:
                rec = self.props[key.lower()]
            elif isinstance(key, sql.ColumnElement):
                label = self.column_labels.get(key._label, key.name).lower()
                if label in self.props:
                    rec = self.props[label]
                        
            if not "rec" in locals():
                raise exceptions.NoSuchColumnError("Could not locate column in row for column '%s'" % (repr(key)))

            self.__key_cache[key] = rec
            return rec
            

    def _has_key(self, row, key):
        try:
            self._convert_key(key)
            return True
        except KeyError:
            return False

    def _get_col(self, row, key):
        rec = self._convert_key(key)
        return rec[0].dialect_impl(self.dialect).convert_result_value(row[rec[1]], self.dialect)

    def __iter__(self):
        while True:
            row = self.fetchone()
            if row is None:
                raise StopIteration
            else:
                yield row

    def last_inserted_ids(self):
        """Return ``last_inserted_ids()`` from the underlying ExecutionContext.

        See ExecutionContext for details.
        """

        return self.executioncontext.last_inserted_ids()

    def last_updated_params(self):
        """Return ``last_updated_params()`` from the underlying ExecutionContext.

        See ExecutionContext for details.
        """

        return self.executioncontext.last_updated_params()

    def last_inserted_params(self):
        """Return ``last_inserted_params()`` from the underlying ExecutionContext.

        See ExecutionContext for details.
        """

        return self.executioncontext.last_inserted_params()

    def lastrow_has_defaults(self):
        """Return ``lastrow_has_defaults()`` from the underlying ExecutionContext.

        See ExecutionContext for details.
        """

        return self.executioncontext.lastrow_has_defaults()

    def supports_sane_rowcount(self):
        """Return ``supports_sane_rowcount()`` from the underlying ExecutionContext.

        See ExecutionContext for details.
        """

        return self.executioncontext.supports_sane_rowcount()

    def fetchall(self):
        """Fetch all rows, just like DBAPI ``cursor.fetchall()``."""

        l = []
        for row in self.cursor.fetchall():
            l.append(RowProxy(self, row))
        self.close()
        return l

    def fetchmany(self, size=None):
        """Fetch many rows, just like DBAPI ``cursor.fetchmany(size=cursor.arraysize)``."""

        if size is None:
            rows = self.cursor.fetchmany()
        else:
            rows = self.cursor.fetchmany(size)
        l = []
        for row in rows:
            l.append(RowProxy(self, row))
        if len(l) == 0:
            self.close()
        return l

    def fetchone(self):
        """Fetch one row, just like DBAPI ``cursor.fetchone()``."""

        row = self.cursor.fetchone()
        if row is not None:
            return RowProxy(self, row)
        else:
            self.close()
            return None

    def scalar(self):
        """Fetch the first column of the first row, and close the result set."""

        row = self.cursor.fetchone()
        try:
            if row is not None:
                return RowProxy(self, row)[0]
            else:
                return None
        finally:
            self.close()

class PrefetchingResultProxy(ResultProxy):
    """ResultProxy that loads all columns into memory each time fetchone() is
    called.  If fetchmany() or fetchall() are called, the full grid of results
    is fetched.
    """

    def _get_col(self, row, key):
        rec = self._convert_key(key)
        return row[rec[1]]

    def fetchall(self):
        l = []
        while True:
            row = self.fetchone()
            if row is not None:
                l.append(row)
            else:
                break
        return l

    def fetchmany(self, size=None):
        if size is None:
            return self.fetchall()
        l = []
        for i in xrange(size):
            row = self.fetchone()
            if row is not None:
                l.append(row)
            else:
                break
        return l

    def fetchone(self):
        sup = super(PrefetchingResultProxy, self)
        row = self.cursor.fetchone()
        if row is not None:
            row = [sup._get_col(row, i) for i in xrange(len(row))]
            return RowProxy(self, row)
        else:
            self.close()
            return None

class RowProxy(object):
    """Proxie a single cursor row for a parent ResultProxy.

    Mostly follows "ordered dictionary" behavior, mapping result
    values to the string-based column name, the integer position of
    the result in the row, as well as Column instances which can be
    mapped to the original Columns that produced this result set (for
    results that correspond to constructed SQL expressions).
    """

    def __init__(self, parent, row):
        """RowProxy objects are constructed by ResultProxy objects."""

        self.__parent = parent
        self.__row = row
        if self.__parent._ResultProxy__echo:
            self.__parent.engine.logger.debug("Row " + repr(row))

    def close(self):
        """Close the parent ResultProxy."""

        self.__parent.close()

    def __iter__(self):
        for i in range(0, len(self.__row)):
            yield self.__parent._get_col(self.__row, i)

    def __eq__(self, other):
        return (other is self) or (other == tuple([self.__parent._get_col(self.__row, key) for key in range(0, len(self.__row))]))

    def __repr__(self):
        return repr(tuple([self.__parent._get_col(self.__row, key) for key in range(0, len(self.__row))]))

    def has_key(self, key):
        """Return True if this RowProxy contains the given key."""

        return self.__parent._has_key(self.__row, key)

    def __getitem__(self, key):
        return self.__parent._get_col(self.__row, key)

    def __getattr__(self, name):
        try:
            return self.__parent._get_col(self.__row, name)
        except KeyError, e:
            raise AttributeError(e.args[0])

    def items(self):
        """Return a list of tuples, each tuple containing a key/value pair."""

        return [(key, getattr(self, key)) for key in self.keys()]

    def keys(self):
        """Return the list of keys as strings represented by this RowProxy."""

        return self.__parent.keys

    def values(self):
        """Return the values represented by this RowProxy as a list."""

        return list(self)

    def __len__(self):
        return len(self.__row)

class SchemaIterator(schema.SchemaVisitor):
    """A visitor that can gather text into a buffer and execute the contents of the buffer."""

    def __init__(self, engine, proxy, **params):
        """Construct a new SchemaIterator.

        engine
          the Engine used by this SchemaIterator

        proxy
          a callable which takes a statement and bind parameters and
          executes it, returning the cursor (the actual DBAPI cursor).
          The callable should use the same cursor repeatedly.
        """

        self.proxy = proxy
        self.engine = engine
        self.buffer = StringIO.StringIO()

    def append(self, s):
        """Append content to the SchemaIterator's query buffer."""

        self.buffer.write(s)

    def execute(self):
        """Execute the contents of the SchemaIterator's buffer."""

        try:
            return self.proxy(self.buffer.getvalue(), None)
        finally:
            self.buffer.truncate(0)

class DefaultRunner(schema.SchemaVisitor):
    """A visitor which accepts ColumnDefault objects, produces the
    dialect-specific SQL corresponding to their execution, and
    executes the SQL, returning the result value.

    DefaultRunners are used internally by Engines and Dialects.
    Specific database modules should provide their own subclasses of
    DefaultRunner to allow database-specific behavior.
    """

    def __init__(self, engine, proxy):
        self.proxy = proxy
        self.engine = engine

    def get_column_default(self, column):
        if column.default is not None:
            return column.default.accept_visitor(self)
        else:
            return None

    def get_column_onupdate(self, column):
        if column.onupdate is not None:
            return column.onupdate.accept_visitor(self)
        else:
            return None

    def visit_passive_default(self, default):
        """Do nothing.

        Passive defaults by definition return None on the app side,
        and are post-fetched to get the DB-side value.
        """

        return None

    def visit_sequence(self, seq):
        """Do nothing.

        Sequences are not supported by default.
        """

        return None

    def exec_default_sql(self, default):
        c = sql.select([default.arg], engine=self.engine).compile()
        return self.proxy(str(c), c.get_params()).fetchone()[0]

    def visit_column_onupdate(self, onupdate):
        if isinstance(onupdate.arg, sql.ClauseElement):
            return self.exec_default_sql(onupdate)
        elif callable(onupdate.arg):
            return onupdate.arg()
        else:
            return onupdate.arg

    def visit_column_default(self, default):
        if isinstance(default.arg, sql.ClauseElement):
            return self.exec_default_sql(default)
        elif callable(default.arg):
            return default.arg()
        else:
            return default.arg
