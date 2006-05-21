from sqlalchemy import exceptions, sql, schema, util, types
import StringIO, sys, re

class ConnectionProvider(object):
    """defines an interface that returns raw Connection objects (or compatible)."""
    def get_connection(self):
        """this method should return a Connection or compatible object from a DBAPI which
        also contains a close() method.  
        It is not defined what context this connection belongs to.  It may be newly connected, 
        returned from a pool, part of some other kind of context such as thread-local,
        or can be a fixed member of this object."""
        raise NotImplementedError()
    def dispose(self):
        """releases all resources corresponding to this ConnectionProvider, such 
        as any underlying connection pools."""
        raise NotImplementedError()

class Dialect(sql.AbstractDialect):
    """Adds behavior to the execution of queries to provide 
    support for column defaults, differences between paramstyles, quirks between post-execution behavior, 
    and a general consistentization of the behavior of various DBAPIs. 
    
    The Dialect should also implement the following two attributes:

    positional - True if the paramstyle for this Dialect is positional

    paramstyle - the paramstyle to be used (some DBAPIs support multiple paramstyles)

    supports_autoclose_results - usually True; if False, indicates that rows returned by fetchone()
    might not be just plain tuples, and may be "live" proxy objects which still require the cursor
    to be open in order to be read (such as pyPgSQL which has active filehandles for BLOBs).  in that
    case, an auto-closing ResultProxy cannot automatically close itself after results are consumed.

    convert_unicode - True if unicode conversion should be applied to all str types

    encoding - type of encoding to use for unicode, usually defaults to 'utf-8'
    """
    def create_connect_args(self, opts):
        """given a dictionary of key-valued connect parameters, returns a tuple 
        consisting of a *args/**kwargs suitable to send directly to the dbapi's connect function.
        The connect args will have any number of the following keynames:  host, hostname, database, dbanme,
        user,username, password, pw, passwd, filename."""
        raise NotImplementedError()
    def convert_compiled_params(self, parameters):
        """given a sql.ClauseParameters object, returns an array or dictionary suitable to pass 
        directly to this Dialect's DBAPI's execute method."""
    def type_descriptor(self, typeobj):
        """provides a database-specific TypeEngine object, given the generic object
        which comes from the types module.  Subclasses will usually use the adapt_type()
        method in the types module to make this job easy."""
        raise NotImplementedError()
    def oid_column_name(self):
        """returns the oid column name for this dialect, or None if the dialect cant/wont support OID/ROWID."""
        raise NotImplementedError()
    def supports_sane_rowcount(self):
        """Provided to indicate when MySQL is being used, which does not have standard behavior
        for the "rowcount" function on a statement handle.  """
        raise NotImplementedError()
    def schemagenerator(self, engine, proxy, **params):
        """returns a schema.SchemaVisitor instance that can generate schemas, when it is
        invoked to traverse a set of schema objects. 

        schemagenerator is called via the create() method on Table, Index, and others.
        """
        raise NotImplementedError()
    def schemadropper(self, engine, proxy, **params):
        """returns a schema.SchemaVisitor instance that can drop schemas, when it is
        invoked to traverse a set of schema objects. 

        schemagenerator is called via the drop() method on Table, Index, and others.
        """
        raise NotImplementedError()
    def defaultrunner(self, engine, proxy, **params):
        """returns a schema.SchemaVisitor instances that can execute defaults."""
        raise NotImplementedError()
    def compiler(self, statement, parameters):
        """returns a sql.ClauseVisitor which will produce a string representation of the given
        ClauseElement and parameter dictionary.  This object is usually a subclass of 
        ansisql.ANSICompiler.  

        compiler is called within the context of the compile() method."""
        raise NotImplementedError()
    def reflecttable(self, connection, table):
        """given an Connection and a Table object, reflects its columns and properties from the database."""
        raise NotImplementedError()
    def has_table(self, connection, table_name):
        raise NotImplementedError()
    def dbapi(self):
        """subclasses override this method to provide the DBAPI module used to establish
        connections."""
        raise NotImplementedError()
    def get_default_schema_name(self, connection):
        """returns the currently selected schema given an connection"""
        raise NotImplementedError()
    def execution_context(self):
        """returns a new ExecutionContext object."""
        raise NotImplementedError()
    def do_begin(self, connection):
        """provides an implementation of connection.begin()"""
        raise NotImplementedError()
    def do_rollback(self, connection):
        """provides an implementation of connection.rollback()"""
        raise NotImplementedError()
    def do_commit(self, connection):
        """provides an implementation of connection.commit()"""
        raise NotImplementedError()
    def do_executemany(self, cursor, statement, parameters):
        raise NotImplementedError()
    def do_execute(self, cursor, statement, parameters):
        raise NotImplementedError()
        
class ExecutionContext(object):
    """a messenger object for a Dialect that corresponds to a single execution.  The Dialect
    should provide an ExecutionContext via the create_execution_context() method.  
    The pre_exec and post_exec methods will be called for compiled statements, afterwhich
    it is expected that the various methods last_inserted_ids, last_inserted_params, etc.
    will contain appropriate values, if applicable."""
    def pre_exec(self, engine, proxy, compiled, parameters):
        """called before an execution of a compiled statement.  proxy is a callable that
        takes a string statement and a bind parameter list/dictionary."""
        raise NotImplementedError()
    def post_exec(self, engine, proxy, compiled, parameters):
        """called after the execution of a compiled statement.  proxy is a callable that
        takes a string statement and a bind parameter list/dictionary."""
        raise NotImplementedError()
    def get_rowcount(self, cursor):
        """returns the count of rows updated/deleted for an UPDATE/DELETE statement"""
        raise NotImplementedError()
    def supports_sane_rowcount(self):
        """Provided to indicate when MySQL is being used, which does not have standard behavior
        for the "rowcount" function on a statement handle.  """
        raise NotImplementedError()
    def last_inserted_ids(self):
        """returns the list of the primary key values for the last insert statement executed.
        This does not apply to straight textual clauses; only to sql.Insert objects compiled against 
        a schema.Table object, which are executed via statement.execute().  The order of items in the 
        list is the same as that of the Table's 'primary_key' attribute.
        
        In some cases, this method may invoke a query back to the database to retrieve the data, based on
        the "lastrowid" value in the cursor."""
        raise NotImplementedError()
    def last_inserted_params(self):
        """returns a dictionary of the full parameter dictionary for the last compiled INSERT statement,
        including any ColumnDefaults or Sequences that were pre-executed.  this value is thread-local."""
        raise NotImplementedError()
    def last_updated_params(self):
        """returns a dictionary of the full parameter dictionary for the last compiled UPDATE statement,
        including any ColumnDefaults that were pre-executed. this value is thread-local."""
        raise NotImplementedError()
    def lastrow_has_defaults(self):
        """returns True if the last row INSERTED via a compiled insert statement contained PassiveDefaults,
        indicating that the database inserted data beyond that which we gave it. this value is thread-local."""
        raise NotImplementedError()

class Connectable(object):
    """interface for an object that can provide an Engine and a Connection object which correponds to that Engine."""
    def contextual_connect(self):
        """returns a Connection object which may be part of an ongoing context."""
        raise NotImplementedError()
    def create(self, entity, **kwargs):
        """creates a table or index given an appropriate schema object."""
        raise NotImplementedError()
    def drop(self, entity, **kwargs):
        raise NotImplementedError()
    def execute(self, object, *multiparams, **params):
        raise NotImplementedError()
    def _not_impl(self):
        raise NotImplementedError()
    engine = property(_not_impl, doc="returns the Engine which this Connectable is associated with.")
            
class Connection(Connectable):
    """represents a single DBAPI connection returned from the underlying connection pool.  Provides
    execution support for string-based SQL statements as well as ClauseElement, Compiled and DefaultGenerator objects.
    provides a begin method to return Transaction objects."""
    def __init__(self, engine, connection=None, close_with_result=False):
        self.__engine = engine
        self.__connection = connection or engine.raw_connection()
        self.__transaction = None
        self.__close_with_result = close_with_result
    engine = property(lambda s:s.__engine, doc="The Engine with which this Connection is associated (read only)")
    connection = property(lambda s:s.__connection, doc="The underlying DBAPI connection managed by this Connection.")
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
    def _begin_impl(self):
        if self.__engine.echo:
            self.__engine.log("BEGIN")
        self.__engine.dialect.do_begin(self.__connection)
    def _rollback_impl(self):
        if self.__engine.echo:
            self.__engine.log("ROLLBACK")
        self.__engine.dialect.do_rollback(self.__connection)
    def _commit_impl(self):
        if self.__engine.echo:
            self.__engine.log("COMMIT")
        self.__engine.dialect.do_commit(self.__connection)
    def _autocommit(self, statement):
        """when no Transaction is present, this is called after executions to provide "autocommit" behavior."""
        # TODO: have the dialect determine if autocommit can be set on the connection directly without this 
        # extra step
        if self.__transaction is None and re.match(r'UPDATE|INSERT|CREATE|DELETE|DROP', statement.lstrip().upper()):
            self._commit_impl()
    def close(self):
        if self.__connection is not None:
            self.__connection.close()
            self.__connection = None
    def scalar(self, object, parameters, **kwargs):
        row = self.execute(object, parameters, **kwargs).fetchone()
        if row is not None:
            return row[0]
        else:
            return None
    def execute(self, object, *multiparams, **params):
        return Connection.executors[type(object).__mro__[-2]](self, object, *multiparams, **params)
    def execute_default(self, default, **kwargs):
        return default.accept_schema_visitor(self.__engine.dialect.defaultrunner(self.__engine, self.proxy, **kwargs))
    def execute_text(self, statement, parameters=None):
        cursor = self._execute_raw(statement, parameters)
        return ResultProxy(self.__engine, self, cursor)
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
    def execute_clauseelement(self, elem, *multiparams, **params):
        executemany = len(multiparams) > 0
        if executemany:
            param = multiparams[0]
        else:
            param = params
        return self.execute_compiled(elem.compile(engine=self.__engine, parameters=param), *multiparams, **params)
    def execute_compiled(self, compiled, *multiparams, **params):
        """executes a sql.Compiled object."""
        cursor = self.__connection.cursor()
        parameters = [compiled.get_params(**m) for m in self._params_to_listofdicts(*multiparams, **params)]
        if len(parameters) == 1:
            parameters = parameters[0]
        def proxy(statement=None, parameters=None):
            if statement is None:
                return cursor

            parameters = self.__engine.dialect.convert_compiled_params(parameters)
            self._execute_raw(statement, parameters, cursor=cursor, context=context)
            return cursor
        context = self.__engine.dialect.create_execution_context()
        context.pre_exec(self.__engine, proxy, compiled, parameters)
        proxy(str(compiled), parameters)
        context.post_exec(self.__engine, proxy, compiled, parameters)
        return ResultProxy(self.__engine, self, cursor, context, typemap=compiled.typemap)
        
    # poor man's multimethod/generic function thingy
    executors = {
        sql.ClauseElement : execute_clauseelement,
        sql.Compiled : execute_compiled,
        schema.SchemaItem:execute_default,
        str.__mro__[-2] : execute_text
    }
    
    def create(self, entity, **kwargs):
        """creates a table or index given an appropriate schema object."""
        return self.__engine.create(entity, connection=self, **kwargs)
    def drop(self, entity, **kwargs):
        """drops a table or index given an appropriate schema object."""
        return self.__engine.drop(entity, connection=self, **kwargs)
    def reflecttable(self, table, **kwargs):
        """reflects the columns in the given table from the database."""
        return self.__engine.reflecttable(table, connection=self, **kwargs)
    def default_schema_name(self):
        return self.__engine.dialect.get_default_schema_name(self)
    def run_callable(self, callable_):
        callable_(self)
    def _execute_raw(self, statement, parameters=None, cursor=None, echo=None, context=None, **kwargs):
        if cursor is None:
            cursor = self.__connection.cursor()
        try:
            if echo is True or self.__engine.echo is not False:
                self.__engine.log(statement)
                self.__engine.log(repr(parameters))
            if parameters is not None and isinstance(parameters, list) and len(parameters) > 0 and (isinstance(parameters[0], list) or isinstance(parameters[0], dict)):
                self._executemany(cursor, statement, parameters, context=context)
            else:
                self._execute(cursor, statement, parameters, context=context)
            self._autocommit(statement)
        except:
            raise
        return cursor

    def _execute(self, c, statement, parameters, context=None):
        if parameters is None:
            if self.__engine.dialect.positional:
                parameters = ()
            else:
                parameters = {}
        try:
            self.__engine.dialect.do_execute(c, statement, parameters, context=context)
        except Exception, e:
            self._rollback_impl()
            if self.__close_with_result:
                self.close()
            raise exceptions.SQLError(statement, parameters, e)
    def _executemany(self, c, statement, parameters, context=None):
        try:
            self.__engine.dialect.do_executemany(c, statement, parameters, context=context)
        except Exception, e:
            self._rollback_impl()
            if self.__close_with_result:
                self.close()
            raise exceptions.SQLError(statement, parameters, e)
    def proxy(self, statement=None, parameters=None):
        """executes the given statement string and parameter object.
        the parameter object is expected to be the result of a call to compiled.get_params().
        This callable is a generic version of a connection/cursor-specific callable that
        is produced within the execute_compiled method, and is used for objects that require
        this style of proxy when outside of an execute_compiled method, primarily the DefaultRunner."""
        parameters = self.__engine.dialect.convert_compiled_params(parameters)
        return self._execute_raw(statement, parameters)

class Transaction(object):
    """represents a Transaction in progress"""
    def __init__(self, connection, parent):
        self.__connection = connection
        self.__parent = parent or self
        self.__is_active = True
        if self.__parent is self:
            self.__connection._begin_impl()
    connection = property(lambda s:s.__connection, doc="The Connection object referenced by this Transaction")
    def rollback(self):
        if not self.__parent.__is_active:
            raise exceptions.InvalidRequestError("This transaction is inactive")
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
        
class ComposedSQLEngine(sql.Engine, Connectable):
    """
    Connects a ConnectionProvider, a Dialect and a CompilerFactory together to 
    provide a default implementation of SchemaEngine.
    """
    def __init__(self, connection_provider, dialect, echo=False, logger=None, **kwargs):
        self.connection_provider = connection_provider
        self.dialect=dialect
        self.echo = echo
        self.logger = logger or util.Logger(origin='engine')

    name = property(lambda s:sys.modules[s.dialect.__module__].descriptor()['name'])
    engine = property(lambda s:s)

    def dispose(self):
        self.connection_provider.dispose()
    def create(self, entity, connection=None, **kwargs):
        """creates a table or index within this engine's database connection given a schema.Table object."""
        self._run_visitor(self.dialect.schemagenerator, entity, connection=connection, **kwargs)
    def drop(self, entity, connection=None, **kwargs):
        """drops a table or index within this engine's database connection given a schema.Table object."""
        self._run_visitor(self.dialect.schemadropper, entity, connection=connection, **kwargs)
    def execute_default(self, default, **kwargs):
        connection = self.contextual_connect()
        try:
            return connection.execute_default(default, **kwargs)
        finally:
            connection.close()
    
    def _func(self):
        return sql.FunctionGenerator(self)
    func = property(_func)
    def text(self, text, *args, **kwargs):
        """returns a sql.text() object for performing literal queries."""
        return sql.text(text, engine=self, *args, **kwargs)

    def _run_visitor(self, visitorcallable, element, connection=None, **kwargs):
        if connection is None:
            conn = self.contextual_connect()
        else:
            conn = connection
        try:
            element.accept_schema_visitor(visitorcallable(self, conn.proxy, **kwargs))
        finally:
            if connection is None:
                conn.close()
    
    def transaction(self, callable_, connection=None, *args, **kwargs):
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
        
    def execute_compiled(self, compiled, *multiparams, **params):
        connection = self.contextual_connect(close_with_result=True)
        return connection.execute_compiled(compiled, *multiparams, **params)
        
    def compiler(self, statement, parameters, **kwargs):
        return self.dialect.compiler(statement, parameters, engine=self, **kwargs)

    def connect(self, **kwargs):
        """returns a newly allocated Connection object."""
        return Connection(self, **kwargs)
    
    def contextual_connect(self, close_with_result=False, **kwargs):
        """returns a Connection object which may be newly allocated, or may be part of some 
        ongoing context.  This Connection is meant to be used by the various "auto-connecting" operations."""
        return Connection(self, close_with_result=close_with_result, **kwargs)
            
    def reflecttable(self, table, connection=None):
        """given a Table object, reflects its columns and properties from the database."""
        if connection is None:
            conn = self.contextual_connect()
        else:
            conn = connection
        try:
            self.dialect.reflecttable(conn, table)
        finally:
            if connection is None:
                conn.close()
    def has_table(self, table_name):
        return self.run_callable(lambda c: self.dialect.has_table(c, table_name))
        
    def raw_connection(self):
        """returns a DBAPI connection."""
        return self.connection_provider.get_connection()

    def log(self, msg):
        """logs a message using this SQLEngine's logger stream."""
        self.logger.write(msg)

class ResultProxy:
    """wraps a DBAPI cursor object to provide access to row columns based on integer
    position, case-insensitive column name, or by schema.Column object. e.g.:
    
    row = fetchone()

    col1 = row[0]    # access via integer position

    col2 = row['col2']   # access via name

    col3 = row[mytable.c.mycol] # access via Column object.  
    
    ResultProxy also contains a map of TypeEngine objects and will invoke the appropriate
    convert_result_value() method before returning columns.
    """
    class AmbiguousColumn(object):
        def __init__(self, key):
            self.key = key
        def convert_result_value(self, arg, engine):
            raise InvalidRequestError("Ambiguous column name '%s' in result set! try 'use_labels' option on select statement." % (self.key))
    
    def __init__(self, engine, connection, cursor, executioncontext=None, typemap=None):
        """ResultProxy objects are constructed via the execute() method on SQLEngine."""
        self.connection = connection
        self.dialect = engine.dialect
        self.cursor = cursor
        self.engine = engine
        self.closed = False
        self.executioncontext = executioncontext
        self.echo = engine.echo=="debug"
        if executioncontext:
            self.rowcount = executioncontext.get_rowcount(cursor)
        else:
            self.rowcount = cursor.rowcount
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
    def close(self):
        if not self.closed:
            self.closed = True
            if self.connection.should_close_with_result and self.dialect.supports_autoclose_results:
                self.connection.close()
    def _get_col(self, row, key):
        if isinstance(key, sql.ColumnElement):
            try:
                rec = self.props[key._label.lower()]
            except KeyError:
                try:
                    rec = self.props[key.key.lower()]
                except KeyError:
                    rec = self.props[key.name.lower()]
        elif isinstance(key, str):
            rec = self.props[key.lower()]
        else:
            rec = self.props[key]
        return rec[0].dialect_impl(self.dialect).convert_result_value(row[rec[1]], self.dialect)
    
    def __iter__(self):
        while True:
            row = self.fetchone()
            if row is None:
                raise StopIteration
            else:
                yield row
     
    def last_inserted_ids(self):
        return self.executioncontext.last_inserted_ids()
    def last_updated_params(self):
        return self.executioncontext.last_updated_params()
    def last_inserted_params(self):
        return self.executioncontext.last_inserted_params()
    def lastrow_has_defaults(self):
        return self.executioncontext.lastrow_has_defaults()
    def supports_sane_rowcount(self):
        return self.executioncontext.supports_sane_rowcount()
        
    def fetchall(self):
        """fetches all rows, just like DBAPI cursor.fetchall()."""
        l = []
        while True:
            v = self.fetchone()
            if v is None:
                return l
            l.append(v)
            
    def fetchone(self):
        """fetches one row, just like DBAPI cursor.fetchone()."""
        row = self.cursor.fetchone()
        if row is not None:
            if self.echo: self.engine.log(repr(row))
            return RowProxy(self, row)
        else:
            # controversy!  can we auto-close the cursor after results are consumed ?
            # what if the returned rows are still hanging around, and are "live" objects 
            # and not just plain tuples ?
            self.close()
            return None

class RowProxy:
    """proxies a single cursor row for a parent ResultProxy."""
    def __init__(self, parent, row):
        """RowProxy objects are constructed by ResultProxy objects."""
        self.__parent = parent
        self.__row = row
    def close(self):
        self.__parent.close()
    def __iter__(self):
        for i in range(0, len(self.__row)):
            yield self.__parent._get_col(self.__row, i)
    def __eq__(self, other):
        return (other is self) or (other == tuple([self.__parent._get_col(self.__row, key) for key in range(0, len(self.__row))]))
    def __repr__(self):
        return repr(tuple([self.__parent._get_col(self.__row, key) for key in range(0, len(self.__row))]))
    def __getitem__(self, key):
        return self.__parent._get_col(self.__row, key)
    def __getattr__(self, name):
        try:
            return self.__parent._get_col(self.__row, name)
        except KeyError:
            raise AttributeError
    def items(self):
        return [(key, getattr(self, key)) for key in self.keys()]
    def keys(self):
        return self.__parent.keys
    def values(self): 
        return list(self)
    def __len__(self): 
        return len(self.__row)

class SchemaIterator(schema.SchemaVisitor):
    """a visitor that can gather text into a buffer and execute the contents of the buffer."""
    def __init__(self, engine, proxy, **params):
        self.proxy = proxy
        self.engine = engine
        self.buffer = StringIO.StringIO()

    def append(self, s):
        """appends content to the SchemaIterator's query buffer."""
        self.buffer.write(s)

    def execute(self):
        """executes the contents of the SchemaIterator's buffer using its sql proxy and
        clears out the buffer."""
        try:
            return self.proxy(self.buffer.getvalue(), None)
        finally:
            self.buffer.truncate(0)

class DefaultRunner(schema.SchemaVisitor):
    def __init__(self, engine, proxy):
        self.proxy = proxy
        self.engine = engine

    def get_column_default(self, column):
        if column.default is not None:
            return column.default.accept_schema_visitor(self)
        else:
            return None

    def get_column_onupdate(self, column):
        if column.onupdate is not None:
            return column.onupdate.accept_schema_visitor(self)
        else:
            return None

    def visit_passive_default(self, default):
        """passive defaults by definition return None on the app side,
        and are post-fetched to get the DB-side value"""
        return None

    def visit_sequence(self, seq):
        """sequences are not supported by default"""
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
