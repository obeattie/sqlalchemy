<%flags>inherit='document_base.myt'</%flags>
<%attr>title='Connection Pooling'</%attr>
<!-- WARNING! This file was automatically generated.
     Modify .txt file if need you to change the content.-->
<&|doclib.myt:item, name="pooling", description="Connection Pooling"&>

<p>This section describes the connection pool module of SQLAlchemy.  The <code>Pool</code> object it provides is normally embedded within an <code>Engine</code> instance.  For most cases, explicit access to the pool module is not required.  However, the <code>Pool</code> object can be used on its own, without the rest of SA, to manage DBAPI connections; this section describes that usage.  Also, this section will describe in more detail how to customize the pooling strategy used by an <code>Engine</code>.
</p>
<p>At the base of any database helper library is a system of efficiently acquiring connections to the database.  Since the establishment of a database connection is typically a somewhat expensive operation, an application needs a way to get at database connections repeatedly without incurring the full overhead each time.  Particularly for server-side web applications, a connection pool is the standard way to maintain a "pool" of database connections which are used over and over again among many requests.  Connection pools typically are configured to maintain a certain "size", which represents how many connections can be used simultaneously without resorting to creating more newly-established connections.
</p>

<&|doclib.myt:item, name="establishing", description="Establishing a Transparent Connection Pool"&>

<p>Any DBAPI module can be "proxied" through the connection pool using the following technique (note that the usage of 'psycopg2' is <strong>just an example</strong>; substitute whatever DBAPI module you'd like):
</p>
<&|formatting.myt:code, use_sliders="True", syntaxtype="python"&>  import sqlalchemy.pool as pool
  import psycopg2 as psycopg
  psycopg = pool.manage(psycopg)
  
  # then connect normally
  connection = psycopg.connect(database='test', username='scott', password='tiger')
  </&><p>This produces a <code>sqlalchemy.pool.DBProxy</code> object which supports the same <code>connect()</code> function as the original DBAPI module.  Upon connection, a thread-local connection proxy object is returned, which delegates its calls to a real DBAPI connection object.  This connection object is stored persistently within a connection pool (an instance of <code>sqlalchemy.pool.Pool</code>) that corresponds to the exact connection arguments sent to the <code>connect()</code> function.  The connection proxy also returns a proxied cursor object upon calling <code>connection.cursor()</code>.  When all cursors as well as the connection proxy are de-referenced, the connection is automatically made available again by the owning pool object.
</p>
<p>Basically, the <code>connect()</code> function is used in its usual way, and the pool module transparently returns thread-local pooled connections.  Each distinct set of connect arguments corresponds to a brand new connection pool created; in this way, an application can maintain connections to multiple schemas and/or databases, and each unique connect argument set will be managed by a different pool object.
</p>

</&>
<&|doclib.myt:item, name="configuration", description="Connection Pool Configuration"&>

<p>When proxying a DBAPI module through the <code>pool</code> module, options exist for how the connections should be pooled:
</p>
<ul>
 <li>
     echo=False : if set to True, connections being pulled and retrieved from/to the pool will be logged to the standard output, as well as pool sizing information.
 </li>

 <li>
     use_threadlocal=True : if set to True, repeated calls to connect() within the same application thread will be guaranteed to return the <strong>same</strong> connection object, if one has already been retrieved from the pool and has not been returned yet.  This allows code to retrieve a connection from the pool, and then while still holding on to that connection, to call other functions which also ask the pool for a connection of the same arguments;  those functions will act upon the same connection that the calling method is using.  Note that once the connection is returned to the pool, it then may be used by another thread.  To guarantee a single unique connection per thread that <strong>never</strong> changes, use the option <code>poolclass=SingletonThreadPool</code>, in which case the use_threadlocal parameter is automatically set to False.
 </li>

 <li>
     poolclass=QueuePool :  the Pool class used by the pool module to provide pooling.  QueuePool uses the Python <code>Queue.Queue</code> class to maintain a list of available connections.  A developer can supply his or her own Pool class to supply a different pooling algorithm.  Also included is the <code>SingletonThreadPool</code>, which provides a single distinct connection per thread and is required with SQLite.
 </li>

 <li>
     pool_size=5 : used by <code>QueuePool</code> - the size of the pool to be maintained.  This is the largest number of connections that will be kept persistently in the pool.  Note that the pool begins with no connections; once this number of connections is requested, that number of connections will remain.
 </li>

 <li>
     max_overflow=10 : used by <code>QueuePool</code> - the maximum overflow size of the pool.  When the number of checked-out connections reaches the size set in pool_size, additional connections will be returned up to this limit.  When those additional connections are returned to the pool, they are disconnected and discarded.  It follows then that the total number of simultaneous connections the pool will allow is <code>pool_size</code> + <code>max_overflow</code>, and the total number of "sleeping" connections the pool will allow is <code>pool_size</code>.  <code>max_overflow</code> can be set to -1 to indicate no overflow limit; no limit will be placed on the total number of concurrent connections.
 </li>

 <li>
     timeout=30 : used by <code>QueuePool</code> - the timeout before giving up on returning a connection, if none are available and the <code>max_overflow</code> has been reached.
 </li>
</ul>

</&>
<&|doclib.myt:item, name="custom", description="Custom Pool Construction"&>

<p>One level below using a DBProxy to make transparent pools is creating the pool yourself.  The pool module comes with two implementations of connection pools: <code>QueuePool</code> and <code>SingletonThreadPool</code>.  While <code>QueuePool</code> uses <code>Queue.Queue</code> to provide connections, <code>SingletonThreadPool</code> provides a single per-thread connection which SQLite requires.
</p>
<p>Constructing your own pool involves passing a callable used to create a connection.  Through this method, custom connection schemes can be made, such as a connection that automatically executes some initialization commands to start.  The options from the previous section can be used as they apply to <code>QueuePool</code> or <code>SingletonThreadPool</code>.
</p>
<&|formatting.myt:code, use_sliders="True", syntaxtype="python", title="Plain QueuePool"&>  import sqlalchemy.pool as pool
  import psycopg2
  
  def getconn():
      c = psycopg2.connect(username='ed', host='127.0.0.1', dbname='test')
      # execute an initialization function on the connection before returning
      c.cursor.execute("setup_encodings()")
      return c
  
  p = pool.QueuePool(getconn, max_overflow=10, pool_size=5, use_threadlocal=True)
  </&><p>Or with SingletonThreadPool:
</p>
<&|formatting.myt:code, use_sliders="True", syntaxtype="python", title="SingletonThreadPool"&>  import sqlalchemy.pool as pool
  import sqlite
  
  def getconn():
      return sqlite.connect(filename='myfile.db')
  
  # SQLite connections require the SingletonThreadPool    
  p = pool.SingletonThreadPool(getconn)
  </&>
</&>
</&>
