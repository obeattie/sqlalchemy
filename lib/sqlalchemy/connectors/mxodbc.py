
import sys
import re

from sqlalchemy.connectors import Connector

class MxODBCConnector(Connector):
    driver='mxodbc'
    
    supports_sane_multi_rowcount = False
    supports_unicode_statements = False
    supports_unicode_binds = False
    supports_native_decimal = False
    
    @classmethod
    def dbapi(cls):
        platform = sys.platform
        if platform == 'win32':
            from mx.ODBC import Windows as module
        # this can be the string "linux2", and possibly others
        elif 'linux' in platform:
            from mx.ODBC import unixODBC as module
        elif platform == 'darwin':
            from mx.ODBC import iODBC as module
        else:
            raise ImportError, "Unrecognized platform for mxODBC import"
        return module

    def visit_pool(self, pool):
        def connect(conn, rec):
            conn.stringformat = self.dbapi.MIXED_STRINGFORMAT
            conn.datetimeformat = self.dbapi.PYDATETIME_DATETIMEFORMAT
            #conn.bindmethod = self.dbapi.BIND_USING_PYTHONTYPE
            #conn.bindmethod = self.dbapi.BIND_USING_SQLTYPE

        pool.add_listener({'connect':connect})

    def create_connect_args(self, url):
        """ Return a tuple of *args,**kwargs for creating a connection.

        The mxODBC 3.x connection constructor looks like this:

            connect(dsn, user='', password='',
                    clear_auto_commit=1, errorhandler=None)

        This method translates the values in the provided uri
        into args and kwargs needed to instantiate an mxODBC Connection.

        The arg 'errorhandler' is not used by SQLAlchemy and will
        not be populated.
        """
        opts = url.translate_connect_args(username='user')
        opts.update(url.query)
        args = opts['host'],
        kwargs = {'user':opts['user'],
                  'password': opts['password']}
        return args, kwargs

    def is_disconnect(self, e):
        if isinstance(e, self.dbapi.ProgrammingError):
            return "connection already closed" in str(e)
        elif isinstance(e, self.dbapi.Error):
            return '[08S01]' in str(e)
        else:
            return False

    def _get_server_version_info(self, connection):
        dbapi_con = connection.connection
        version = []
        r = re.compile('[.\-]')
        # 18 == pyodbc.SQL_DBMS_VER
        for n in r.split(dbapi_con.getinfo(18)[1]):
            try:
                version.append(int(n))
            except ValueError:
                version.append(n)
        return tuple(version)
