from sqlalchemy.connectors import Connector

import sys
import re
import urllib

class PyODBCConnector(Connector):
    driver='pyodbc'
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    # PyODBC unicode is broken on UCS-4 builds
    supports_unicode = sys.maxunicode == 65535
    supports_unicode_statements = supports_unicode
    default_paramstyle = 'named'
    
    # for non-DSN connections, this should
    # hold the desired driver name
    pyodbc_driver_name = None
    
    @classmethod
    def dbapi(cls):
        return __import__('pyodbc')

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        opts.update(url.query)
        
        keys = opts
        query = url.query

        if 'odbc_connect' in keys:
            connectors = [urllib.unquote_plus(keys.pop('odbc_connect'))]
        else:
            dsn_connection = 'dsn' in keys or ('host' in keys and 'database' not in keys)
            if dsn_connection:
                connectors= ['dsn=%s' % (keys.pop('host', '') or keys.pop('dsn', ''))]
            else:
                port = ''
                if 'port' in keys and not 'port' in query:
                    port = ',%d' % int(keys.pop('port'))

                connectors = ["DRIVER={%s}" % keys.pop('driver', self.pyodbc_driver_name),
                              'Server=%s%s' % (keys.pop('host', ''), port),
                              'Database=%s' % keys.pop('database', '') ]

            user = keys.pop("user", None)
            if user:
                connectors.append("UID=%s" % user)
                connectors.append("PWD=%s" % keys.pop('password', ''))
            else:
                connectors.append("Trusted_Connection=Yes")

            # if set to 'Yes', the ODBC layer will try to automagically convert 
            # textual data from your database encoding to your client encoding 
            # This should obviously be set to 'No' if you query a cp1253 encoded 
            # database from a latin1 client... 
            if 'odbc_autotranslate' in keys:
                connectors.append("AutoTranslate=%s" % keys.pop("odbc_autotranslate"))

            connectors.extend(['%s=%s' % (k,v) for k,v in keys.iteritems()])
        return [[";".join (connectors)], {}]

    def is_disconnect(self, e):
        if isinstance(e, self.dbapi.ProgrammingError):
            return "The cursor's connection has been closed." in str(e) or 'Attempt to use a closed connection.' in str(e)
        elif isinstance(e, self.dbapi.Error):
            return '[08S01]' in str(e)
        else:
            return False

    def _get_server_version_info(self, connection):
        dbapi_con = connection.connection
        version = []
        r = re.compile('[.\-]')
        for n in r.split(dbapi_con.getinfo(self.dbapi.SQL_DBMS_VER)):
            try:
                version.append(int(n))
            except ValueError:
                version.append(n)
        return tuple(version)
