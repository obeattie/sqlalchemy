# kinterbasdb.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009, 2010 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
The most common way to connect to a Firebird engine is implemented by
kinterbasdb__, currently maintained__ directly by the Firebird people.

The connection URL is of the form
``firebird[+kinterbasdb]://user:password@host:port/path/to/db[?key=value&key=value...]``.

Kinterbasedb backend specific keyword arguments are:

type_conv
  select the kind of mapping done on the types: by default SQLAlchemy
  uses 200 with Unicode, datetime and decimal support (see details__).

concurrency_level
  set the backend policy with regards to threading issues: by default
  SQLAlchemy uses policy 1 (see details__).

__ http://sourceforge.net/projects/kinterbasdb
__ http://firebirdsql.org/index.php?op=devel&sub=python
__ http://kinterbasdb.sourceforge.net/dist_docs/usage.html#adv_param_conv_dynamic_type_translation
__ http://kinterbasdb.sourceforge.net/dist_docs/usage.html#special_issue_concurrency
"""

from sqlalchemy.dialects.firebird.base import FBDialect, FBCompiler


class Firebird_kinterbasdb(FBDialect):
    driver = 'kinterbasdb'
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False

    def __init__(self, type_conv=200, concurrency_level=1, **kwargs):
        super(Firebird_kinterbasdb, self).__init__(**kwargs)

        self.type_conv = type_conv
        self.concurrency_level = concurrency_level

    @classmethod
    def dbapi(cls):
        k = __import__('kinterbasdb')
        return k

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        if opts.get('port'):
            opts['host'] = "%s/%s" % (opts['host'], opts['port'])
            del opts['port']
        opts.update(url.query)

        type_conv = opts.pop('type_conv', self.type_conv)
        concurrency_level = opts.pop('concurrency_level', self.concurrency_level)

        if self.dbapi is not None:
            initialized = getattr(self.dbapi, 'initialized', None)
            if initialized is None:
                # CVS rev 1.96 changed the name of the attribute:
                # http://kinterbasdb.cvs.sourceforge.net/viewvc/kinterbasdb/Kinterbasdb-3.0/__init__.py?r1=1.95&r2=1.96
                initialized = getattr(self.dbapi, '_initialized', False)
            if not initialized:
                self.dbapi.init(type_conv=type_conv, concurrency_level=concurrency_level)
        return ([], opts)

    def _get_server_version_info(self, connection):
        """Get the version of the Firebird server used by a connection.

        Returns a tuple of (`major`, `minor`, `build`), three integers
        representing the version of the attached server.
        """

        # This is the simpler approach (the other uses the services api),
        # that for backward compatibility reasons returns a string like
        #   LI-V6.3.3.12981 Firebird 2.0
        # where the first version is a fake one resembling the old
        # Interbase signature. This is more than enough for our purposes,
        # as this is mainly (only?) used by the testsuite.

        from re import match

        fbconn = connection.connection
        version = fbconn.server_version
        m = match('\w+-V(\d+)\.(\d+)\.(\d+)\.(\d+) \w+ (\d+)\.(\d+)', version)
        if not m:
            raise AssertionError("Could not determine version from string '%s'" % version)
        return tuple([int(x) for x in m.group(5, 6, 4)])

    def is_disconnect(self, e):
        if isinstance(e, (self.dbapi.OperationalError, self.dbapi.ProgrammingError)):
            msg = str(e)
            return ('Unable to complete network request to host' in msg or
                    'Invalid connection state' in msg or
                    'Invalid cursor state' in msg)
        else:
            return False

dialect = Firebird_kinterbasdb
