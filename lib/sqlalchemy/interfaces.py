# interfaces.py
# Copyright (C) 2007 Jason Kirtland jek@discorporate.us
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Interfaces and abstract types."""


class PoolListener(object):
    """Hooks into the lifecycle of connections in a ``Pool``.

    All of the standard connection [sqlalchemy.pool#Pool] types can
    accept event listeners for key connection lifecycle events:
    creation, pool check-out and check-in.  There are no events raised
    when a connection closes.

    For any given DB-API connection, there will be one ``connect``
    event, `n` number of ``checkout`` events, and `n` or `n -1`
    ``checkin`` events.  (If a ``Connection`` is detached from its
    pool via the ``detach()`` method, it won't be checked back in.)

    These are low-level events for low-level objects: raw Python
    DB-API connections, without the conveniences of the SQLAlchemy
    ``Connection`` wrapper, ``Dialect`` services or ``ClauseElement``
    execution.  If you execute SQL through the connection, explicitly
    closing all cursors and other resources is recommended.

    Events also receive a ``_ConnectionRecord``, a long-lived internal
    ``Pool`` object that basically represents a "slot" in the
    connection pool.  ``_ConnectionRecord`` objects have one public
    attribute of note: ``properties``, a dictionary whose contents are
    scoped to the lifetime of the DB-API connection managed by the
    record.

    There is no need to subclass ``PoolListener`` to handle events.
    Any class that implements one or more of these methods can be used
    as a pool listener.  The ``Pool`` will inspect a listener and add
    it to each internal event queue that it can handle.  In terms of
    efficiency and function call overhead, you're better off only
    providing implementations for the hooks you'll be using.
    """

    def connect(dbapi_con, con_record):
        """Called once for each new DB-API connection or Pool's ``creator()``.

        dbapi_con
          A newly connected raw DB-API connection (not a SQLAlchemy
          ``Connection`` wrapper).

        con_record
          The ``_ConnectionRecord`` that currently owns the connection
        """

    def checkout(dbapi_con, con_record):
        """Called when a connection is retrieved from the Pool.

        dbapi_con
          A raw DB-API connection

        con_record
          The ``_ConnectionRecord`` that currently owns the connection

        If you raise an ``exceptions.DisconnectionError``, the current
        connection will be disposed and a fresh connection retrieved.
        Processing of all checkout listeners will abort and restart
        using the new connection.
        """

    def checkin(dbapi_con, con_record):
        """Called when a connection returns to the pool.

        Note that the connection may be closed, and may be None if the
        connection has been invalidated.  ``checkin`` will not be called
        for detached connections.  (They do not return to the pool.)

        dbapi_con
          A raw DB-API connection

        con_record
          The _ConnectionRecord that currently owns the connection
        """
