# engine/__init__.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""SQL connections, SQL execution and high-level DB-API interface.

The engine package defines the basic components used to interface
DB-API modules with higher-level statement construction,
connection-management, execution and result contexts.  The primary
"entry point" class into this package is the Engine and it's public
constructor ``create_engine()``.

This package includes:

base.py
    Defines interface classes and some implementation classes which
    comprise the basic components used to interface between a DB-API,
    constructed and plain-text statements, connections, transactions,
    and results.

default.py
    Contains default implementations of some of the components defined
    in base.py.  All current database dialects use the classes in
    default.py as base classes for their own database-specific
    implementations.

strategies.py
    The mechanics of constructing ``Engine`` objects are represented
    here.  Defines the ``EngineStrategy`` class which represents how
    to go from arguments specified to the ``create_engine()``
    function, to a fully constructed ``Engine``, including
    initialization of connection pooling, dialects, and specific
    subclasses of ``Engine``.

threadlocal.py
    The ``TLEngine`` class is defined here, which is a subclass of
    the generic ``Engine`` and tracks ``Connection`` and
    ``Transaction`` objects against the identity of the current
    thread.  This allows certain programming patterns based around
    the concept of a "thread-local connection" to be possible.
    The ``TLEngine`` is created by using the "threadlocal" engine
    strategy in conjunction with the ``create_engine()`` function.

url.py
    Defines the ``URL`` class which represents the individual
    components of a string URL passed to ``create_engine()``.  Also
    defines a basic module-loading strategy for the dialect specifier
    within a URL.
"""

import sqlalchemy.databases
from sqlalchemy.engine.base import (
    BufferedColumnResultProxy,
    BufferedColumnRow,
    BufferedRowResultProxy,
    Compiled,
    Connectable,
    Connection,
    DefaultRunner,
    Dialect,
    Engine,
    ExecutionContext,
    NestedTransaction,
    ResultProxy,
    RootTransaction,
    RowProxy,
    SchemaIterator,
    Transaction,
    TwoPhaseTransaction
    )
from sqlalchemy.engine import strategies
from sqlalchemy import util


__all__ = (
    'BufferedColumnResultProxy',
    'BufferedColumnRow',
    'BufferedRowResultProxy',
    'Compiled',
    'Connectable',
    'Connection',
    'DefaultRunner',
    'Dialect',
    'Engine',
    'ExecutionContext',
    'NestedTransaction',
    'ResultProxy',
    'RootTransaction',
    'RowProxy',
    'SchemaIterator',
    'Transaction',
    'TwoPhaseTransaction',
    'create_engine',
    'engine_from_config',
    )


default_strategy = 'plain'
def create_engine(*args, **kwargs):
    """Create a new Engine instance.

    The standard method of specifying the engine is via URL as the
    first positional argument, to indicate the appropriate database
    dialect and connection arguments, with additional keyword
    arguments sent as options to the dialect and resulting Engine.

    The URL is a string in the form
    ``dialect://user:password@host/dbname[?key=value..]``, where
    ``dialect`` is a name such as ``mysql``, ``oracle``, ``postgres``,
    etc.  Alternatively, the URL can be an instance of
    ``sqlalchemy.engine.url.URL``.

    `**kwargs` represents options to be sent to the Engine itself as
    well as the components of the Engine, including the Dialect, the
    ConnectionProvider, and the Pool. 

    Descriptions of arguments can be found at :ref:`create_engine_args`.
      
    """

    strategy = kwargs.pop('strategy', default_strategy)
    strategy = strategies.strategies[strategy]
    return strategy.create(*args, **kwargs)

def engine_from_config(configuration, prefix='sqlalchemy.', **kwargs):
    """Create a new Engine instance using a configuration dictionary.

    The dictionary is typically produced from a config file where keys
    are prefixed, such as sqlalchemy.url, sqlalchemy.echo, etc.  The
    'prefix' argument indicates the prefix to be searched for.

    A select set of keyword arguments will be "coerced" to their
    expected type based on string values.  In a future release, this
    functionality will be expanded and include dialect-specific
    arguments.
    """

    opts = _coerce_config(configuration, prefix)
    opts.update(kwargs)
    url = opts.pop('url')
    return create_engine(url, **opts)

def _coerce_config(configuration, prefix):
    """Convert configuration values to expected types."""

    options = dict((key[len(prefix):], configuration[key])
                   for key in configuration
                   if key.startswith(prefix))
    for option, type_ in (
        ('convert_unicode', bool),
        ('pool_timeout', int),
        ('echo', bool),
        ('echo_pool', bool),
        ('pool_recycle', int),
        ('pool_size', int),
        ('max_overflow', int),
        ('pool_threadlocal', bool),
    ):
        util.coerce_kw_type(options, option, type_)
    return options
