# exceptions.py - exceptions for SQLAlchemy
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


class SQLAlchemyError(Exception):
    """generic error class"""
    pass
    
class SQLError(SQLAlchemyError):
    """raised when the execution of a SQL statement fails.  includes accessors
    for the underlying exception, as well as the SQL and bind parameters"""
    def __init__(self, statement, params, orig):
        SQLAlchemyError.__init__(self, "(%s) %s"% (orig.__class__.__name__, str(orig)))
        self.statement = statement
        self.params = params
        self.orig = orig

class ArgumentError(SQLAlchemyError):
    """raised for all those conditions where invalid arguments are sent to constructed
    objects.  This error generally corresponds to construction time state errors."""
    pass
    
class CommitError(SQLAlchemyError):
    """raised when an invalid condition is detected upon a commit()"""
    pass
    
class InvalidRequestError(SQLAlchemyError):
    """sqlalchemy was asked to do something it cant do, return nonexistent data, etc.
    This error generally corresponds to runtime state errors."""
    pass

class AssertionError(SQLAlchemyError):
    """corresponds to internal state being detected in an invalid state"""
    pass
    
class DBAPIError(SQLAlchemyError):
    """something weird happened with a particular DBAPI version"""
    pass