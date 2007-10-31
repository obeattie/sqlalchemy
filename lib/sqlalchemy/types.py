# types.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

__all__ = [ 'TypeEngine', 'TypeDecorator',
            'INT', 'CHAR', 'VARCHAR', 'NCHAR', 'TEXT', 'FLOAT',
            'NUMERIC', 'DECIMAL', 'TIMESTAMP', 'DATETIME', 'CLOB', 'BLOB',
            'BOOLEAN', 'SMALLINT', 'DATE', 'TIME',
            'String', 'Integer', 'SmallInteger','Smallinteger',
            'Numeric', 'Float', 'DateTime', 'Date', 'Time', 'Binary',
            'Boolean', 'Unicode', 'PickleType', 'Interval',
            ]

import inspect
import datetime as dt

from sqlalchemy import exceptions
from sqlalchemy.util import Decimal, pickle

class _UserTypeAdapter(type):
    """adapts 0.3 style user-defined types with convert_bind_param/convert_result_value
    to use newer bind_processor()/result_processor() methods."""
    
    def __init__(cls, clsname, bases, dict):
        if not hasattr(cls.convert_result_value, '_sa_override'):
            cls.__instrument_result_proc(cls)
            
        if not hasattr(cls.convert_bind_param, '_sa_override'):
            cls.__instrument_bind_proc(cls)
            
        return super(_UserTypeAdapter, cls).__init__(clsname, bases, dict)

    def __instrument_bind_proc(cls, class_):
        def bind_processor(self, dialect):
            def process(value):
                return self.convert_bind_param(value, dialect)
            return process
        class_.super_bind_processor = class_.bind_processor
        class_.bind_processor = bind_processor

    def __instrument_result_proc(cls, class_):    
        def result_processor(self, dialect):
            def process(value):
                return self.convert_result_value(value, dialect)
            return process
        class_.super_result_processor = class_.result_processor
        class_.result_processor = result_processor

        
class AbstractType(object):
    __metaclass__ = _UserTypeAdapter
    
    def __init__(self, *args, **kwargs):
        pass
        
    def copy_value(self, value):
        return value

    def convert_result_value(self, value, dialect):
        """Legacy convert_result_value() compatibility method.

        This adapter method is provided for user-defined types that implement
        the older convert_* interface and need to call their super method.
        These calls are adapted behind the scenes to use the newer
        callable-based interface via result_processor().

        Compatibility is configured on a case-by-case basis at class
        definition time by a legacy adapter metaclass.  This method is only
        available and functional if the concrete subclass implements the
        legacy interface.
        """

        processor = self.super_result_processor(dialect)
        if processor:
            return processor(value)
        else:
            return value
    convert_result_value._sa_override = True
    
    def convert_bind_param(self, value, dialect):
        """Legacy convert_bind_param() compatability method.
        
        This adapter method is provided for user-defined types that implement
        the older convert_* interface and need to call their super method.
        These calls are adapted behind the scenes to use the newer
        callable-based interface via bind_processor().

        Compatibility is configured on a case-by-case basis at class
        definition time by a legacy adapter metaclass.  This method is only
        available and functional if the concrete subclass implements the
        legacy interface.
        """

        processor = self.super_bind_processor(dialect)
        if processor:
            return processor(value)
        else:
            return value
    convert_bind_param._sa_override = True
    
    def bind_processor(self, dialect):
        """Defines a bind parameter processing function."""
        
        return None

    def result_processor(self, dialect):
        """Defines a result-column processing function."""
        
        return None

    def compare_values(self, x, y):
        """compare two values for equality."""
        
        return x == y

    def is_mutable(self):
        """return True if the target Python type is 'mutable'.
        
        This allows systems like the ORM to know if an object
        can be considered 'not changed' by identity alone.
        """
        
        return False

    def get_dbapi_type(self, dbapi):
        """Return the corresponding type object from the underlying DB-API, if any.

        This can be useful for calling ``setinputsizes()``, for example.
        """

        return None

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, ",".join(["%s=%s" % (k, getattr(self, k)) for k in inspect.getargspec(self.__init__)[0][1:]]))

class TypeEngine(AbstractType):
    def dialect_impl(self, dialect):
        try:
            return self._impl_dict[dialect]
        except AttributeError:
            self._impl_dict = {}
            return self._impl_dict.setdefault(dialect, dialect.type_descriptor(self))
        except KeyError:
            return self._impl_dict.setdefault(dialect, dialect.type_descriptor(self))
    
    def __getstate__(self):
        d = self.__dict__.copy()
        d['_impl_dict'] = {}
        return d
        
    def get_col_spec(self):
        raise NotImplementedError()


    def bind_processor(self, dialect):
        return None
        
    def result_processor(self, dialect):
        return None
        
    def adapt(self, cls):
        return cls()   
    
    def get_search_list(self):
        """return a list of classes to test for a match 
        when adapting this type to a dialect-specific type.
        
        """
        
        return self.__class__.__mro__[0:-1]
        
class TypeDecorator(AbstractType):
    def __init__(self, *args, **kwargs):
        if not hasattr(self.__class__, 'impl'):
            raise exceptions.AssertionError("TypeDecorator implementations require a class-level variable 'impl' which refers to the class of type being decorated")
        self.impl = self.__class__.impl(*args, **kwargs)

    def dialect_impl(self, dialect):
        try:
            return self._impl_dict[dialect]
        except AttributeError:
            self._impl_dict = {}
        except KeyError:
            pass

        typedesc = self.load_dialect_impl(dialect)
        tt = self.copy()
        if not isinstance(tt, self.__class__):
            raise exceptions.AssertionError("Type object %s does not properly implement the copy() method, it must return an object of type %s" % (self, self.__class__))
        tt.impl = typedesc
        self._impl_dict[dialect] = tt
        return tt

    def load_dialect_impl(self, dialect):
        """loads the dialect-specific implementation of this type.
        
        by default calls dialect.type_descriptor(self.impl), but
        can be overridden to provide different behavior.
        """

        return dialect.type_descriptor(self.impl)
        
    def __getattr__(self, key):
        """Proxy all other undefined accessors to the underlying implementation."""

        return getattr(self.impl, key)

    def get_col_spec(self):
        return self.impl.get_col_spec()

    def bind_processor(self, dialect):
        return self.impl.bind_processor(dialect)

    def result_processor(self, dialect):
        return self.impl.result_processor(dialect)

    def copy(self):
        instance = self.__class__.__new__(self.__class__)
        instance.__dict__.update(self.__dict__)
        return instance

    def get_dbapi_type(self, dbapi):
        return self.impl.get_dbapi_type(dbapi)

    def copy_value(self, value):
        return self.impl.copy_value(value)

    def compare_values(self, x, y):
        return self.impl.compare_values(x,y)

    def is_mutable(self):
        return self.impl.is_mutable()

class MutableType(object):
    """A mixin that marks a Type as holding a mutable object."""

    def is_mutable(self):
        return True

    def copy_value(self, value):
        raise NotImplementedError()

    def compare_values(self, x, y):
        return x == y

def to_instance(typeobj):
    if typeobj is None:
        return NULLTYPE
    elif isinstance(typeobj, type):
        return typeobj()
    else:
        return typeobj

def adapt_type(typeobj, colspecs):
    if isinstance(typeobj, type):
        typeobj = typeobj()
    for t in typeobj.get_search_list():
        try:
            impltype = colspecs[t]
            break
        except KeyError:
            pass
    else:
        # couldnt adapt - so just return the type itself
        # (it may be a user-defined type)
        return typeobj
    # if we adapted the given generic type to a database-specific type,
    # but it turns out the originally given "generic" type
    # is actually a subclass of our resulting type, then we were already
    # given a more specific type than that required; so use that.
    if (issubclass(typeobj.__class__, impltype)):
        return typeobj
    return typeobj.adapt(impltype)

class NullType(TypeEngine):
    def get_col_spec(self):
        raise NotImplementedError()

NullTypeEngine = NullType

class Concatenable(object):
    """marks a type as supporting 'concatenation'"""
    pass
    
class String(TypeEngine, Concatenable):
    def __init__(self, length=None, convert_unicode=False):
        self.length = length
        self.convert_unicode = convert_unicode

    def adapt(self, impltype):
        return impltype(length=self.length, convert_unicode=self.convert_unicode)

    def bind_processor(self, dialect):
        if self.convert_unicode or dialect.convert_unicode:
            def process(value):
                if isinstance(value, unicode):
                    return value.encode(dialect.encoding)
                else:
                    return value
            return process
        else:
            return None
        
    def result_processor(self, dialect):
        if self.convert_unicode or dialect.convert_unicode:
            def process(value):
                if value is not None and not isinstance(value, unicode):
                    return value.decode(dialect.encoding)
                else:
                    return value
            return process
        else:
            return None

    def get_search_list(self):
        l = super(String, self).get_search_list()
        # if we are String or Unicode with no length,
        # return TEXT as the highest-priority type
        # to be adapted by the dialect
        if self.length is None and l[0] in (String, Unicode):
            return (TEXT,) + l
        else:
            return l

    def get_dbapi_type(self, dbapi):
        return dbapi.STRING

class Unicode(String):
    def __init__(self, length=None, **kwargs):
        kwargs['convert_unicode'] = True
        super(Unicode, self).__init__(length=length, **kwargs)
    
class Integer(TypeEngine):
    """Integer datatype."""

    def get_dbapi_type(self, dbapi):
        return dbapi.NUMBER

class SmallInteger(Integer):
    """Smallint datatype."""

    pass

Smallinteger = SmallInteger

class Numeric(TypeEngine):
    def __init__(self, precision=10, length=2, asdecimal=True):
        self.precision = precision
        self.length = length
        self.asdecimal = asdecimal

    def adapt(self, impltype):
        return impltype(precision=self.precision, length=self.length, asdecimal=self.asdecimal)

    def get_dbapi_type(self, dbapi):
        return dbapi.NUMBER

    def bind_processor(self, dialect):
        def process(value):
            if value is not None:
                return float(value)
            else:
                return value
        return process
    
    def result_processor(self, dialect):
        if self.asdecimal:
            def process(value):
                if value is not None:
                    return Decimal(str(value))
                else:
                    return value
            return process
        else:
            return None

class Float(Numeric):
    def __init__(self, precision = 10, asdecimal=False, **kwargs):
        self.precision = precision
        self.asdecimal = asdecimal
        
    def adapt(self, impltype):
        return impltype(precision=self.precision, asdecimal=self.asdecimal)

class DateTime(TypeEngine):
    """Implement a type for ``datetime.datetime()`` objects."""

    def __init__(self, timezone=False):
        self.timezone = timezone

    def adapt(self, impltype):
        return impltype(timezone=self.timezone)

    def get_dbapi_type(self, dbapi):
        return dbapi.DATETIME

class Date(TypeEngine):
    """Implement a type for ``datetime.date()`` objects."""

    def get_dbapi_type(self, dbapi):
        return dbapi.DATETIME

class Time(TypeEngine):
    """Implement a type for ``datetime.time()`` objects."""

    def __init__(self, timezone=False):
        self.timezone = timezone

    def adapt(self, impltype):
        return impltype(timezone=self.timezone)

    def get_dbapi_type(self, dbapi):
        return dbapi.DATETIME

class Binary(TypeEngine):
    def __init__(self, length=None):
        self.length = length

    def bind_processor(self, dialect):
        DBAPIBinary = dialect.dbapi.Binary
        def process(value):
            if value is not None:
                return DBAPIBinary(value)
            else:
                return None
        return process
        
    def adapt(self, impltype):
        return impltype(length=self.length)

    def get_dbapi_type(self, dbapi):
        return dbapi.BINARY

class PickleType(MutableType, TypeDecorator):
    impl = Binary

    def __init__(self, protocol=pickle.HIGHEST_PROTOCOL, pickler=None, mutable=True, comparator=None):
        self.protocol = protocol
        self.pickler = pickler or pickle
        self.mutable = mutable
        self.comparator = comparator
        super(PickleType, self).__init__()

    def bind_processor(self, dialect):
        impl_process = self.impl.bind_processor(dialect)
        dumps = self.pickler.dumps
        protocol = self.protocol
        if impl_process is None:
            def process(value):
                if value is None:
                    return None
                return dumps(value, protocol)
        else:
            def process(value):
                if value is None:
                    return None
                return impl_process(dumps(value, protocol))
        return process
    
    def result_processor(self, dialect):
        impl_process = self.impl.result_processor(dialect)
        loads = self.pickler.loads
        if impl_process is None:
            def process(value):
                if value is None:
                    return None
                return loads(str(value))
        else:
            def process(value):
                if value is None:
                    return None
                return loads(str(impl_process(value)))
        return process
        
    def copy_value(self, value):
        if self.mutable:
            return self.pickler.loads(self.pickler.dumps(value, self.protocol))
        else:
            return value

    def compare_values(self, x, y):
        if self.comparator:
            return self.comparator(x, y)
        elif self.mutable:
            return self.pickler.dumps(x, self.protocol) == self.pickler.dumps(y, self.protocol)
        else:
            return x is y

    def is_mutable(self):
        return self.mutable

class Boolean(TypeEngine):
    pass
    
class Interval(TypeDecorator):
    """Type to be used in Column statements to store python timedeltas.

        If it's possible it uses native engine features to store timedeltas
        (now it's only PostgreSQL Interval type), if there is no such it
        fallbacks to DateTime storage with converting from/to timedelta on the fly

        Converting is very simple - just use epoch(zero timestamp, 01.01.1970) as
        base, so if we need to store timedelta = 1 day (24 hours) in database it
        will be stored as DateTime = '2nd Jan 1970 00:00', see bind_processor
        and result_processor to actual conversion code
    """
    #Empty useless type, because at the moment of creation of instance we don't
    #know what type will be decorated - it depends on used dialect.
    impl = TypeEngine

    def load_dialect_impl(self, dialect):
        """Checks if engine has native implementation of timedelta python type,
        if so it returns right class to handle it, if there is no native support, 
        it fallback to engine's DateTime implementation class
        """
        if not hasattr(self,'__supported'):
            import sqlalchemy.databases.postgres as pg
            self.__supported = {pg.PGDialect:pg.PGInterval}
            del pg
            
        if self.__hasNativeImpl(dialect):
            #For now, only PostgreSQL has native timedelta types support
            return self.__supported[dialect.__class__]()
        else:
            #All others should fallback to DateTime
            return dialect.type_descriptor(DateTime)
        
    def __hasNativeImpl(self,dialect):
        return dialect.__class__ in self.__supported
    
    def bind_processor(self, dialect):
        impl_processor = self.impl.bind_processor(dialect)
        if self.__hasNativeImpl(dialect):
            return impl_processor
        else:
            zero_timestamp = dt.datetime.utcfromtimestamp(0)
            if impl_processor is None:
                def process(value):
                    if value is None:
                        return None
                    return zero_timestamp + value
            else:
                def process(value):
                    if value is None:
                        return None
                    return impl_processor(zero_timestamp + value)
            return process
            
    def result_processor(self, dialect):
        impl_processor = self.impl.result_processor(dialect)
        if self.__hasNativeImpl(dialect):
            return impl_processor
        else:
            zero_timestamp = dt.datetime.utcfromtimestamp(0)
            if impl_processor is None:
                def process(value):
                    if value is None:
                        return None
                    return value - zero_timestamp
            else:
                def process(value):
                    if value is None:
                        return None
                    return impl_processor(value) - zero_timestamp
            return process
            
class FLOAT(Float): pass
class TEXT(String): pass
class NUMERIC(Numeric): pass
class DECIMAL(Numeric): pass
class INT(Integer): pass
INTEGER = INT
class SMALLINT(Smallinteger): pass
class TIMESTAMP(DateTime): pass
class DATETIME(DateTime): pass
class DATE(Date): pass
class TIME(Time): pass
class CLOB(TEXT): pass
class VARCHAR(String): pass
class CHAR(String): pass
class NCHAR(Unicode): pass
class BLOB(Binary): pass
class BOOLEAN(Boolean): pass

NULLTYPE = NullType()
