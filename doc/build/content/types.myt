<%flags>inherit='document_base.myt'</%flags>
<%attr>title='The Types System'</%attr>

<&|doclib.myt:item, name="types", description="The Types System" &>
<p>The package <span class="codeline">sqlalchemy.types</span> defines the datatype identifiers which may be used when defining <&formatting.myt:link, path="metadata", text="table metadata"&>.  This package includes a set of generic types, a set of SQL-specific subclasses of those types, and a small extension system used by specific database connectors to adapt these generic types into database-specific type objects.
</p>
<&|doclib.myt:item, name="standard", description="Built-in Types" &>

<p>SQLAlchemy comes with a set of standard generic datatypes, which are defined as classes.  They are specified to table meta data using either the class itself, or an instance of the class.  Creating an instance of the class allows you to specify parameters for the type, such as string length, numerical precision, etc. 
</p>
<p>The standard set of generic types are:</p>
<&|formatting.myt:code&>
# sqlalchemy.types package:
class String: pass
class Integer: pass
class Numeric: pass
class DateTime: pass
class Binary: pass
class Boolean: pass
</&>
<p>More specific subclasses of these types are available, to allow finer grained control over types:</p>
<&|formatting.myt:code&>
class FLOAT(Numeric):pass
class TEXT(String):pass
class DECIMAL(Numeric):pass
class INT(Integer):pass
INTEGER = INT
class TIMESTAMP(DateTime): pass
class DATETIME(DateTime): pass
class CLOB(String): pass
class VARCHAR(String): pass
class CHAR(String):pass
class BLOB(Binary): pass
class BOOLEAN(Boolean): pass
</&>
<p>When using a specific database engine, these types are adapted even further via a set of database-specific subclasses defined by the database engine.</p>
</&>

<&|doclib.myt:item, name="custom", description="Creating your Own Types" &>
<p>Types also support pre-processing of query parameters as well as post-processing of result set data.  You can make your own classes to perform these operations.  They are specified by subclassing the desired type class as well as the special mixin TypeDecorator, which manages the adaptation of the underlying type to a database-specific type:</p>
<&|formatting.myt:code&>
    import sqlalchemy.types as types

    class MyType(types.TypeDecorator, types.String):
        """basic type that decorates String, prefixes values with "PREFIX:" on 
        the way in and strips it off on the way out."""
        def convert_bind_param(self, value):
            return "PREFIX:" + value
        def convert_result_value(self, value):
            return value[7:]
</&>
<p>Another example, which illustrates a fully defined datatype.  This just overrides the base type class TypeEngine:</p>
<&|formatting.myt:code&>
    import sqlalchemy.types as types

    class MyType(types.TypeEngine):
        def __init__(self, precision = 8):
            self.precision = precision
        def get_col_spec(self):
            return "MYTYPE(%s)" % self.precision
        def convert_bind_param(self, value):
            return value
        def convert_result_value(self, value):
            return value
        def adapt(self, typeobj):
            """produces an adaptation of this object given a type which is a subclass of this object"""
            return typeobj(self.precision)
        def adapt_args(self):
            """allows for the adaptation of this TypeEngine object into a new kind of type depending on its arguments."""
            return self
</&>
</&>
</&>