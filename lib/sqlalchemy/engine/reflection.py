"""Provides an abstracting for obtaining database schema information.

Notes:

The Inspector currently returns tuples and lists of tuples, but it may be
better to return dicts and lists of dicts.  This would make it easier to add
attributes.


"""
import sqlalchemy

class Inspector(object):
    
    def __init__(self, conn):
        """performs database introspection for `conn`

        conn
          [sqlalchemy.engine.base.#Connectable]

        """
        self.info_cache = {}
        self.conn = conn
        # set the engine
        if hasattr(conn, 'engine'):
            self.engine = conn.engine
        else:
            self.engine = conn

    def table_names(self, schema=None):
        """
        schema:
          Optional, retrieve names from a non-default schema.

        This should probably not return view names or maybe it should return
        them with an indicator t or v.

        """
        return self.engine.table_names(schema)

    def get_columns(self, table_name, schema=None):
        """Return information about columns in `table_name`.

        Given a string `table_name` and an optional string `schema`, return
        column information as a list of tuples of the form:

        (name, coltype, nullable, colattrs)
        
        name
          the column's name

        coltype
          [sqlalchemy.types#TypeEngine]

        nullable
          boolean

        colattrs
          dict containing optional column attributes

        """

        return self.engine.dialect.get_columns(self.conn, table_name,
                                               schema=schema,
                                               info_cache=self.info_cache)

    def get_primary_keys(self, table_name, schema=None):
        """Return information about primary keys in `table_name`.

        Given a string `table_name`, and an optional string `schema`, return 
        primary key information as a list of tuples of the form:

        (colname, )

        A tuple is used here to leave room for other data items should they be
        added to the spec.

        """

        return self.engine.dialect.get_primary_keys(self.conn, table_name,
                                               schema=schema,
                                               info_cache=self.info_cache)

    def get_foreign_keys(self, table_name, schema=None):
        """Return information about foreign_keys in `table_name`.

        Given a string `table_name`, and an optional string `schema`, return 
        foreign key information as a list of tuples of the form:

        (constraint_name, constrained_columns, referred_schema, referred_table, 
         referred_columns)

        constrained_columns
          a list of column names that make up the foreign key

        referred_schema
          the name of the referred schema

        referred_table
          the name of the referred table

        referred_columns
          a list of column names in the referred table that correspond to
          constrained_columns

        """

        return self.engine.dialect.get_foreign_keys(self.conn, table_name,
                                               schema=schema,
                                               info_cache=self.info_cache)

if __name__ == '__main__':
    e = sqlalchemy.create_engine('postgres:///test1')
    insp = Inspector(e)
    print insp.table_names()
    print insp.get_columns('customers')
    print insp.get_primary_keys('customers')
    print insp.get_foreign_keys('orders', schema='public')
