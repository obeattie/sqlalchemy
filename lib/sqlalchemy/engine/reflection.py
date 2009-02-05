"""Provides an abstracting for obtaining database schema information.

Development Notes:

I'm still trying to decide upon conventions for both the Inspector interface as well as the dialect interface the Inspector is to consume.  Below are some of the current conventions.

  1. Inspector methods should return lists of dicts in most cases for the 
     following reasons:
    a) They're both simple standard types.
    b) Using a dict instead of a tuple allows easy expansion of attributes.
    c) Using a list for the outer structure maintains order and is easy to work 
       with (e.g. list comprehension [d['name'] for d in cols]).
    d) Being consistent is just good.
  2. Records that contain a name, such as the column name in a column record
     should use the key 'name' in the dict.  This allows the user to expect a
     'name' key and to know what it will reference.


"""
import sqlalchemy
from sqlalchemy.types import TypeEngine

class Inspector(object):
    """performs database introspection

    """
    
    def __init__(self, conn):
        """

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

    def default_schema_name(self):
        return self.engine.dialect.get_default_schema_name(self.conn)
    default_schema_name = property(default_schema_name)

    def get_schema_names(self):
        """Return all schema names.

        """
        if hasattr(self.engine.dialect, 'get_schema_names'):
            return self.engine.dialect.get_schema_names(self.conn)
        return []

    def get_table_names(self, schema=None):
        """Return all table names in `schema`.
        schema:
          Optional, retrieve names from a non-default schema.

        This should probably not return view names or maybe it should return
        them with an indicator t or v.

        """
        if hasattr(self.engine.dialect, 'get_table_names'):
            return self.engine.dialect.get_table_names(self.conn, schema)
        return self.engine.table_names(schema)

    def get_view_names(self, schema=None):
        """Return all view names in `schema`.
        schema:
          Optional, retrieve names from a non-default schema.

        """
        return self.engine.dialect.get_view_names(self.conn, schema)

    def get_view_definition(self, view_name, schema=None):
        """Return definition for `view_name`.
        schema:
          Optional, retrieve names from a non-default schema.

        """
        return self.engine.dialect.get_view_definition(
            self.conn, view_name, schema)

    def get_columns(self, table_name, schema=None):
        """Return information about columns in `table_name`.

        Given a string `table_name` and an optional string `schema`, return
        column information as a list of dicts with these keys:

        name
          the column's name

        type
          [sqlalchemy.types#TypeEngine]

        nullable
          boolean

        default
          the column's default value

        attrs
          dict containing optional column attributes

        """

        col_defs = self.engine.dialect.get_columns(self.conn, table_name,
                                               schema=schema,
                                               info_cache=self.info_cache)
        for col_def in col_defs:
            # make this easy and only return instances for coltype
            coltype = col_def['type']
            if not isinstance(coltype, TypeEngine):
                col_def['type'] = coltype()
        return col_defs

    def get_primary_keys(self, table_name, schema=None):
        """Return information about primary keys in `table_name`.

        Given a string `table_name`, and an optional string `schema`, return 
        primary key information as a list of column names:

        """

        pkeys = self.engine.dialect.get_primary_keys(self.conn, table_name,
                                               schema=schema,
                                               info_cache=self.info_cache)

        return pkeys

    def get_foreign_keys(self, table_name, schema=None):
        """Return information about foreign_keys in `table_name`.

        Given a string `table_name`, and an optional string `schema`, return 
        foreign key information as a list of dicts with these keys:

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

        fk_defs = self.engine.dialect.get_foreign_keys(self.conn, table_name,
                                               schema=schema,
                                               info_cache=self.info_cache)
        for fk_def in fk_defs:
            referred_schema = fk_def['referred_schema']
            # always set the referred_schema.
            if referred_schema is None and schema is None:
                referred_schema = self.engine.dialect.get_default_schema_name(
                                                                    self.conn)
                fk_def['referred_schema'] = referred_schema
        return fk_defs

    def get_indexes(self, table_name, schema=None):
        """Return information about indexes in `table_name`.

        Given a string `table_name` and an optional string `schema`, return
        index information as a list of dicts with these keys:

        name
          the index's name

        column_names
          list of column names in order

        unique
          boolean

        """

        indexes = self.engine.dialect.get_indexes(self.conn, table_name,
                                                  schema=schema,
                                                  info_cache=self.info_cache)
        return indexes


if __name__ == '__main__':
    e = sqlalchemy.create_engine('postgres:///test1')
    insp = Inspector(e)
    print insp.get_table_names()
    print insp.get_columns('customers')
    print insp.get_primary_keys('customers')
    print insp.get_foreign_keys('orders', schema='public')