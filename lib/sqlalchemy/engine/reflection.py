"""Provides an abstracting for obtaining database schema information.

Notes:

The Inspector currently returns tuples and lists of tuples, but it may be
better to return dicts and lists of dicts.  This would make it easier to add
attributes.


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

    def get_schema_names(self):
        """Return all schema names.

        """
        if hasattr(self.engine.dialect, 'get_schema_names'):
            return self.engine.dialect.get_schema_names(self.conn)
        return []

    def get_table_names(self, schema=None):
        """
        schema:
          Optional, retrieve names from a non-default schema.

        This should probably not return view names or maybe it should return
        them with an indicator t or v.

        """
        if hasattr(self.engine.dialect, 'get_table_names'):
            return self.engine.dialect.get_table_names(self.conn, schema)
        return self.engine.table_names(schema)

    def get_view_names(self, schema=None):
        """
        schema:
          Optional, retrieve names from a non-default schema.

        """
        return self.engine.dialect.get_view_names(self.conn, schema)

    def get_columns(self, table_name, schema=None):
        """Return information about columns in `table_name`.

        Given a string `table_name` and an optional string `schema`, return
        column information as a list of dicts of the form:

        dict(name=name, coltype=coltype, nullable=nullable, colattrs=colattrs)
        
        name
          the column's name

        coltype
          [sqlalchemy.types#TypeEngine]

        nullable
          boolean

        colattrs
          dict containing optional column attributes

        """

        col_defs = self.engine.dialect.get_columns(self.conn, table_name,
                                               schema=schema,
                                               info_cache=self.info_cache)
        cols = []
        for col_def in col_defs:
            # make this easy and only return instances for coltype
            coltype = col_def[1]
            if not isinstance(coltype, TypeEngine):
                coltype = coltype()
            cols.append(
                {'name':col_def[0],
                 'coltype':col_def[1],
                 'nullable':col_def[2],
                 'colattrs':col_def[3],
                }
            )
        return cols

    def get_primary_keys(self, table_name, schema=None):
        """Return information about primary keys in `table_name`.

        Given a string `table_name`, and an optional string `schema`, return 
        primary key information as a list of dicts of the form:

        dict(colname=colname)

        """

        pk_defs = self.engine.dialect.get_primary_keys(self.conn, table_name,
                                               schema=schema,
                                               info_cache=self.info_cache)
        pks = []
        for pk_def in pk_defs:
            pks.append(
                {'colname':pk_def[0]}
            )
        return pks

    def get_foreign_keys(self, table_name, schema=None):
        """Return information about foreign_keys in `table_name`.

        Given a string `table_name`, and an optional string `schema`, return 
        foreign key information as a list of dicts of the form:

        dict(constraint_name=constraint_name,
             constrained_columns=constrained_columns,
             referred_schema=referred_schema,
             referred_table=referred_table, 
             referred_columns=referred_columns)

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
        fks = []
        for fk_def in fk_defs:
            referred_schema = fk_def[2]
            if referred_schema is None and schema is None:
                referred_schema = self.engine.dialect.get_default_schema_name(
                                                                    self.conn)
            fks.append(
                {'constraint_name':fk_def[0],
                 'constrained_columns':fk_def[1],
                 'referred_schema':referred_schema,
                 'referred_table':fk_def[3],
                 'referred_columns':fk_def[4]
                }
            )
        return fks

if __name__ == '__main__':
    e = sqlalchemy.create_engine('postgres:///test1')
    insp = Inspector(e)
    print insp.get_table_names()
    print insp.get_columns('customers')
    print insp.get_primary_keys('customers')
    print insp.get_foreign_keys('orders', schema='public')
