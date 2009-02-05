"""tests for sqlalchemy.engine.reflection

"""

import testenv; testenv.configure_for_tests()
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector
from testlib.sa import MetaData, Table, Column
from testlib import TestBase, testing, engines

if 'set' not in dir(__builtins__):
    from sets import Set as set

def getSchema():
    if testing.against('oracle'):
        return 'scott'
    else:
        return 'test_schema'

def createTables(meta, schema=None):
    if schema:
        parent_user_id = Column('parent_user_id', sa.Integer,
            sa.ForeignKey('%s.users.user_id' % schema)
        )
    else:
        parent_user_id = Column('parent_user_id', sa.Integer,
            sa.ForeignKey('users.user_id')
        )

    users = Table('users', meta,
        Column('user_id', sa.INT, primary_key=True),
        Column('user_name', sa.VARCHAR(20), nullable=False),
        Column('test1', sa.CHAR(5), nullable=False),
        Column('test2', sa.Float(5), nullable=False),
        Column('test3', sa.Text),
        Column('test4', sa.Numeric, nullable = False),
        Column('test5', sa.DateTime),
        parent_user_id,
        Column('test6', sa.DateTime, nullable=False),
        Column('test7', sa.Text),
        Column('test8', sa.Binary),
        Column('test_passivedefault2', sa.Integer, server_default='5'),
        Column('test9', sa.Binary(100)),
        Column('test_numeric', sa.Numeric()),
        schema=schema,
        test_needs_fk=True,
    )
    addresses = Table('email_addresses', meta,
        Column('address_id', sa.Integer, primary_key = True),
        Column('remote_user_id', sa.Integer,
               sa.ForeignKey(users.c.user_id)),
        Column('email_address', sa.String(20)),
        schema=schema,
        test_needs_fk=True,
    )
    return (users, addresses)

def createIndexes(con, schema=None):
    fullname = 'users'
    if schema:
        fullname = "%s.%s" % (schema, 'users')
    query = "CREATE INDEX users_t_idx ON %s (test1, test2)" % fullname
    con.execute(sa.sql.text(query))

def createViews(con, schema=None):
    for table_name in ('users', 'email_addresses'):
        fullname = table_name
        if schema:
            fullname = "%s.%s" % (schema, table_name)
        view_name = fullname + '_v'
        query = "CREATE OR REPLACE VIEW %s AS SELECT * FROM %s" % (view_name,
                                                                   fullname)
        con.execute(sa.sql.text(query))

def dropViews(con, schema=None):
    for table_name in ('email_addresses', 'users'):
        fullname = table_name
        if schema:
            fullname = "%s.%s" % (schema, table_name)
        view_name = fullname + '_v'
        query = "DROP VIEW %s" % view_name
        con.execute(sa.sql.text(query))


class ReflectionTest(TestBase):

    def test_get_schema_names(self):
        meta = MetaData(testing.db)
        insp = Inspector(meta.bind)
        self.assert_(getSchema() in insp.get_schema_names())

    def _test_get_table_names(self, schema=None, table_type='table'):
        meta = MetaData(testing.db)
        (users, addresses) = createTables(meta, schema)
        meta.create_all()
        createViews(meta.bind, schema)
        try:
            insp = Inspector(meta.bind)
            if table_type == 'view':
                table_names = insp.get_view_names(schema)
                table_names.sort()
                answer = ['email_addresses_v', 'users_v']
            else:
                table_names = insp.get_table_names(schema)
                table_names.sort()
                answer = ['email_addresses', 'users']
            self.assertEqual(table_names, answer)
        finally:
            dropViews(meta.bind, schema)
            addresses.drop()
            users.drop()

    def test_get_table_names(self):
        self._test_get_table_names()

    def test_get_table_names_with_schema(self):
        self._test_get_table_names(getSchema())

    def test_get_view_names(self):
        self._test_get_table_names(table_type='view')

    def test_get_view_names_with_schema(self):
        self._test_get_table_names(getSchema(), table_type='view')

    def _test_get_columns(self, schema=None, table_type='table'):
        meta = MetaData(testing.db)
        (users, addresses) = createTables(meta, schema)
        table_names = ['users', 'email_addresses']
        meta.create_all()
        if table_type == 'view':
            createViews(meta.bind, schema)
            table_names = ['users_v', 'email_addresses_v']
        try:
            insp = Inspector(meta.bind)
            for (table_name, table) in zip(table_names, (users, addresses)):
                schema_name = schema
                if schema and testing.against('oracle'):
                    schema_name = schema.upper()
                cols = insp.get_columns(table_name, schema=schema_name)
                self.assert_(len(cols) > 0, len(cols))
                # should be in order
                for (i, col) in enumerate(table.columns):
                    self.assertEqual(col.name, cols[i]['name'])
                    # coltype is tricky
                    # It may not inherit from col.type while they share
                    # the same base.
                    coltype = cols[i]['type'].__class__
                    self.assert_(
                        issubclass(coltype, col.type.__class__) or \
                        len(
                            set(
                                coltype.__bases__
                            ).intersection(col.type.__class__.__bases__)) > 0
                    ,("%s, %s", (col.type, coltype)))
        finally:
            if table_type == 'view':
                dropViews(meta.bind, schema)
            addresses.drop()
            users.drop()

    def test_get_columns(self):
        self._test_get_columns()

    def test_get_columns_with_schema(self):
        self._test_get_columns(schema=getSchema())

    def test_get_view_columns(self):
        self._test_get_columns(table_type='view')

    def test_get_view_columns_with_schema(self):
        self._test_get_columns(schema=getSchema(), table_type='view')

    def _test_get_primary_keys(self, schema=None):
        meta = MetaData(testing.db)
        (users, addresses) = createTables(meta, schema)
        meta.create_all()
        insp = Inspector(meta.bind)
        try:
            users_pkeys = insp.get_primary_keys(users.name, schema=schema)
            self.assertEqual(users_pkeys,  ['user_id'])
            addr_pkeys = insp.get_primary_keys(addresses.name, schema=schema)
            self.assertEqual(addr_pkeys,  ['address_id'])

        finally:
            addresses.drop()
            users.drop()

    def test_get_primary_keys(self):
        self._test_get_primary_keys()

    def test_get_primary_keys_with_schema(self):
        self._test_get_primary_keys(schema=getSchema())

    def _test_get_foreign_keys(self, schema=None):
        meta = MetaData(testing.db)
        (users, addresses) = createTables(meta, schema)
        meta.create_all()
        insp = Inspector(meta.bind)
        try:
            expected_schema = schema
            if schema is None:
                expected_schema = meta.bind.dialect.get_default_schema_name(
                                    meta.bind)
            # users
            users_fkeys = insp.get_foreign_keys(users.name, schema=schema)
            fkey1 = users_fkeys[0]
            self.assert_(fkey1['name'] is not None)
            self.assertEqual(fkey1['referred_schema'], expected_schema)
            self.assertEqual(fkey1['referred_table'], users.name)
            self.assertEqual(fkey1['referred_columns'], ['user_id', ])
            self.assertEqual(fkey1['constrained_columns'], ['parent_user_id'])
            #addresses
            addr_fkeys = insp.get_foreign_keys(addresses.name, schema=schema)
            fkey1 = addr_fkeys[0]
            self.assert_(fkey1['name'] is not None)
            self.assertEqual(fkey1['referred_schema'], expected_schema)
            self.assertEqual(fkey1['referred_table'], users.name)
            self.assertEqual(fkey1['referred_columns'], ['user_id', ])
            self.assertEqual(fkey1['constrained_columns'], ['remote_user_id'])
        finally:
            addresses.drop()
            users.drop()

    def test_get_foreign_keys(self):
        self._test_get_foreign_keys()

    def test_get_foreign_keys_with_schema(self):
        self._test_get_foreign_keys(schema=getSchema())

    def _test_get_indexes(self, schema=None):
        meta = MetaData(testing.db)
        (users, addresses) = createTables(meta, schema)
        meta.create_all()
        createIndexes(meta.bind, schema)
        try:
            insp = Inspector(meta.bind)
            indexes = insp.get_indexes('users', schema=schema)
            indexes.sort()
            expected_indexes = [
                {'unique': False,
                 'column_names': ['test1', 'test2'],
                 'name': 'users_t_idx'}]
            self.assertEqual(indexes, expected_indexes)
        finally:
            addresses.drop()
            users.drop()

    def test_get_indexes(self):
        self._test_get_indexes()

    def test_get_indexes_with_schema(self):
        self._test_get_indexes(schema=getSchema())

    def _test_get_view_definition(self, schema=None):
        meta = MetaData(testing.db)
        (users, addresses) = createTables(meta, schema)
        meta.create_all()
        createViews(meta.bind, schema)
        view_name1 = 'users_v'
        view_name2 = 'email_addresses_v'
        try:
            insp = Inspector(meta.bind)
            v1 = insp.get_view_definition(view_name1, schema=schema)
            self.assert_(v1)
            v2 = insp.get_view_definition(view_name2, schema=schema)
            self.assert_(v2)
        finally:
            dropViews(meta.bind, schema)
            addresses.drop()
            users.drop()

    def test_get_view_definition(self):
        self._test_get_view_definition()

    def test_get_view_definition_with_schema(self):
        self._test_get_view_definition(schema=getSchema())

if __name__ == "__main__":
    testenv.main()