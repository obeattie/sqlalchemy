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
            sa.ForeignKey('%s.engine_users.user_id' % schema)
        )
    else:
        parent_user_id = Column('parent_user_id', sa.Integer,
            sa.ForeignKey('engine_users.user_id')
        )

    users = Table('engine_users', meta,
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
    addresses = Table('engine_email_addresses', meta,
        Column('address_id', sa.Integer, primary_key = True),
        Column('remote_user_id', sa.Integer,
               sa.ForeignKey(users.c.user_id)),
        Column('email_address', sa.String(20)),
        schema=schema,
        test_needs_fk=True,
    )
    return (users, addresses)

class ReflectionTest(TestBase):

    def _test_get_columns(self, schema=None):
        meta = MetaData(testing.db)
        (users, addresses) = createTables(meta, schema)
        meta.create_all()

        try:
            insp = Inspector(meta.bind)
            for table in (users, addresses):
                schema_name = schema
                if schema:
                    # fixme.  issue with case on oracle
                    schema_name = table.schema
                    if testing.against('oracle'):
                        schema_name = table.schema.upper()
                cols = insp.get_columns(table.name, schema=schema_name)
                self.assert_(len(cols) > 0, len(cols))
                # should be in order
                for (i, col) in enumerate(table.columns):
                    self.assertEqual(col.name, cols[i]['name'])
                    # coltype is tricky.  It can be a class or instance.
                    # Also, it may not inherit from col.type while they share
                    # the same base.
                    coltype = cols[i]['coltype']
                    if not hasattr(coltype, '__bases__'):
                        coltype = coltype.__class__
                    self.assert_(
                        issubclass(coltype, col.type.__class__) or \
                        len(
                            set(
                                coltype.__bases__
                            ).intersection(col.type.__class__.__bases__)) > 0
                    ,("%s, %s", (col.type, coltype)))
        finally:
            addresses.drop()
            users.drop()

    def test_get_columns(self):
        self._test_get_columns()

    def test_get_columns_with_schema(self):
        self._test_get_columns(schema=getSchema())

    def _test_get_primary_keys(self, schema=None):
        meta = MetaData(testing.db)
        (users, addresses) = createTables(meta, schema)
        meta.create_all()
        insp = Inspector(meta.bind)
        try:
            users_pkeys = insp.get_primary_keys(users.name, schema=schema)
            self.assertEqual(users_pkeys,  [{'colname':'user_id'}])
            addr_pkeys = insp.get_primary_keys(addresses.name, schema=schema)
            self.assertEqual(addr_pkeys,  [{'colname':'address_id'}])

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
            self.assert_(fkey1['constraint_name'] is not None)
            self.assertEqual(fkey1['referred_schema'], expected_schema)
            self.assertEqual(fkey1['referred_table'], users.name)
            self.assertEqual(fkey1['referred_columns'], ['user_id', ])
            self.assertEqual(fkey1['constrained_columns'], ['parent_user_id'])
            #addresses
            addr_fkeys = insp.get_foreign_keys(addresses.name, schema=schema)
            fkey1 = addr_fkeys[0]
            self.assert_(fkey1['constraint_name'] is not None)
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

if __name__ == "__main__":
    testenv.main()
