import testenv; testenv.configure_for_tests()
import pickle
from sqlalchemy import MetaData, create_engine
from testlib.sa import Table, Column, Integer, String, UniqueConstraint, \
     CheckConstraint, ForeignKey
import testlib.sa as tsa
from testlib import TestBase, ComparesTables, testing


class MetaDataTest(TestBase, ComparesTables):
    def test_metadata_connect(self):
        metadata = MetaData()
        t1 = Table('table1', metadata, Column('col1', Integer, primary_key=True),
            Column('col2', String(20)))
        metadata.bind = testing.db
        metadata.create_all()
        try:
            assert t1.count().scalar() == 0
        finally:
            metadata.drop_all()


    def test_dupe_tables(self):
        metadata = MetaData()
        t1 = Table('table1', metadata, Column('col1', Integer, primary_key=True),
            Column('col2', String(20)))

        metadata.bind = testing.db
        metadata.create_all()
        try:
            try:
                t1 = Table('table1', metadata, autoload=True)
                t2 = Table('table1', metadata, Column('col1', Integer, primary_key=True),
                    Column('col2', String(20)))
                assert False
            except tsa.exc.InvalidRequestError, e:
                assert str(e) == "Table 'table1' is already defined for this MetaData instance.  Specify 'useexisting=True' to redefine options and columns on an existing Table object."
        finally:
            metadata.drop_all()

    @testing.exclude('mysql', '<', (4, 1, 1), 'early types are squirrely')
    def test_to_metadata(self):
        meta = MetaData()

        table = Table('mytable', meta,
            Column('myid', Integer, primary_key=True),
            Column('name', String(40), nullable=True),
            Column('description', String(30), CheckConstraint("description='hi'")),
            UniqueConstraint('name'),
            test_needs_fk=True,
        )

        table2 = Table('othertable', meta,
            Column('id', Integer, primary_key=True),
            Column('myid', Integer, ForeignKey('mytable.myid')),
            test_needs_fk=True,
            )

        def test_to_metadata():
            meta2 = MetaData()
            table_c = table.tometadata(meta2)
            table2_c = table2.tometadata(meta2)
            return (table_c, table2_c)

        def test_pickle():
            meta.bind = testing.db
            meta2 = pickle.loads(pickle.dumps(meta))
            assert meta2.bind is None
            meta3 = pickle.loads(pickle.dumps(meta2))
            return (meta2.tables['mytable'], meta2.tables['othertable'])

        def test_pickle_via_reflect():
            # this is the most common use case, pickling the results of a
            # database reflection
            meta2 = MetaData(bind=testing.db)
            t1 = Table('mytable', meta2, autoload=True)
            t2 = Table('othertable', meta2, autoload=True)
            meta3 = pickle.loads(pickle.dumps(meta2))
            assert meta3.bind is None
            assert meta3.tables['mytable'] is not t1
            return (meta3.tables['mytable'], meta3.tables['othertable'])

        meta.create_all(testing.db)
        try:
            for test, has_constraints in ((test_to_metadata, True), (test_pickle, True), (test_pickle_via_reflect, False)):
                table_c, table2_c = test()
                self.assert_tables_equal(table, table_c)
                self.assert_tables_equal(table2, table2_c)

                assert table is not table_c
                assert table.primary_key is not table_c.primary_key
                assert list(table2_c.c.myid.foreign_keys)[0].column is table_c.c.myid
                assert list(table2_c.c.myid.foreign_keys)[0].column is not table.c.myid

                # constraints dont get reflected for any dialect right now
                if has_constraints:
                    for c in table_c.c.description.constraints:
                        if isinstance(c, CheckConstraint):
                            break
                    else:
                        assert False
                    assert c.sqltext=="description='hi'"

                    for c in table_c.constraints:
                        if isinstance(c, UniqueConstraint):
                            break
                    else:
                        assert False
                    assert c.columns.contains_column(table_c.c.name)
                    assert not c.columns.contains_column(table.c.name)
        finally:
            meta.drop_all(testing.db)

    def test_nonexistent(self):
        self.assertRaises(tsa.exc.NoSuchTableError, Table,
                          'fake_table',
                          MetaData(testing.db), autoload=True)


class TableOptionsTest(TestBase):
    def mock_engine(self):
        buffer = []
        def executor(sql, *a, **kw):
            buffer.append(sql)
        engine = create_engine(testing.db.name + '://',
                               strategy='mock', executor=executor)
        assert not hasattr(engine, 'mock')
        engine.mock = buffer
        return engine

    def setUp(self):
        self.engine = self.mock_engine()
        self.metadata = MetaData(self.engine)

    def test_prefixes(self):
        table1 = Table("temporary_table_1", self.metadata,
                      Column("col1", Integer),
                      prefixes = ["TEMPORARY"])
        table1.create()
        assert [str(x) for x in self.engine.mock if 'CREATE TEMPORARY TABLE' in str(x)]
        del self.engine.mock[:]
        table2 = Table("temporary_table_2", self.metadata,
                      Column("col1", Integer),
                      prefixes = ["VIRTUAL"])
        table2.create()
        assert [str(x) for x in self.engine.mock if 'CREATE VIRTUAL TABLE' in str(x)]

if __name__ == '__main__':
    testenv.main()
