from sqlalchemy.test.testing import assert_raises, assert_raises_message
import pickle
from sqlalchemy import Integer, String, UniqueConstraint, CheckConstraint, ForeignKey, MetaData, Sequence
from sqlalchemy.test.schema import Table, Column
from sqlalchemy import schema
import sqlalchemy as tsa
from sqlalchemy.test import TestBase, ComparesTables, AssertsCompiledSQL, testing, engines
from sqlalchemy.test.testing import eq_

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
            Column('foo', String(40), nullable=False, server_default='x', server_onupdate='q'),
            Column('bar', String(40), nullable=False, default='y', onupdate='z'),
            Column('description', String(30), CheckConstraint("description='hi'")),
            UniqueConstraint('name'),
            test_needs_fk=True,
        )

        table2 = Table('othertable', meta,
            Column('id', Integer, Sequence('foo_seq'), primary_key=True),
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
            for test, has_constraints, reflect in ((test_to_metadata, True, False), (test_pickle, True, False),(test_pickle_via_reflect, False, True)):
                table_c, table2_c = test()
                self.assert_tables_equal(table, table_c)
                self.assert_tables_equal(table2, table2_c)

                assert table is not table_c
                assert table.primary_key is not table_c.primary_key
                assert list(table2_c.c.myid.foreign_keys)[0].column is table_c.c.myid
                assert list(table2_c.c.myid.foreign_keys)[0].column is not table.c.myid
                assert 'x' in str(table_c.c.foo.server_default.arg)
                
                if not reflect:
                    assert str(table_c.c.foo.server_onupdate.arg) == 'q'
                    assert str(table_c.c.bar.default.arg) == 'y'
                    assert getattr(table_c.c.bar.onupdate.arg, 'arg', table_c.c.bar.onupdate.arg) == 'z'
                    assert isinstance(table2_c.c.id.default, Sequence)
                    
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
    
    def test_tometadata_with_schema(self):
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

        meta2 = MetaData()
        table_c = table.tometadata(meta2, schema='someschema')
        table2_c = table2.tometadata(meta2, schema='someschema')

        eq_(str(table_c.join(table2_c).onclause), str(table_c.c.myid == table2_c.c.myid))
        eq_(str(table_c.join(table2_c).onclause), "someschema.mytable.myid = someschema.othertable.myid")
        
        
    def test_nonexistent(self):
        assert_raises(tsa.exc.NoSuchTableError, Table,
                          'fake_table',
                          MetaData(testing.db), autoload=True)


class TableOptionsTest(TestBase, AssertsCompiledSQL):
    def test_prefixes(self):
        table1 = Table("temporary_table_1", MetaData(),
                      Column("col1", Integer),
                      prefixes = ["TEMPORARY"])
                      
        self.assert_compile(
            schema.CreateTable(table1), 
            "CREATE TEMPORARY TABLE temporary_table_1 (col1 INTEGER)"
        )

        table2 = Table("temporary_table_2", MetaData(),
                      Column("col1", Integer),
                      prefixes = ["VIRTUAL"])
        self.assert_compile(
          schema.CreateTable(table2), 
          "CREATE VIRTUAL TABLE temporary_table_2 (col1 INTEGER)"
        )

    def test_table_info(self):
        metadata = MetaData()
        t1 = Table('foo', metadata, info={'x':'y'})
        t2 = Table('bar', metadata, info={})
        t3 = Table('bat', metadata)
        assert t1.info == {'x':'y'}
        assert t2.info == {}
        assert t3.info == {}
        for t in (t1, t2, t3):
            t.info['bar'] = 'zip'
            assert t.info['bar'] == 'zip'

class ColumnOptionsTest(TestBase):
    def test_column_info(self):
        
        c1 = Column('foo', info={'x':'y'})
        c2 = Column('bar', info={})
        c3 = Column('bat')
        assert c1.info == {'x':'y'}
        assert c2.info == {}
        assert c3.info == {}
        
        for c in (c1, c2, c3):
            c.info['bar'] = 'zip'
            assert c.info['bar'] == 'zip'

