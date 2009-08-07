from sqlalchemy.test.testing import eq_
from sqlalchemy import *
from sqlalchemy.databases import firebird
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql import table, column
from sqlalchemy.test import *


class DomainReflectionTest(TestBase, AssertsExecutionResults):
    "Test Firebird domains"

    __only_on__ = 'firebird'

    @classmethod
    def setup_class(cls):
        con = testing.db.connect()
        try:
            con.execute('CREATE DOMAIN int_domain AS INTEGER DEFAULT 42 NOT NULL')
            con.execute('CREATE DOMAIN str_domain AS VARCHAR(255)')
            con.execute('CREATE DOMAIN rem_domain AS BLOB SUB_TYPE TEXT')
            con.execute('CREATE DOMAIN img_domain AS BLOB SUB_TYPE BINARY')
        except ProgrammingError, e:
            if not "attempt to store duplicate value" in str(e):
                raise e
        con.execute('''CREATE GENERATOR gen_testtable_id''')
        con.execute('''CREATE TABLE testtable (question int_domain,
                                               answer str_domain DEFAULT 'no answer',
                                               remark rem_domain DEFAULT '',
                                               photo img_domain,
                                               d date,
                                               t time,
                                               dt timestamp,
                                               redundant str_domain DEFAULT NULL)''')
        con.execute('''ALTER TABLE testtable
                       ADD CONSTRAINT testtable_pk PRIMARY KEY (question)''')
        con.execute('''CREATE TRIGGER testtable_autoid FOR testtable
                       ACTIVE BEFORE INSERT AS
                       BEGIN
                         IF (NEW.question IS NULL) THEN
                           NEW.question = gen_id(gen_testtable_id, 1);
                       END''')

    @classmethod
    def teardown_class(cls):
        con = testing.db.connect()
        con.execute('DROP TABLE testtable')
        con.execute('DROP DOMAIN int_domain')
        con.execute('DROP DOMAIN str_domain')
        con.execute('DROP DOMAIN rem_domain')
        con.execute('DROP DOMAIN img_domain')
        con.execute('DROP GENERATOR gen_testtable_id')

    def test_table_is_reflected(self):
        from sqlalchemy.types import Integer, Text, Binary, String, Date, Time, DateTime
        metadata = MetaData(testing.db)
        table = Table('testtable', metadata, autoload=True)
        eq_(set(table.columns.keys()),
            set(['question', 'answer', 'remark',
                 'photo', 'd', 't', 'dt', 'redundant']),
            "Columns of reflected table didn't equal expected columns")
        eq_(table.c.question.primary_key, True)
        eq_(table.c.question.sequence.name, 'gen_testtable_id')
        assert isinstance(table.c.question.type, Integer)
        eq_(table.c.question.server_default.arg.text, "42")
        assert isinstance(table.c.answer.type, String)
        assert table.c.answer.type.length == 255
        eq_(table.c.answer.server_default.arg.text, "'no answer'")
        assert isinstance(table.c.remark.type, Text)
        eq_(table.c.remark.server_default.arg.text, "''")
        assert isinstance(table.c.photo.type, Binary)
        assert table.c.redundant.server_default is None
        # The following assume a Dialect 3 database
        assert isinstance(table.c.d.type, Date)
        assert isinstance(table.c.t.type, Time)
        assert isinstance(table.c.dt.type, DateTime)


class CompileTest(TestBase, AssertsCompiledSQL):
    __dialect__ = firebird.FBDialect()

    def test_alias(self):
        t = table('sometable', column('col1'), column('col2'))
        s = select([t.alias()])
        self.assert_compile(s, "SELECT sometable_1.col1, sometable_1.col2 FROM sometable AS sometable_1")

        dialect = firebird.FBDialect()
        dialect._version_two = False
        self.assert_compile(s, "SELECT sometable_1.col1, sometable_1.col2 FROM sometable sometable_1",
            dialect = dialect
        )

    def test_function(self):
        self.assert_compile(func.foo(1, 2), "foo(:foo_1, :foo_2)")
        self.assert_compile(func.current_time(), "CURRENT_TIME")
        self.assert_compile(func.foo(), "foo")

        m = MetaData()
        t = Table('sometable', m, Column('col1', Integer), Column('col2', Integer))
        self.assert_compile(select([func.max(t.c.col1)]), "SELECT max(sometable.col1) AS max_1 FROM sometable")

    def test_substring(self):
        self.assert_compile(func.substring('abc', 1, 2), "SUBSTRING(:substring_1 FROM :substring_2 FOR :substring_3)")
        self.assert_compile(func.substring('abc', 1), "SUBSTRING(:substring_1 FROM :substring_2)")

    def test_update_returning(self):
        table1 = table('mytable',
            column('myid', Integer),
            column('name', String(128)),
            column('description', String(128)),
        )

        u = update(table1, values=dict(name='foo')).returning(table1.c.myid, table1.c.name)
        self.assert_compile(u, "UPDATE mytable SET name=:name RETURNING mytable.myid, mytable.name")

        u = update(table1, values=dict(name='foo')).returning(table1)
        self.assert_compile(u, "UPDATE mytable SET name=:name "\
            "RETURNING mytable.myid, mytable.name, mytable.description")

        u = update(table1, values=dict(name='foo')).returning(func.length(table1.c.name))
        self.assert_compile(u, "UPDATE mytable SET name=:name RETURNING char_length(mytable.name) AS length_1")

    def test_insert_returning(self):
        table1 = table('mytable',
            column('myid', Integer),
            column('name', String(128)),
            column('description', String(128)),
        )

        i = insert(table1, values=dict(name='foo')).returning(table1.c.myid, table1.c.name)
        self.assert_compile(i, "INSERT INTO mytable (name) VALUES (:name) RETURNING mytable.myid, mytable.name")

        i = insert(table1, values=dict(name='foo')).returning(table1)
        self.assert_compile(i, "INSERT INTO mytable (name) VALUES (:name) "\
            "RETURNING mytable.myid, mytable.name, mytable.description")

        i = insert(table1, values=dict(name='foo')).returning(func.length(table1.c.name))
        self.assert_compile(i, "INSERT INTO mytable (name) VALUES (:name) RETURNING char_length(mytable.name) AS length_1")




class MiscTest(TestBase):
    __only_on__ = 'firebird'

    def test_strlen(self):
        # On FB the length() function is implemented by an external
        # UDF, strlen().  Various SA tests fail because they pass a
        # parameter to it, and that does not work (it always results
        # the maximum string length the UDF was declared to accept).
        # This test checks that at least it works ok in other cases.

        meta = MetaData(testing.db)
        t = Table('t1', meta,
            Column('id', Integer, Sequence('t1idseq'), primary_key=True),
            Column('name', String(10))
        )
        meta.create_all()
        try:
            t.insert(values=dict(name='dante')).execute()
            t.insert(values=dict(name='alighieri')).execute()
            select([func.count(t.c.id)],func.length(t.c.name)==5).execute().first()[0] == 1
        finally:
            meta.drop_all()

    def test_server_version_info(self):
        version = testing.db.dialect.server_version_info
        assert len(version) == 3, "Got strange version info: %s" % repr(version)

    def test_percents_in_text(self):
        for expr, result in (
            (text("select '%' from rdb$database"), '%'),
            (text("select '%%' from rdb$database"), '%%'),
            (text("select '%%%' from rdb$database"), '%%%'),
            (text("select 'hello % world' from rdb$database"), "hello % world")
        ):
            eq_(testing.db.scalar(expr), result)
