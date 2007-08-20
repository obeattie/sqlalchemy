import testbase
from sqlalchemy import *
from testlib import *


class FoundRowsTest(AssertMixin):
    """tests rowcount functionality"""
    def setUpAll(self):
        metadata = MetaData(testbase.db)

        global employees_table

        employees_table = Table('employees', metadata,
            Column('employee_id', Integer, Sequence('employee_id_seq', optional=True), primary_key=True),
            Column('name', String(50)),
            Column('department', String(1)),
        )
        employees_table.create()

    def setUp(self):
        global data
        data = [ ('Angela', 'A'),
                 ('Andrew', 'A'),
                 ('Anand', 'A'),
                 ('Bob', 'B'),
                 ('Bobette', 'B'),
                 ('Buffy', 'B'),
                 ('Charlie', 'C'),
                 ('Cynthia', 'C'),
                 ('Chris', 'C') ]

        i = employees_table.insert()
        i.execute(*[{'name':n, 'department':d} for n, d in data])
    def tearDown(self):
        employees_table.delete().execute()

    def tearDownAll(self):
        employees_table.drop()

    def testbasic(self):
        s = employees_table.select()
        r = s.execute().fetchall()

        assert len(r) == len(data)

    def test_update_rowcount1(self):
        # WHERE matches 3, 3 rows changed
        department = employees_table.c.department
        r = employees_table.update(department=='C').execute(department='Z')
        if testbase.db.dialect.supports_sane_rowcount:
            assert r.rowcount == 3

    def test_update_rowcount2(self):
        # WHERE matches 3, 0 rows changed
        department = employees_table.c.department
        r = employees_table.update(department=='C').execute(department='C')
        if testbase.db.dialect.supports_sane_rowcount:
            assert r.rowcount == 3

    def test_delete_rowcount(self):
        # WHERE matches 3, 3 rows deleted
        department = employees_table.c.department
        r = employees_table.delete(department=='C').execute()
        if testbase.db.dialect.supports_sane_rowcount:
            assert r.rowcount == 3

if __name__ == '__main__':
    testbase.main()




