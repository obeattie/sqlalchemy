import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from sqlalchemy import sql
from sqlalchemy.databases import sybase
from testlib import *


class CompileTest(TestBase, AssertsCompiledSQL):
    __dialect__ = sybase.dialect()

    def test_extract(self):
        t = sql.table('t', sql.column('col1'))

        mapping = {
            'day': 'day',
            'doy': 'dayofyear',
            'dow': 'weekday',
            'milliseconds': 'millisecond',
            'millisecond': 'millisecond',
            'year': 'year',
            }

        for field, subst in mapping.items():
            self.assert_compile(
                select([extract(field, t.c.col1)]),
                'SELECT DATEPART("%s", t.col1) AS anon_1 FROM t' % subst)



if __name__ == "__main__":
    testenv.main()
