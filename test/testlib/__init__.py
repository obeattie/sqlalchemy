"""Enhance unittest and instrument SQLAlchemy classes for testing.

Load after sqlalchemy imports to use instrumented stand-ins like Table.
"""

import sys
import testlib.config
from testlib.schema import Table, Column
from testlib.orm import mapper
import testlib.testing as testing
from testlib.testing import rowset
from testlib.testing import TestBase, AssertsExecutionResults, ORMTest, AssertsCompiledSQL, ComparesTables
import testlib.profiling as profiling
import testlib.engines as engines
from testlib.compat import set, frozenset, sorted, _function_named


__all__ = ('testing',
           'mapper',
           'Table', 'Column',
           'rowset',
           'TestBase', 'AssertsExecutionResults', 'ORMTest',
           'AssertsCompiledSQL', 'ComparesTables',
           'profiling', 'engines',
           'set', 'frozenset', 'sorted', '_function_named')


sys.modules['testlib.sa'] = sa = testing.CompositeModule(
    'testlib.sa', 'sqlalchemy', 'testlib.schema', orm=testing.CompositeModule(
    'testlib.sa.orm', 'sqlalchemy.orm', 'testlib.orm'))
sys.modules['testlib.sa.orm'] = sa.orm
