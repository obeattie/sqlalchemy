"""TestCase and TestSuite artifacts and testing decorators."""

# monkeypatches unittest.TestLoader.suiteClass at import time

import unittest, re, sys, os
from cStringIO import StringIO
from sqlalchemy import MetaData, sql
from sqlalchemy.orm import clear_mappers
import testlib.config as config

__all__ = 'PersistTest', 'AssertMixin', 'ORMTest'

def unsupported(*dbs):
    """Mark a test as unsupported by one or more database implementations"""
    
    def decorate(fn):
        fn_name = fn.__name__
        def maybe(*args, **kw):
            if config.db.name in dbs:
                print "'%s' unsupported on DB implementation '%s'" % (
                    fn_name, config.db.name)
                return True
            else:
                return fn(*args, **kw)
        try:
            maybe.__name__ = fn_name
        except:
            pass
        return maybe
    return decorate

def supported(*dbs):
    """Mark a test as supported by one or more database implementations"""
    
    def decorate(fn):
        fn_name = fn.__name__
        def maybe(*args, **kw):
            if config.db.name in dbs:
                return fn(*args, **kw)
            else:
                print "'%s' unsupported on DB implementation '%s'" % (
                    fn_name, config.db.name)
                return True
        try:
            maybe.__name__ = fn_name
        except:
            pass
        return maybe
    return decorate

class TestData(object):
    """Tracks SQL expressions as they are executed via an instrumented ExecutionContext."""
    
    def __init__(self):
        self.set_assert_list(None, None)
        self.sql_count = 0
        self.buffer = None
        
    def set_assert_list(self, unittest, list):
        self.unittest = unittest
        self.assert_list = list
        if list is not None:
            self.assert_list.reverse()

testdata = TestData()


class ExecutionContextWrapper(object):
    """instruments the ExecutionContext created by the Engine so that SQL expressions
    can be tracked."""
    
    def __init__(self, ctx):
        self.__dict__['ctx'] = ctx
    def __getattr__(self, key):
        return getattr(self.ctx, key)
    def __setattr__(self, key, value):
        setattr(self.ctx, key, value)
        
    def post_execution(self):
        ctx = self.ctx
        statement = unicode(ctx.compiled)
        statement = re.sub(r'\n', '', ctx.statement)
        if testdata.buffer is not None:
            testdata.buffer.write(statement + "\n")

        if testdata.assert_list is not None:
            assert len(testdata.assert_list), "Received query but no more assertions: %s" % statement
            item = testdata.assert_list[-1]
            if not isinstance(item, dict):
                item = testdata.assert_list.pop()
            else:
                # asserting a dictionary of statements->parameters
                # this is to specify query assertions where the queries can be in 
                # multiple orderings
                if not item.has_key('_converted'):
                    for key in item.keys():
                        ckey = self.convert_statement(key)
                        item[ckey] = item[key]
                        if ckey != key:
                            del item[key]
                    item['_converted'] = True
                try:
                    entry = item.pop(statement)
                    if len(item) == 1:
                        testdata.assert_list.pop()
                    item = (statement, entry)
                except KeyError:
                    assert False, "Testing for one of the following queries: %s, received '%s'" % (repr([k for k in item.keys()]), statement)

            (query, params) = item
            if callable(params):
                params = params(ctx)
            if params is not None and isinstance(params, list) and len(params) == 1:
                params = params[0]
            
            if isinstance(ctx.compiled_parameters, sql.ClauseParameters):
                parameters = ctx.compiled_parameters.get_original_dict()
            elif isinstance(ctx.compiled_parameters, list):
                parameters = [p.get_original_dict() for p in ctx.compiled_parameters]
                    
            query = self.convert_statement(query)
            if config.db.name == 'mssql' and statement.endswith('; select scope_identity()'):
                statement = statement[:-25]
            testdata.unittest.assert_(statement == query and (params is None or params == parameters), "Testing for query '%s' params %s, received '%s' with params %s" % (query, repr(params), statement, repr(parameters)))
        testdata.sql_count += 1
        self.ctx.post_execution()
        
    def convert_statement(self, query):
        paramstyle = self.ctx.dialect.paramstyle
        if paramstyle == 'named':
            pass
        elif paramstyle =='pyformat':
            query = re.sub(r':([\w_]+)', r"%(\1)s", query)
        else:
            # positional params
            repl = None
            if paramstyle=='qmark':
                repl = "?"
            elif paramstyle=='format':
                repl = r"%s"
            elif paramstyle=='numeric':
                repl = None
            query = re.sub(r':([\w_]+)', repl, query)
        return query

class PersistTest(unittest.TestCase):

    def __init__(self, *args, **params):
        unittest.TestCase.__init__(self, *args, **params)

    def setUpAll(self):
        pass

    def tearDownAll(self):
        pass

    def shortDescription(self):
        """overridden to not return docstrings"""
        return None

class AssertMixin(PersistTest):
    """given a list-based structure of keys/properties which represent information within an object structure, and
    a list of actual objects, asserts that the list of objects corresponds to the structure."""
    
    def assert_result(self, result, class_, *objects):
        result = list(result)
        print repr(result)
        self.assert_list(result, class_, objects)
        
    def assert_list(self, result, class_, list):
        self.assert_(len(result) == len(list),
                     "result list is not the same size as test list, " +
                     "for class " + class_.__name__)
        for i in range(0, len(list)):
            self.assert_row(class_, result[i], list[i])
            
    def assert_row(self, class_, rowobj, desc):
        self.assert_(rowobj.__class__ is class_,
                     "item class is not " + repr(class_))
        for key, value in desc.iteritems():
            if isinstance(value, tuple):
                if isinstance(value[1], list):
                    self.assert_list(getattr(rowobj, key), value[0], value[1])
                else:
                    self.assert_row(value[0], getattr(rowobj, key), value[1])
            else:
                self.assert_(getattr(rowobj, key) == value,
                             "attribute %s value %s does not match %s" % (
                             key, getattr(rowobj, key), value))
                
    def assert_sql(self, db, callable_, list, with_sequences=None):
        global testdata
        testdata = TestData()
        if with_sequences is not None and (config.db.name == 'postgres' or
                                           config.db.name == 'oracle'):
            testdata.set_assert_list(self, with_sequences)
        else:
            testdata.set_assert_list(self, list)
        try:
            callable_()
        finally:
            testdata.set_assert_list(None, None)

    def assert_sql_count(self, db, callable_, count):
        global testdata
        testdata = TestData()
        try:
            callable_()
        finally:
            self.assert_(testdata.sql_count == count,
                         "desired statement count %d does not match %d" % (
                         count, testdata.sql_count))

    def capture_sql(self, db, callable_):
        global testdata
        testdata = TestData()
        buffer = StringIO()
        testdata.buffer = buffer
        try:
            callable_()
            return buffer.getvalue()
        finally:
            testdata.buffer = None

_otest_metadata = None
class ORMTest(AssertMixin):
    keep_mappers = False
    keep_data = False

    def setUpAll(self):
        global _otest_metadata
        _otest_metadata = MetaData(config.db)
        self.define_tables(_otest_metadata)
        _otest_metadata.create_all()
        self.insert_data()

    def define_tables(self, _otest_metadata):
        raise NotImplementedError()

    def insert_data(self):
        pass

    def get_metadata(self):
        return _otest_metadata

    def tearDownAll(self):
        clear_mappers()
        _otest_metadata.drop_all()

    def tearDown(self):
        if not self.keep_mappers:
            clear_mappers()
        if not self.keep_data:
            for t in _otest_metadata.table_iterator(reverse=True):
                t.delete().execute().close()


class TTestSuite(unittest.TestSuite):
    """A TestSuite with once per TestCase setUpAll() and tearDownAll()"""

    def __init__(self, tests=()):
        if len(tests) >0 and isinstance(tests[0], PersistTest):
            self._initTest = tests[0]
        else:
            self._initTest = None
        unittest.TestSuite.__init__(self, tests)

    def do_run(self, result):
        # nice job unittest !  you switched __call__ and run() between py2.3
        # and 2.4 thereby making straight subclassing impossible !
        for test in self._tests:
            if result.shouldStop:
                break
            test(result)
        return result

    def run(self, result):
        return self(result)

    def __call__(self, result):
        try:
            if self._initTest is not None:
                self._initTest.setUpAll()
        except:
            result.addError(self._initTest, self.__exc_info())
            pass
        try:
            return self.do_run(result)
        finally:
            try:
                if self._initTest is not None:
                    self._initTest.tearDownAll()
            except:
                result.addError(self._initTest, self.__exc_info())
                pass

    def __exc_info(self):
        """Return a version of sys.exc_info() with the traceback frame
           minimised; usually the top level of the traceback frame is not
           needed.
           ripped off out of unittest module since its double __
        """
        exctype, excvalue, tb = sys.exc_info()
        if sys.platform[:4] == 'java': ## tracebacks look different in Jython
            return (exctype, excvalue, tb)
        return (exctype, excvalue, tb)

unittest.TestLoader.suiteClass = TTestSuite

def _iter_covered_files():
    import sqlalchemy
    for rec in os.walk(os.path.dirname(sqlalchemy.__file__)):
        for x in rec[2]:
            if x.endswith('.py'):
                yield os.path.join(rec[0], x)

def cover(callable_, file_=None):
    from testlib import coverage
    coverage_client = coverage.the_coverage
    coverage_client.get_ready()
    coverage_client.exclude('#pragma[: ]+[nN][oO] [cC][oO][vV][eE][rR]')
    coverage_client.erase()
    coverage_client.start()
    try:
        return callable_()
    finally:
        coverage_client.stop()
        coverage_client.save()
        coverage_client.report(list(_iter_covered_files()),
                               show_missing=False, ignore_errors=False,
                               file=file_)

class DevNullWriter(object):
    def write(self, msg):
        pass
    def flush(self):
        pass

def runTests(suite):
    verbose = config.options.verbose
    quiet = config.options.quiet
    orig_stdout = sys.stdout

    try:
        if not verbose or quiet:
            sys.stdout = DevNullWriter()
        runner = unittest.TextTestRunner(verbosity = quiet and 1 or 2)
        return runner.run(suite)
    finally:
        if not verbose or quiet:
            sys.stdout = orig_stdout

def main(suite=None):
    if not suite:
        if len(sys.argv[1:]):
            suite =unittest.TestLoader().loadTestsFromNames(
                sys.argv[1:], __import__('__main__'))
        else:
            suite = unittest.TestLoader().loadTestsFromModule(
                __import__('__main__'))

    result = runTests(suite)
    sys.exit(not result.wasSuccessful())
