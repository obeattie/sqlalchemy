from testbase import PersistTest
import sqlalchemy.engine.strategies as strategies
import unittest
        
class ParseConnectTest(PersistTest):
    def testrfc1738(self):
        for url in (
            'dbtype://username:password@hostspec:110//usr/db_file.db',
            'dbtype://username:password@hostspec/database',
            'dbtype://username:password@hostspec',
            'dbtype://username:password@/database',
            'dbtype://username@hostspec',
            'dbtype://username:password@127.0.0.1:1521',
            'dbtype://hostspec/database',
            'dbtype://hostspec',
            'dbtype:///database',
            'dbtype:///:memory:'
        ):
            (name, opts) = strategies._parse_rfc1738_args(url, {})
            # TODO: assertion conditions
            print name, opts

    def testurl(self):
        for url in (
            'dbtype://username=user&password=pw&host=host&port=1234&db=foo',
        ):
            #foo = strategies._parse_rfc1738_args(url, {})
            #assert foo is None
            (name, opts) = strategies._parse_keyvalue_args(url, {})
            # TODO: assertion conditions
            print name, opts
            
if __name__ == "__main__":
    unittest.main()
        