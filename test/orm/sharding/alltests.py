import testbase
import unittest

import orm.inheritance.alltests as inheritance

def suite():
    modules_to_test = (
        'orm.sharding.shard',
        )
    alltests = unittest.TestSuite()
    for name in modules_to_test:
        mod = __import__(name)
        for token in name.split('.')[1:]:
            mod = getattr(mod, token)
        alltests.addTest(unittest.findTestCases(mod, suiteClass=None))
    alltests.addTest(inheritance.suite())
    return alltests


if __name__ == '__main__':
    testbase.main(suite())
