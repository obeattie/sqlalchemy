import testenv; testenv.configure_for_tests()
from sqlalchemy.orm import interfaces, util
from testlib import *
from testlib import fixtures


class ExtensionCarrierTest(TestBase):
    def test_basic(self):
        carrier = util.ExtensionCarrier()

        assert 'get_session' not in carrier.methods
        assert carrier.get_session() is interfaces.EXT_CONTINUE
        assert 'get_session' not in carrier.methods

        self.assertRaises(AttributeError, lambda: carrier.snickysnack)

        class Partial(object):
            def __init__(self, marker):
                self.marker = marker
            def get_session(self):
                return self.marker

        carrier.append(Partial('end'))
        assert 'get_session' in carrier.methods
        assert carrier.get_session() == 'end'

        carrier.push(Partial('front'))
        assert carrier.get_session() == 'front'

        assert 'populate_instance' not in carrier.methods
        carrier.append(interfaces.MapperExtension)
        assert 'populate_instance' in carrier.methods

        assert carrier.interface
        for m in carrier.interface:
            assert getattr(interfaces.MapperExtension, m)


if __name__ == "__main__":
    testenv.main()
