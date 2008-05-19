from testlib.sa import MetaData, Table, Column, Integer, String, ForeignKey
from testlib.sa.orm import attributes
from testlib.compat import set
from testlib.testing import fixture
from orm import _base

__all__ = ()

fixture_metadata = MetaData()

def fixture_table(table, columns, *rows):
    def load_fixture(bind=None):
        bind = bind or table.bind
        bind.execute(
            table.insert(),
            [dict(zip(columns, column_values)) for column_values in rows])
    table.info[('fixture', 'loader')] = load_fixture
    table.info[('fixture', 'columns')] = columns
    table.info[('fixture', 'rows')] = rows
    return table

users = fixture_table(
    Table('users', fixture_metadata,
          Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
          Column('name', String(30), nullable=False),
          test_needs_acid=True,
          test_needs_fk=True),
    ('id', 'name'),
    (7, 'jack'),
    (8, 'ed'),
    (9, 'fred'),
    (10, 'chuck'))

addresses = fixture_table(
    Table('addresses', fixture_metadata,
          Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
          Column('user_id', None, ForeignKey('users.id')),
          Column('email_address', String(50), nullable=False),
          test_needs_acid=True,
          test_needs_fk=True),
    ('id', 'user_id', 'email_address'),
    (1, 7, "jack@bean.com"),
    (2, 8, "ed@wood.com"),
    (3, 8, "ed@bettyboop.com"),
    (4, 8, "ed@lala.com"),
    (5, 9, "fred@fred.com"))

email_bounces = fixture_table(
    Table('email_bounces', fixture_metadata,
          Column('id', Integer, ForeignKey('addresses.id')),
          Column('bounces', Integer)),
    ('id', 'bounces'),
    (1, 1),
    (2, 0),
    (3, 5),
    (4, 0),
    (5, 0))

orders = fixture_table(
    Table('orders', fixture_metadata,
          Column('id', Integer, primary_key=True),
          Column('user_id', None, ForeignKey('users.id')),
          Column('address_id', None, ForeignKey('addresses.id')),
          Column('description', String(30)),
          Column('isopen', Integer),
          test_needs_acid=True,
          test_needs_fk=True),
    ('id', 'user_id', 'description', 'isopen', 'address_id'),
    (1, 7, 'order 1', 0, 1),
    (2, 9, 'order 2', 0, 4),
    (3, 7, 'order 3', 1, 1),
    (4, 9, 'order 4', 1, 4),
    (5, 7, 'order 5', 0, None))

dingalings = fixture_table(
    Table("dingalings", fixture_metadata,
          Column('id', Integer, primary_key=True),
          Column('address_id', None, ForeignKey('addresses.id')),
          Column('data', String(30)),
          test_needs_acid=True,
          test_needs_fk=True),
    ('id', 'address_id', 'data'),
    (1, 2, 'ding 1/2'),
    (2, 5, 'ding 2/5'))

items = fixture_table(
    Table('items', fixture_metadata,
          Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
          Column('description', String(30), nullable=False),
          test_needs_acid=True,
          test_needs_fk=True),
    ('id', 'description'),
    (1, 'item 1'),
    (2, 'item 2'),
    (3, 'item 3'),
    (4, 'item 4'),
    (5, 'item 5'))

order_items = fixture_table(
    Table('order_items', fixture_metadata,
          Column('item_id', None, ForeignKey('items.id')),
          Column('order_id', None, ForeignKey('orders.id')),
          test_needs_acid=True,
          test_needs_fk=True),
    ('item_id', 'order_id'),
    (1, 1),
    (2, 1),
    (3, 1),

    (1, 2),
    (2, 2),
    (3, 2),

    (3, 3),
    (4, 3),
    (5, 3),

    (1, 4),
    (5, 4),

    (5, 5))

keywords = fixture_table(
    Table('keywords', fixture_metadata,
          Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
          Column('name', String(30), nullable=False),
          test_needs_acid=True,
          test_needs_fk=True),
    ('id', 'name'),
    (1, 'blue'),
    (2, 'red'),
    (3, 'green'),
    (4, 'big'),
    (5, 'small'),
    (6, 'round'),
    (7, 'square'))

item_keywords = fixture_table(
    Table('item_keywords', fixture_metadata,
          Column('item_id', None, ForeignKey('items.id')),
          Column('keyword_id', None, ForeignKey('keywords.id')),
          test_needs_acid=True,
          test_needs_fk=True),
    ('keyword_id', 'item_id'),
    (2, 1),
    (2, 2),
    (4, 1),
    (6, 1),
    (5, 2),
    (3, 3),
    (4, 3),
    (7, 2),
    (6, 3))


def _load_fixtures():
    for table in fixture_metadata.table_iterator(reverse=False):
        table.info[('fixture', 'loader')]()

def run_inserts_for(table, bind=None):
    table.info[('fixture', 'loader')](bind)


class Base(_base.ComparableEntity):
    pass

_recursion_stack = set()
class ZBase(_base.BasicEntity):
    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        """'passively' compare this object to another.

        only look at attributes that are present on the source object.

        """
        if self in _recursion_stack:
            return True
        _recursion_stack.add(self)
        try:
            # pick the entity thats not SA persisted as the source
            try:
                state = attributes.instance_state(self)
                key = state.key
            except (KeyError, AttributeError):
                key = None
            if other is None:
                a = self
                b = other
            elif key is not None:
                a = other
                b = self
            else:
                a = self
                b = other

            for attr in a.__dict__.keys():
                if attr[0] == '_':
                    continue
                value = getattr(a, attr)
                #print "looking at attr:", attr, "start value:", value
                if hasattr(value, '__iter__') and not isinstance(value, basestring):
                    try:
                        # catch AttributeError so that lazy loaders trigger
                        battr = getattr(b, attr)
                    except AttributeError:
                        #print "b class does not have attribute named '%s'" % attr
                        #raise
                        return False

                    if list(value) == list(battr):
                        continue
                    else:
                        return False
                else:
                    if value is not None:
                        if value != getattr(b, attr, None):
                            #print "2. Attribute named '%s' does not match that of b" % attr
                            return False
            else:
                return True
        finally:
            _recursion_stack.remove(self)

class User(Base):
    pass
class Order(Base):
    pass
class Item(Base):
    pass
class Keyword(Base):
    pass
class Address(Base):
    pass
class Dingaling(Base):
    pass


class FixtureTest(_base.MappedTest):
    """A MappedTest pre-configured for fixtures.

    All fixture tables are pre-loaded into cls.tables, as are all fixture
    lasses in cls.classes and as cls.ClassName.

    Fixture.mapper() still functions and willregister non-fixture classes into
    cls.classes.

    """

    run_define_tables = 'once'
    run_setup_classes = 'once'
    run_setup_mappers = 'each'
    run_inserts = 'each'
    run_deletes = 'each'
    
    metadata = fixture_metadata
    fixture_classes = dict(User=User,
                           Order=Order,
                           Item=Item,
                           Keyword=Keyword,
                           Address=Address,
                           Dingaling=Dingaling)

    def setUpAll(self):
        assert not hasattr(self, 'refresh_data')
        assert not hasattr(self, 'only_tables')
        #refresh_data = False
        #only_tables = False

        #if type(self) is not FixtureTest:
        #    setattr(type(self), 'classes', _base.adict(self.classes))

        #if self.run_setup_classes:
        #    for cls in self.classes.values():
        #        self.register_class(cls)
        super(FixtureTest, self).setUpAll()

        #if not self.only_tables and self.keep_data:
        #    _registry.load()

    def define_tables(self, metadata):
        pass

    def setup_classes(self):
        for cls in self.fixture_classes.values():
            self.register_class(cls)

    def setup_mappers(self):
        pass

    def insert_data(self):
        _load_fixtures()


class CannedResults(object):
    """Built on demand, instances use mappers in effect at time of call."""

    @property
    def user_result(self):
        return [
            User(id=7),
            User(id=8),
            User(id=9),
            User(id=10)]

    @property
    def user_address_result(self):
        return [
            User(id=7, addresses=[
                Address(id=1)
            ]),
            User(id=8, addresses=[
                Address(id=2, email_address='ed@wood.com'),
                Address(id=3, email_address='ed@bettyboop.com'),
                Address(id=4, email_address='ed@lala.com'),
            ]),
            User(id=9, addresses=[
                Address(id=5)
            ]),
            User(id=10, addresses=[])]

    @property
    def user_all_result(self):
        return [
            User(id=7,
                 addresses=[
                   Address(id=1)],
                 orders=[
                   Order(description='order 1',
                         items=[
                           Item(description='item 1'),
                           Item(description='item 2'),
                           Item(description='item 3')]),
                   Order(description='order 3'),
                   Order(description='order 5')]),
            User(id=8,
                 addresses=[
                   Address(id=2),
                   Address(id=3),
                   Address(id=4)]),
            User(id=9,
                 addresses=[
                   Address(id=5)],
                 orders=[
                   Order(description='order 2',
                         items=[
                           Item(description='item 1'),
                           Item(description='item 2'),
                           Item(description='item 3')]),
                   Order(description='order 4',
                         items=[
                           Item(description='item 1'),
                           Item(description='item 5')])]),
            User(id=10, addresses=[])]

    @property
    def user_order_result(self):
        return [
            User(id=7,
                 orders=[
                   Order(id=1,
                         items=[
                           Item(id=1),
                           Item(id=2),
                           Item(id=3)]),
                   Order(id=3,
                         items=[
                           Item(id=3),
                           Item(id=4),
                           Item(id=5)]),
                   Order(id=5,
                         items=[
                           Item(id=5)])]),
            User(id=8,
                 orders=[]),
            User(id=9,
                 orders=[
                   Order(id=2,
                         items=[
                           Item(id=1),
                           Item(id=2),
                           Item(id=3)]),
                   Order(id=4,
                         items=[
                           Item(id=1),
                           Item(id=5)])]),
            User(id=10)]

    @property
    def item_keyword_result(self):
        return [
            Item(id=1,
                 keywords=[
                   Keyword(name='red'),
                   Keyword(name='big'),
                   Keyword(name='round')]),
            Item(id=2,
                 keywords=[
                   Keyword(name='red'),
                   Keyword(name='small'),
                   Keyword(name='square')]),
            Item(id=3,
                 keywords=[
                   Keyword(name='green'),
                   Keyword(name='big'),
                   Keyword(name='round')]),
            Item(id=4,
                 keywords=[]),
            Item(id=5,
                 keywords=[])]
FixtureTest.static = CannedResults()

