import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from sqlalchemy.orm import *
from testlib import *


class PolymorphicCircularTest(ORMTest):
    keep_mappers = True
    def define_tables(self, metadata):
        global Table1, Table1B, Table2, Table3,  Data
        table1 = Table('table1', metadata,
                       Column('id', Integer, primary_key=True),
                       Column('related_id', Integer, ForeignKey('table1.id'), nullable=True),
                       Column('type', String(30)),
                       Column('name', String(30))
                       )

        table2 = Table('table2', metadata,
                       Column('id', Integer, ForeignKey('table1.id'), primary_key=True),
                       )

        table3 = Table('table3', metadata,
                      Column('id', Integer, ForeignKey('table1.id'), primary_key=True),
                      )

        data = Table('data', metadata,
            Column('id', Integer, primary_key=True),
            Column('node_id', Integer, ForeignKey('table1.id')),
            Column('data', String(30))
            )

        #join = polymorphic_union(
        #    {
        #    'table3' : table1.join(table3),
        #    'table2' : table1.join(table2),
        #    'table1' : table1.select(table1.c.type.in_(['table1', 'table1b'])),
        #    }, None, 'pjoin')

        join = table1.outerjoin(table2).outerjoin(table3).alias('pjoin')
        #join = None

        class Table1(object):
            def __init__(self, name, data=None):
                self.name = name
                if data is not None:
                    self.data = data
            def __repr__(self):
                return "%s(%s, %s, %s)" % (self.__class__.__name__, self.id, repr(str(self.name)), repr(self.data))

        class Table1B(Table1):
            pass

        class Table2(Table1):
            pass

        class Table3(Table1):
            pass

        class Data(object):
            def __init__(self, data):
                self.data = data
            def __repr__(self):
                return "%s(%s, %s)" % (self.__class__.__name__, self.id, repr(str(self.data)))

        try:
            # this is how the mapping used to work.  ensure that this raises an error now
            table1_mapper = mapper(Table1, table1,
                                   select_table=join,
                                   polymorphic_on=table1.c.type,
                                   polymorphic_identity='table1',
                                   properties={
                                    'next': relation(Table1,
                                        backref=backref('prev', primaryjoin=join.c.id==join.c.related_id, foreignkey=join.c.id, uselist=False),
                                        uselist=False, primaryjoin=join.c.id==join.c.related_id),
                                    'data':relation(mapper(Data, data))
                                    },
                            order_by=table1.c.id)
            table1_mapper.compile()
            assert False
        except:
            assert True
            clear_mappers()

        # currently, the "eager" relationships degrade to lazy relationships
        # due to the polymorphic load.
        # the "next" relation used to have a "lazy=False" on it, but the EagerLoader raises the "self-referential"
        # exception now.  since eager loading would never work for that relation anyway, its better that the user
        # gets an exception instead of it silently not eager loading.
        table1_mapper = mapper(Table1, table1,
                               #select_table=join,
                               polymorphic_on=table1.c.type,
                               polymorphic_identity='table1',
                               properties={
                               'next': relation(Table1,
                                   backref=backref('prev', primaryjoin=table1.c.id==table1.c.related_id, remote_side=table1.c.id, uselist=False),
                                   uselist=False, primaryjoin=table1.c.id==table1.c.related_id),
                               'data':relation(mapper(Data, data), lazy=False, order_by=data.c.id)
                                },
                                order_by=table1.c.id
                        )

        table1b_mapper = mapper(Table1B, inherits=table1_mapper, polymorphic_identity='table1b')

        table2_mapper = mapper(Table2, table2,
                               inherits=table1_mapper,
                               polymorphic_identity='table2')

        table3_mapper = mapper(Table3, table3, inherits=table1_mapper, polymorphic_identity='table3')

        table1_mapper.compile()
        assert table1_mapper.primary_key == [table1.c.id], table1_mapper.primary_key

    @testing.fails_on('maxdb')
    def testone(self):
        self.do_testlist([Table1, Table2, Table1, Table2])

    @testing.fails_on('maxdb')
    def testtwo(self):
        self.do_testlist([Table3])

    @testing.fails_on('maxdb')
    def testthree(self):
        self.do_testlist([Table2, Table1, Table1B, Table3, Table3, Table1B, Table1B, Table2, Table1])

    @testing.fails_on('maxdb')
    def testfour(self):
        self.do_testlist([
                Table2('t2', [Data('data1'), Data('data2')]),
                Table1('t1', []),
                Table3('t3', [Data('data3')]),
                Table1B('t1b', [Data('data4'), Data('data5')])
                ])

    def do_testlist(self, classes):
        sess = create_session( )

        # create objects in a linked list
        count = 1
        obj = None
        for c in classes:
            if isinstance(c, type):
                newobj = c('item %d' % count)
                count += 1
            else:
                newobj = c
            if obj is not None:
                obj.next = newobj
            else:
                t = newobj
            obj = newobj

        # save to DB
        sess.save(t)
        sess.flush()

        # string version of the saved list
        assertlist = []
        node = t
        while (node):
            assertlist.append(node)
            n = node.next
            if n is not None:
                assert n.prev is node
            node = n
        original = repr(assertlist)


        # clear and query forwards
        sess.clear()
        node = sess.query(Table1).filter(Table1.id==t.id).first()
        assertlist = []
        while (node):
            assertlist.append(node)
            n = node.next
            if n is not None:
                assert n.prev is node
            node = n
        forwards = repr(assertlist)

        # clear and query backwards
        sess.clear()
        node = sess.query(Table1).filter(Table1.id==obj.id).first()
        assertlist = []
        while (node):
            assertlist.insert(0, node)
            n = node.prev
            if n is not None:
                assert n.next is node
            node = n
        backwards = repr(assertlist)

        # everything should match !
        assert original == forwards == backwards

if __name__ == '__main__':
    testenv.main()
