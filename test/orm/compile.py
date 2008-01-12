import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from sqlalchemy import exceptions
from sqlalchemy.orm import *
from testlib import *


class CompileTest(AssertMixin):
    """test various mapper compilation scenarios"""
    def tearDown(self):
        clear_mappers()

    def testone(self):
        global metadata, order, employee, product, tax, orderproduct
        metadata = MetaData(testing.db)

        order = Table('orders', metadata,
            Column('id', Integer, primary_key=True),
            Column('employee_id', Integer, ForeignKey('employees.id'), nullable=False),
            Column('type', Unicode(16)))

        employee = Table('employees', metadata,
            Column('id', Integer, primary_key=True),
            Column('name', Unicode(16), unique=True, nullable=False))

        product = Table('products', metadata,
            Column('id', Integer, primary_key=True),
        )

        orderproduct = Table('orderproducts', metadata,
            Column('id', Integer, primary_key=True),
            Column('order_id', Integer, ForeignKey("orders.id"), nullable=False),
            Column('product_id', Integer, ForeignKey("products.id"), nullable=False),
        )

        class Order(object):
            pass

        class Employee(object):
            pass

        class Product(object):
            pass

        class OrderProduct(object):
            pass

        order_join = order.select().alias('pjoin')

        order_mapper = mapper(Order, order,
            select_table=order_join,
            polymorphic_on=order_join.c.type,
            polymorphic_identity='order',
            properties={
                'orderproducts': relation(OrderProduct, lazy=True, backref='order')}
            )

        mapper(Product, product,
            properties={
                'orderproducts': relation(OrderProduct, lazy=True, backref='product')}
            )

        mapper(Employee, employee,
            properties={
                'orders': relation(Order, lazy=True, backref='employee')})

        mapper(OrderProduct, orderproduct)

        # this requires that the compilation of order_mapper's "surrogate mapper" occur after
        # the initial setup of MapperProperty objects on the mapper.
        class_mapper(Product).compile()

    def testtwo(self):
        """test that conflicting backrefs raises an exception"""
        global metadata, order, employee, product, tax, orderproduct
        metadata = MetaData(testing.db)

        order = Table('orders', metadata,
            Column('id', Integer, primary_key=True),
            Column('type', Unicode(16)))

        product = Table('products', metadata,
            Column('id', Integer, primary_key=True),
        )

        orderproduct = Table('orderproducts', metadata,
            Column('id', Integer, primary_key=True),
            Column('order_id', Integer, ForeignKey("orders.id"), nullable=False),
            Column('product_id', Integer, ForeignKey("products.id"), nullable=False),
        )

        class Order(object):
            pass

        class Product(object):
            pass

        class OrderProduct(object):
            pass

        order_join = order.select().alias('pjoin')

        order_mapper = mapper(Order, order,
            select_table=order_join,
            polymorphic_on=order_join.c.type,
            polymorphic_identity='order',
            properties={
                'orderproducts': relation(OrderProduct, lazy=True, backref='product')}
            )

        mapper(Product, product,
            properties={
                'orderproducts': relation(OrderProduct, lazy=True, backref='product')}
            )

        mapper(OrderProduct, orderproduct)

        try:
            class_mapper(Product).compile()
            assert False
        except exceptions.ArgumentError, e:
            assert str(e).index("Error creating backref ") > -1

    def testthree(self):
        metadata = MetaData(testing.db)
        node_table = Table("node", metadata,
            Column('node_id', Integer, primary_key=True),
            Column('name_index', Integer, nullable=True),
            )
        node_name_table = Table("node_name", metadata,
            Column('node_name_id', Integer, primary_key=True),
            Column('node_id', Integer, ForeignKey('node.node_id')),
            Column('host_id', Integer, ForeignKey('host.host_id')),
            Column('name', String(64), nullable=False),
            )
        host_table = Table("host", metadata,
            Column('host_id', Integer, primary_key=True),
            Column('hostname', String(64), nullable=False,
        unique=True),
            )
        metadata.create_all()
        try:
            node_table.insert().execute(node_id=1, node_index=5)
            class Node(object):pass
            class NodeName(object):pass
            class Host(object):pass

            node_mapper = mapper(Node, node_table)
            host_mapper = mapper(Host, host_table)
            node_name_mapper = mapper(NodeName, node_name_table,
            properties = {
                'node' : relation(Node, backref=backref('names')),
                'host' : relation(Host),
                }
            )
            sess = create_session()
            assert sess.query(Node).get(1).names == []
        finally:
            metadata.drop_all()

    def testfour(self):
        meta = MetaData()

        a = Table('a', meta, Column('id', Integer, primary_key=True))
        b = Table('b', meta, Column('id', Integer, primary_key=True), Column('a_id', Integer, ForeignKey('a.id')))

        class A(object):pass
        class B(object):pass

        mapper(A, a, properties={
            'b':relation(B, backref='a')
        })
        mapper(B, b, properties={
            'a':relation(A, backref='b')
        })

        try:
            compile_mappers()
            assert False
        except exceptions.ArgumentError, e:
            assert str(e).index("Error creating backref") > -1

if __name__ == '__main__':
    testenv.main()
