import testenv; testenv.configure_for_tests()
from datetime import datetime
from sqlalchemy import *
from sqlalchemy.orm import *
from testlib import *


class InheritTest(ORMTest):
    """tests some various inheritance round trips involving a particular set of polymorphic inheritance relationships"""
    def define_tables(self, metadata):
        global products_table, specification_table, documents_table
        global Product, Detail, Assembly, SpecLine, Document, RasterDocument

        products_table = Table('products', metadata,
           Column('product_id', Integer, primary_key=True),
           Column('product_type', String(128)),
           Column('name', String(128)),
           Column('mark', String(128)),
           )

        specification_table = Table('specification', metadata,
            Column('spec_line_id', Integer, primary_key=True),
            Column('master_id', Integer, ForeignKey("products.product_id"),
                nullable=True),
            Column('slave_id', Integer, ForeignKey("products.product_id"),
                nullable=True),
            Column('quantity', Float, default=1.),
            )

        documents_table = Table('documents', metadata,
            Column('document_id', Integer, primary_key=True),
            Column('document_type', String(128)),
            Column('product_id', Integer, ForeignKey('products.product_id')),
            Column('create_date', DateTime, default=lambda:datetime.now()),
            Column('last_updated', DateTime, default=lambda:datetime.now(),
                onupdate=lambda:datetime.now()),
            Column('name', String(128)),
            Column('data', Binary),
            Column('size', Integer, default=0),
            )

        class Product(object):
            def __init__(self, name, mark=''):
                self.name = name
                self.mark = mark
            def __repr__(self):
                return '<%s %s>' % (self.__class__.__name__, self.name)

        class Detail(Product):
            def __init__(self, name):
                self.name = name

        class Assembly(Product):
            def __repr__(self):
                return Product.__repr__(self) + " " + " ".join([x + "=" + repr(getattr(self, x, None)) for x in ['specification', 'documents']])

        class SpecLine(object):
            def __init__(self, master=None, slave=None, quantity=1):
                self.master = master
                self.slave = slave
                self.quantity = quantity

            def __repr__(self):
                return '<%s %.01f %s>' % (
                    self.__class__.__name__,
                    self.quantity or 0.,
                    repr(self.slave)
                    )

        class Document(object):
            def __init__(self, name, data=None):
                self.name = name
                self.data = data
            def __repr__(self):
                return '<%s %s>' % (self.__class__.__name__, self.name)

        class RasterDocument(Document):
            pass

    def testone(self):
        product_mapper = mapper(Product, products_table,
            polymorphic_on=products_table.c.product_type,
            polymorphic_identity='product')

        detail_mapper = mapper(Detail, inherits=product_mapper,
            polymorphic_identity='detail')

        assembly_mapper = mapper(Assembly, inherits=product_mapper,
            polymorphic_identity='assembly')

        specification_mapper = mapper(SpecLine, specification_table,
            properties=dict(
                master=relation(Assembly,
                    foreign_keys=[specification_table.c.master_id],
                    primaryjoin=specification_table.c.master_id==products_table.c.product_id,
                    lazy=True, backref=backref('specification'),
                    uselist=False),
                slave=relation(Product,
                    foreign_keys=[specification_table.c.slave_id],
                    primaryjoin=specification_table.c.slave_id==products_table.c.product_id,
                    lazy=True, uselist=False),
                quantity=specification_table.c.quantity,
                )
            )

        session = create_session( )

        a1 = Assembly(name='a1')

        p1 = Product(name='p1')
        a1.specification.append(SpecLine(slave=p1))

        d1 = Detail(name='d1')
        a1.specification.append(SpecLine(slave=d1))

        session.save(a1)
        orig = repr(a1)
        session.flush()
        session.clear()

        a1 = session.query(Product).filter_by(name='a1').one()
        new = repr(a1)
        print orig
        print new
        assert orig == new == '<Assembly a1> specification=[<SpecLine 1.0 <Product p1>>, <SpecLine 1.0 <Detail d1>>] documents=None'

    def testtwo(self):
        product_mapper = mapper(Product, products_table,
            polymorphic_on=products_table.c.product_type,
            polymorphic_identity='product')

        detail_mapper = mapper(Detail, inherits=product_mapper,
            polymorphic_identity='detail')

        specification_mapper = mapper(SpecLine, specification_table,
            properties=dict(
                slave=relation(Product,
                    foreign_keys=[specification_table.c.slave_id],
                    primaryjoin=specification_table.c.slave_id==products_table.c.product_id,
                    lazy=True, uselist=False),
                )
            )

        session = create_session( )

        s = SpecLine(slave=Product(name='p1'))
        s2 = SpecLine(slave=Detail(name='d1'))
        session.save(s)
        session.save(s2)
        orig = repr([s, s2])
        session.flush()
        session.clear()
        new = repr(session.query(SpecLine).all())
        print orig
        print new
        assert orig == new == '[<SpecLine 1.0 <Product p1>>, <SpecLine 1.0 <Detail d1>>]'

    def testthree(self):
        product_mapper = mapper(Product, products_table,
            polymorphic_on=products_table.c.product_type,
            polymorphic_identity='product')
        detail_mapper = mapper(Detail, inherits=product_mapper,
            polymorphic_identity='detail')
        assembly_mapper = mapper(Assembly, inherits=product_mapper,
            polymorphic_identity='assembly')

        specification_mapper = mapper(SpecLine, specification_table,
            properties=dict(
                master=relation(Assembly, lazy=False, uselist=False,
                    foreign_keys=[specification_table.c.master_id],
                    primaryjoin=specification_table.c.master_id==products_table.c.product_id,
                    backref=backref('specification', cascade="all, delete-orphan"),
                    ),
                slave=relation(Product, lazy=False,  uselist=False,
                    foreign_keys=[specification_table.c.slave_id],
                    primaryjoin=specification_table.c.slave_id==products_table.c.product_id,
                    ),
                quantity=specification_table.c.quantity,
                )
            )

        document_mapper = mapper(Document, documents_table,
            polymorphic_on=documents_table.c.document_type,
            polymorphic_identity='document',
            properties=dict(
                name=documents_table.c.name,
                data=deferred(documents_table.c.data),
                product=relation(Product, lazy=True, backref=backref('documents', cascade="all, delete-orphan")),
                ),
            )
        raster_document_mapper = mapper(RasterDocument, inherits=document_mapper,
            polymorphic_identity='raster_document')

        session = create_session()

        a1 = Assembly(name='a1')
        a1.specification.append(SpecLine(slave=Detail(name='d1')))
        a1.documents.append(Document('doc1'))
        a1.documents.append(RasterDocument('doc2'))
        session.save(a1)
        orig = repr(a1)
        session.flush()
        session.clear()

        a1 = session.query(Product).filter_by(name='a1').one()
        new = repr(a1)
        print orig
        print new
        assert orig == new  == '<Assembly a1> specification=[<SpecLine 1.0 <Detail d1>>] documents=[<Document doc1>, <RasterDocument doc2>]'

    def testfour(self):
        """this tests the RasterDocument being attached to the Assembly, but *not* the Document.  this means only
        a "sub-class" task, i.e. corresponding to an inheriting mapper but not the base mapper, is created. """
        product_mapper = mapper(Product, products_table,
            polymorphic_on=products_table.c.product_type,
            polymorphic_identity='product')
        detail_mapper = mapper(Detail, inherits=product_mapper,
            polymorphic_identity='detail')
        assembly_mapper = mapper(Assembly, inherits=product_mapper,
            polymorphic_identity='assembly')

        document_mapper = mapper(Document, documents_table,
            polymorphic_on=documents_table.c.document_type,
            polymorphic_identity='document',
            properties=dict(
                name=documents_table.c.name,
                data=deferred(documents_table.c.data),
                product=relation(Product, lazy=True, backref=backref('documents', cascade="all, delete-orphan")),
                ),
            )
        raster_document_mapper = mapper(RasterDocument, inherits=document_mapper,
            polymorphic_identity='raster_document')

        session = create_session( )

        a1 = Assembly(name='a1')
        a1.documents.append(RasterDocument('doc2'))
        session.save(a1)
        orig = repr(a1)
        session.flush()
        session.clear()

        a1 = session.query(Product).filter_by(name='a1').one()
        new = repr(a1)
        print orig
        print new
        assert orig == new  == '<Assembly a1> specification=None documents=[<RasterDocument doc2>]'

        del a1.documents[0]
        session.flush()
        session.clear()

        a1 = session.query(Product).filter_by(name='a1').one()
        assert len(session.query(Document).all()) == 0

    def testfive(self):
        """tests the late compilation of mappers"""

        specification_mapper = mapper(SpecLine, specification_table,
            properties=dict(
                master=relation(Assembly, lazy=False, uselist=False,
                    foreign_keys=[specification_table.c.master_id],
                    primaryjoin=specification_table.c.master_id==products_table.c.product_id,
                    backref=backref('specification'),
                    ),
                slave=relation(Product, lazy=False,  uselist=False,
                    foreign_keys=[specification_table.c.slave_id],
                    primaryjoin=specification_table.c.slave_id==products_table.c.product_id,
                    ),
                quantity=specification_table.c.quantity,
                )
            )

        product_mapper = mapper(Product, products_table,
            polymorphic_on=products_table.c.product_type,
            polymorphic_identity='product', properties={
            'documents' : relation(Document, lazy=True,
                    backref='product', cascade='all, delete-orphan'),
            })

        detail_mapper = mapper(Detail, inherits=Product,
            polymorphic_identity='detail')

        document_mapper = mapper(Document, documents_table,
            polymorphic_on=documents_table.c.document_type,
            polymorphic_identity='document',
            properties=dict(
                name=documents_table.c.name,
                data=deferred(documents_table.c.data),
                ),
            )

        raster_document_mapper = mapper(RasterDocument, inherits=Document,
            polymorphic_identity='raster_document')

        assembly_mapper = mapper(Assembly, inherits=Product,
            polymorphic_identity='assembly')

        session = create_session()

        a1 = Assembly(name='a1')
        a1.specification.append(SpecLine(slave=Detail(name='d1')))
        a1.documents.append(Document('doc1'))
        a1.documents.append(RasterDocument('doc2'))
        session.save(a1)
        orig = repr(a1)
        session.flush()
        session.clear()

        a1 = session.query(Product).filter_by(name='a1').one()
        new = repr(a1)
        print orig
        print new
        assert orig == new  == '<Assembly a1> specification=[<SpecLine 1.0 <Detail d1>>] documents=[<Document doc1>, <RasterDocument doc2>]'

if __name__ == "__main__":
    testenv.main()
