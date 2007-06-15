from testbase import PersistTest, AssertMixin
import testbase
import unittest, sys, os
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.sessioncontext import SessionContext
from testbase import Table, Column
import datetime

class EagerTest(AssertMixin):
    def setUpAll(self):
        global companies_table, addresses_table, invoice_table, phones_table, items_table, ctx, metadata

        metadata = BoundMetaData(testbase.db)
        ctx = SessionContext(create_session)
        
        companies_table = Table('companies', metadata,
            Column('company_id', Integer, Sequence('company_id_seq', optional=True), primary_key = True),
            Column('company_name', String(40)),

        )
        
        addresses_table = Table('addresses', metadata,
                                Column('address_id', Integer, Sequence('address_id_seq', optional=True), primary_key = True),
                                Column('company_id', Integer, ForeignKey("companies.company_id")),
                                Column('address', String(40)),
                                )

        phones_table = Table('phone_numbers', metadata,
                                Column('phone_id', Integer, Sequence('phone_id_seq', optional=True), primary_key = True),
                                Column('address_id', Integer, ForeignKey('addresses.address_id')),
                                Column('type', String(20)),
                                Column('number', String(10)),
                                )

        invoice_table = Table('invoices', metadata,
                              Column('invoice_id', Integer, Sequence('invoice_id_seq', optional=True), primary_key = True),
                              Column('company_id', Integer, ForeignKey("companies.company_id")),
                              Column('date', DateTime),   
                              )

        items_table = Table('items', metadata,
                            Column('item_id', Integer, Sequence('item_id_seq', optional=True), primary_key = True),
                            Column('invoice_id', Integer, ForeignKey('invoices.invoice_id')),
                            Column('code', String(20)),
                            Column('qty', Integer),
                            )

        metadata.create_all()
        
    def tearDownAll(self):
        metadata.drop_all()

    def tearDown(self):
        clear_mappers()
        for t in metadata.table_iterator(reverse=True):
            t.delete().execute()

    def testone(self):
        """tests eager load of a many-to-one attached to a one-to-many.  this testcase illustrated 
        the bug, which is that when the single Company is loaded, no further processing of the rows
        occurred in order to load the Company's second Address object."""
        class Company(object):
            def __init__(self):
                self.company_id = None
            def __repr__(self):
                return "Company:" + repr(getattr(self, 'company_id', None)) + " " + repr(getattr(self, 'company_name', None)) + " " + str([repr(addr) for addr in self.addresses])

        class Address(object):
            def __repr__(self):
                return "Address: " + repr(getattr(self, 'address_id', None)) + " " + repr(getattr(self, 'company_id', None)) + " " + repr(self.address)

        class Invoice(object):
            def __init__(self):
                self.invoice_id = None
            def __repr__(self):
                return "Invoice:" + repr(getattr(self, 'invoice_id', None)) + " " + repr(getattr(self, 'date', None))  + " " + repr(self.company)

        mapper(Address, addresses_table, properties={
            }, extension=ctx.mapper_extension)
        mapper(Company, companies_table, properties={
            'addresses' : relation(Address, lazy=False),
            }, extension=ctx.mapper_extension)
        mapper(Invoice, invoice_table, properties={
            'company': relation(Company, lazy=False, )
            }, extension=ctx.mapper_extension)

        c1 = Company()
        c1.company_name = 'company 1'
        a1 = Address()
        a1.address = 'a1 address'
        c1.addresses.append(a1)
        a2 = Address()
        a2.address = 'a2 address'
        c1.addresses.append(a2)
        i1 = Invoice()
        i1.date = datetime.datetime.now()
        i1.company = c1

        ctx.current.flush()

        company_id = c1.company_id
        invoice_id = i1.invoice_id

        ctx.current.clear()

        c = ctx.current.query(Company).get(company_id)

        ctx.current.clear()

        i = ctx.current.query(Invoice).get(invoice_id)

        self.echo(repr(c))
        self.echo(repr(i.company))
        self.assert_(repr(c) == repr(i.company))

    def testtwo(self):
        """this is the original testcase that includes various complicating factors"""
        class Company(object):
            def __init__(self):
                self.company_id = None
            def __repr__(self):
                return "Company:" + repr(getattr(self, 'company_id', None)) + " " + repr(getattr(self, 'company_name', None)) + " " + str([repr(addr) for addr in self.addresses])

        class Address(object):
            def __repr__(self):
                return "Address: " + repr(getattr(self, 'address_id', None)) + " " + repr(getattr(self, 'company_id', None)) + " " + repr(self.address) + str([repr(ph) for ph in self.phones])

        class Phone(object):
            def __repr__(self):
                return "Phone: " + repr(getattr(self, 'phone_id', None)) + " " + repr(getattr(self, 'address_id', None)) + " " + repr(self.type) + " " + repr(self.number)

        class Invoice(object):
            def __init__(self):
                self.invoice_id = None
            def __repr__(self):
                return "Invoice:" + repr(getattr(self, 'invoice_id', None)) + " " + repr(getattr(self, 'date', None))  + " " + repr(self.company) + " " + str([repr(item) for item in self.items])

        class Item(object):
            def __repr__(self):
                return "Item: " + repr(getattr(self, 'item_id', None)) + " " + repr(getattr(self, 'invoice_id', None)) + " " + repr(self.code) + " " + repr(self.qty)

        mapper(Phone, phones_table, extension=ctx.mapper_extension)

        mapper(Address, addresses_table, properties={
            'phones': relation(Phone, lazy=False, backref='address')
            }, extension=ctx.mapper_extension)

        mapper(Company, companies_table, properties={
            'addresses' : relation(Address, lazy=False, backref='company'),
            }, extension=ctx.mapper_extension)

        mapper(Item, items_table, extension=ctx.mapper_extension)

        mapper(Invoice, invoice_table, properties={
            'items': relation(Item, lazy=False, backref='invoice'),
            'company': relation(Company, lazy=False, backref='invoices')
            }, extension=ctx.mapper_extension)

        ctx.current.clear()
        c1 = Company()
        c1.company_name = 'company 1'

        a1 = Address()
        a1.address = 'a1 address'

        p1 = Phone()
        p1.type = 'home'
        p1.number = '1111'

        a1.phones.append(p1)

        p2 = Phone()
        p2.type = 'work'
        p2.number = '22222'
        a1.phones.append(p2)

        c1.addresses.append(a1)

        a2 = Address()
        a2.address = 'a2 address'

        p3 = Phone()
        p3.type = 'home'
        p3.number = '3333'
        a2.phones.append(p3)

        p4 = Phone()
        p4.type = 'work'
        p4.number = '44444'
        a2.phones.append(p4)

        c1.addresses.append(a2)

        ctx.current.flush()

        company_id = c1.company_id
        
        ctx.current.clear()

        a = ctx.current.query(Company).get(company_id)
        self.echo(repr(a))

        # set up an invoice
        i1 = Invoice()
        i1.date = datetime.datetime.now()
        i1.company = c1

        item1 = Item()
        item1.code = 'aaaa'
        item1.qty = 1
        item1.invoice = i1

        item2 = Item()
        item2.code = 'bbbb'
        item2.qty = 2
        item2.invoice = i1

        item3 = Item()
        item3.code = 'cccc'
        item3.qty = 3
        item3.invoice = i1

        ctx.current.flush()

        invoice_id = i1.invoice_id

        ctx.current.clear()

        c = ctx.current.query(Company).get(company_id)
        self.echo(repr(c))

        ctx.current.clear()

        i = ctx.current.query(Invoice).get(invoice_id)

        assert repr(i.company) == repr(c), repr(i.company) +  " does not match " + repr(c)
        
if __name__ == "__main__":    
    testbase.main()
