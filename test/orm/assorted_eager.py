"""Exercises for eager loading.

Derived from mailing list-reported problems and trac tickets.

"""
import testenv; testenv.configure_for_tests()
import random, datetime
from sqlalchemy import *
from sqlalchemy.orm import *
from testlib import *
from orm import _base

class EagerTest(_base.ORMTest):
    def setUpAll(self):
        global dbmeta, owners, categories, tests, options, Owner, Category, Test, Option, false
        dbmeta = MetaData(testing.db)

        # determine a literal value for "false" based on the dialect
        # FIXME: this PassiveDefault setup is bogus.
        bp = Boolean().dialect_impl(testing.db.dialect).bind_processor(testing.db.dialect)
        if bp:
            false = str(bp(False))
        elif testing.against('maxdb'):
            false = text('FALSE')
        else:
            false = str(False)

        owners = Table('owners', dbmeta ,
            Column('id', Integer, primary_key=True, nullable=False),
            Column('data', String(30)))
        categories=Table('categories', dbmeta,
            Column('id', Integer,primary_key=True, nullable=False),
            Column('name', VARCHAR(20), index=True))
        tests = Table('tests', dbmeta ,
            Column('id', Integer, primary_key=True, nullable=False ),
            Column('owner_id',Integer, ForeignKey('owners.id'), nullable=False,index=True ),
            Column('category_id', Integer, ForeignKey('categories.id'),nullable=False,index=True ))
        options = Table('options', dbmeta ,
            Column('test_id', Integer, ForeignKey('tests.id'), primary_key=True, nullable=False ),
            Column('owner_id', Integer, ForeignKey('owners.id'), primary_key=True, nullable=False ),
            Column('someoption', Boolean, PassiveDefault(false), nullable=False ) )

        dbmeta.create_all()

        class Owner(object):
            pass
        class Category(object):
            pass
        class Test(object):
            pass
        class Option(object):
            pass
        mapper(Owner,owners)
        mapper(Category,categories)
        mapper(Option,options,properties={'owner':relation(Owner),'test':relation(Test)})
        mapper(Test,tests,properties={
            'owner':relation(Owner,backref='tests'),
            'category':relation(Category),
            'owner_option': relation(Option,primaryjoin=and_(tests.c.id==options.c.test_id,tests.c.owner_id==options.c.owner_id),
                foreign_keys=[options.c.test_id, options.c.owner_id],
            uselist=False )
        })

        s=create_session()

        # an owner
        o=Owner()
        s.add(o)

        # owner a has 3 tests, one of which he has specified options for
        c=Category()
        c.name='Some Category'
        s.add(c)

        for i in range(3):
            t=Test()
            t.owner=o
            t.category=c
            s.add(t)
            if i==1:
                op=Option()
                op.someoption=True
                t.owner_option=op
            if i==2:
                op=Option()
                t.owner_option=op

        s.flush()
        s.close()

    def tearDownAll(self):
        clear_mappers()
        dbmeta.drop_all()

    def test_noorm(self):
        """test the control case"""
        # I want to display a list of tests owned by owner 1
        # if someoption is false or he hasn't specified it yet (null)
        # but not if he set it to true (example someoption is for hiding)

        # desired output for owner 1
        # test_id, cat_name
        # 1 'Some Category'
        # 3  "

        # not orm style correct query
        print "Obtaining correct results without orm"
        result = select( [tests.c.id,categories.c.name],
            and_(tests.c.owner_id==1,or_(options.c.someoption==None,options.c.someoption==False)),
            order_by=[tests.c.id],
            from_obj=[tests.join(categories).outerjoin(options,and_(tests.c.id==options.c.test_id,tests.c.owner_id==options.c.owner_id))] ).execute().fetchall()
        print result
        assert result == [(1, u'Some Category'), (3, u'Some Category')]

    def test_withouteagerload(self):
        s = create_session()
        l = (s.query(Test).
             select_from(tests.outerjoin(options,
                                         and_(tests.c.id == options.c.test_id,
                                              tests.c.owner_id ==
                                              options.c.owner_id))).
             filter(and_(tests.c.owner_id==1,
                         or_(options.c.someoption==None,
                             options.c.someoption==False))))

        result = ["%d %s" % ( t.id,t.category.name ) for t in l]
        print result
        assert result == [u'1 Some Category', u'3 Some Category']

    def test_witheagerload(self):
        """test that an eagerload locates the correct "from" clause with
        which to attach to, when presented with a query that already has a complicated from clause."""
        s = create_session()
        q=s.query(Test).options(eagerload('category'))

        l=(q.select_from(tests.outerjoin(options,
                                         and_(tests.c.id ==
                                              options.c.test_id,
                                              tests.c.owner_id ==
                                              options.c.owner_id))).
           filter(and_(tests.c.owner_id==1,or_(options.c.someoption==None,
                                               options.c.someoption==False))))

        result = ["%d %s" % ( t.id,t.category.name ) for t in l]
        print result
        assert result == [u'1 Some Category', u'3 Some Category']

    def test_dslish(self):
        """test the same as witheagerload except using generative"""
        s = create_session()
        q=s.query(Test).options(eagerload('category'))
        l=q.filter (
            and_(tests.c.owner_id==1,or_(options.c.someoption==None,options.c.someoption==False))
            ).outerjoin('owner_option')

        result = ["%d %s" % ( t.id,t.category.name ) for t in l]
        print result
        assert result == [u'1 Some Category', u'3 Some Category']

    @testing.unsupported('sybase', 'FIXME: unknown, verify not fails_on')
    def test_withoutouterjoin_literal(self):
        s = create_session()
        q = s.query(Test).options(eagerload('category'))
        l = (q.filter(
            (tests.c.owner_id==1) &
            ('options.someoption is null or options.someoption=%s' % false)).
             join('owner_option'))

        result = ["%d %s" % ( t.id,t.category.name ) for t in l]
        print result
        assert result == [u'3 Some Category']

    def test_withoutouterjoin(self):
        s = create_session()
        q=s.query(Test).options(eagerload('category'))
        l = q.filter( (tests.c.owner_id==1) & ((options.c.someoption==None) | (options.c.someoption==False)) ).join('owner_option')
        result = ["%d %s" % ( t.id,t.category.name ) for t in l]
        print result
        assert result == [u'3 Some Category']

class EagerTest2(_base.ORMTest):
    def setUpAll(self):
        global metadata, middle, left, right
        metadata = MetaData(testing.db)
        middle = Table('middle', metadata,
            Column('id', Integer, primary_key = True),
            Column('data', String(50)),
        )

        left = Table('left', metadata,
            Column('id', Integer, ForeignKey(middle.c.id), primary_key=True),
            Column('tag', String(50), primary_key=True),
        )

        right = Table('right', metadata,
            Column('id', Integer, ForeignKey(middle.c.id), primary_key=True),
            Column('tag', String(50), primary_key=True),
        )
        metadata.create_all()
    def tearDownAll(self):
        metadata.drop_all()
    def tearDown(self):
        for t in metadata.table_iterator(reverse=True):
            t.delete().execute()

    @testing.fails_on('maxdb')
    def testeagerterminate(self):
        """test that eager query generation does not include the same mapper's table twice.

        or, that bi-directional eager loads dont include each other in eager query generation."""
        class Middle(object):
            def __init__(self, data): self.data = data
        class Left(object):
            def __init__(self, data): self.tag = data
        class Right(object):
            def __init__(self, data): self.tag = data

        # set up bi-directional eager loads
        mapper(Left, left)
        mapper(Right, right)
        mapper(Middle, middle, properties = {
            'left': relation(Left, lazy=False, backref=backref('middle',lazy=False)),
            'right': relation(Right, lazy=False, backref=backref('middle', lazy=False)),
            }
        )
        session = create_session(bind=testing.db)
        p = Middle('test1')
        p.left.append(Left('tag1'))
        p.right.append(Right('tag2'))
        session.add(p)
        session.flush()
        session.clear()
        obj = session.query(Left).filter_by(tag='tag1').one()
        print obj.middle.right[0]

class EagerTest3(_base.MappedTest):
    """test eager loading combined with nested SELECT statements, functions, and aggregates"""
    def define_tables(self, metadata):
        global datas, foo, stats
        datas=Table('datas',metadata,
         Column('id', Integer, primary_key=True,nullable=False ),
         Column('a', Integer , nullable=False ) )

        foo=Table('foo',metadata,
         Column('data_id', Integer, ForeignKey('datas.id'),nullable=False,primary_key=True ),
         Column('bar', Integer))

        stats=Table('stats', metadata,
        Column('id', Integer, primary_key=True, nullable=False ),
        Column('data_id', Integer, ForeignKey('datas.id')),
        Column('somedata', Integer, nullable=False ))

    @testing.fails_on('maxdb')
    def test_nesting_with_functions(self):
        class Data(object): pass
        class Foo(object):pass
        class Stat(object): pass

        Data.mapper=mapper(Data,datas)
        Foo.mapper=mapper(Foo,foo,properties={'data':relation(Data,backref=backref('foo',uselist=False))})
        Stat.mapper=mapper(Stat,stats,properties={'data':relation(Data)})

        s=create_session()
        data = []
        for x in range(5):
            d=Data()
            d.a=x
            s.add(d)
            data.append(d)

        for x in range(10):
            rid=random.randint(0,len(data) - 1)
            somedata=random.randint(1,50000)
            stat=Stat()
            stat.data = data[rid]
            stat.somedata=somedata
            s.add(stat)

        s.flush()

        arb_data=select(
            [stats.c.data_id,func.max(stats.c.somedata).label('max')],
            stats.c.data_id<=25,
            group_by=[stats.c.data_id]).alias('arb')

        arb_result = arb_data.execute().fetchall()
        # order the result list descending based on 'max'
        arb_result.sort(lambda a, b:cmp(b['max'],a['max']))
        # extract just the "data_id" from it
        arb_result = [row['data_id'] for row in arb_result]

        # now query for Data objects using that above select, adding the
        # "order by max desc" separately
        q=(s.query(Data).options(eagerload('foo')).
           select_from(datas.join(arb_data,arb_data.c.data_id==datas.c.id)).
           order_by(desc(arb_data.c.max)).
           limit(10))

        # extract "data_id" from the list of result objects
        verify_result = [d.id for d in q]

        # assert equality including ordering (may break if the DB "ORDER BY" and python's sort() used differing
        # algorithms and there are repeated 'somedata' values in the list)
        assert verify_result == arb_result

class EagerTest4(_base.MappedTest):
    def define_tables(self, metadata):
        global departments, employees
        departments = Table('departments', metadata,
                            Column('department_id', Integer, primary_key=True),
                            Column('name', String(50)))

        employees = Table('employees', metadata,
                          Column('person_id', Integer, primary_key=True),
                          Column('name', String(50)),
                          Column('department_id', Integer,
                                 ForeignKey('departments.department_id')))

    @testing.fails_on('maxdb')
    def test_basic(self):
        class Department(object):
            def __init__(self, **kwargs):
                for k, v in kwargs.iteritems():
                    setattr(self, k, v)
            def __repr__(self):
                return "<Department %s>" % (self.name,)

        class Employee(object):
            def __init__(self, **kwargs):
                for k, v in kwargs.iteritems():
                    setattr(self, k, v)
            def __repr__(self):
                return "<Employee %s>" % (self.name,)

        mapper(Employee, employees)
        mapper(Department, departments,
                      properties=dict(employees=relation(Employee,
                                                         lazy=False,
                                                         backref='department')))

        d1 = Department(name='One')
        for e in 'Jim Jack John Susan'.split():
            d1.employees.append(Employee(name=e))

        d2 = Department(name='Two')
        for e in 'Joe Bob Mary Wally'.split():
            d2.employees.append(Employee(name=e))

        sess = create_session()
        sess.add_all((d1, d2))
        sess.flush()

        q = sess.query(Department)
        q = q.join('employees').filter(Employee.name.startswith('J')).distinct().order_by([desc(Department.name)])
        assert q.count() == 2
        assert q[0] is d2

class EagerTest5(_base.MappedTest):
    """test the construction of AliasedClauses for the same eager load property but different
    parent mappers, due to inheritance"""
    def define_tables(self, metadata):
        global base, derived, derivedII, comments
        base = Table(
            'base', metadata,
            Column('uid', String(30), primary_key=True),
            Column('x', String(30))
            )

        derived = Table(
            'derived', metadata,
            Column('uid', String(30), ForeignKey(base.c.uid), primary_key=True),
            Column('y', String(30))
            )

        derivedII = Table(
            'derivedII', metadata,
            Column('uid', String(30), ForeignKey(base.c.uid), primary_key=True),
            Column('z', String(30))
            )

        comments = Table(
            'comments', metadata,
            Column('id', Integer, primary_key=True),
            Column('uid', String(30), ForeignKey(base.c.uid)),
            Column('comment', String(30))
            )
    def test_basic(self):
        class Base(object):
            def __init__(self, uid, x):
                self.uid = uid
                self.x = x

        class Derived(Base):
            def __init__(self, uid, x, y):
                self.uid = uid
                self.x = x
                self.y = y

        class DerivedII(Base):
            def __init__(self, uid, x, z):
                self.uid = uid
                self.x = x
                self.z = z

        class Comment(object):
            def __init__(self, uid, comment):
                self.uid = uid
                self.comment = comment


        commentMapper = mapper(Comment, comments)

        baseMapper = mapper(
            Base, base,
                properties={
                'comments': relation(
                    Comment, lazy=False, cascade='all, delete-orphan'
                    )
                }
            )

        derivedMapper = mapper(Derived, derived, inherits=baseMapper)
        derivedIIMapper = mapper(DerivedII, derivedII, inherits=baseMapper)
        sess = create_session()
        d = Derived('uid1', 'x', 'y')
        d.comments = [Comment('uid1', 'comment')]
        d2 = DerivedII('uid2', 'xx', 'z')
        d2.comments = [Comment('uid2', 'comment')]
        sess.add_all((d, d2))
        sess.flush()
        sess.clear()
        # this eager load sets up an AliasedClauses for the "comment" relationship,
        # then stores it in clauses_by_lead_mapper[mapper for Derived]
        d = sess.query(Derived).get('uid1')
        sess.clear()
        assert len([c for c in d.comments]) == 1

        # this eager load sets up an AliasedClauses for the "comment" relationship,
        # and should store it in clauses_by_lead_mapper[mapper for DerivedII].
        # the bug was that the previous AliasedClause create prevented this population
        # from occurring.
        d2 = sess.query(DerivedII).get('uid2')
        sess.clear()
        # object is not in the session; therefore the lazy load cant trigger here,
        # eager load had to succeed
        assert len([c for c in d2.comments]) == 1

class EagerTest6(_base.MappedTest):
    def define_tables(self, metadata):
        global designType, design, part, inheritedPart
        designType = Table('design_types', metadata,
            Column('design_type_id', Integer, primary_key=True),
            )

        design =Table('design', metadata,
            Column('design_id', Integer, primary_key=True),
            Column('design_type_id', Integer, ForeignKey('design_types.design_type_id')))

        part = Table('parts', metadata,
            Column('part_id', Integer, primary_key=True),
            Column('design_id', Integer, ForeignKey('design.design_id')),
            Column('design_type_id', Integer, ForeignKey('design_types.design_type_id')))

        inheritedPart = Table('inherited_part', metadata,
            Column('ip_id', Integer, primary_key=True),
            Column('part_id', Integer, ForeignKey('parts.part_id')),
            Column('design_id', Integer, ForeignKey('design.design_id')),
            )

    def testone(self):
        class Part(object):pass
        class Design(object):pass
        class DesignType(object):pass
        class InheritedPart(object):pass

        mapper(Part, part)

        mapper(InheritedPart, inheritedPart, properties=dict(
            part=relation(Part, lazy=False)
        ))

        mapper(Design, design, properties=dict(
            inheritedParts=relation(InheritedPart,
                                    cascade="all, delete-orphan",
                                    backref="design"),
        ))

        mapper(DesignType, designType, properties=dict(
        #   designs=relation(Design, private=True, backref="type"),
        ))

        class_mapper(Design).add_property("type", relation(DesignType, lazy=False, backref="designs"))

        class_mapper(Part).add_property("design", relation(Design, lazy=False, backref=backref("parts", cascade="all, delete-orphan")))

        #Part.mapper.add_property("designType", relation(DesignType))

        d = Design()
        sess = create_session()
        sess.add(d)
        sess.flush()
        sess.clear()
        x = sess.query(Design).get(1)
        x.inheritedParts

class EagerTest7(_base.MappedTest):
    def define_tables(self, metadata):
        global companies_table, addresses_table, invoice_table, phones_table, items_table, ctx
        global Company, Address, Phone, Item,Invoice

        ctx = scoped_session(create_session)

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

        class Company(object):
            def __init__(self):
                self.company_id = None
            def __repr__(self):
                return "Company:" + repr(getattr(self, 'company_id', None)) + " " + repr(getattr(self, 'company_name', None)) + " " + str([repr(addr) for addr in self.addresses])

        class Address(object):
            def __repr__(self):
                return "Address: " + repr(getattr(self, 'address_id', None)) + " " + repr(getattr(self, 'company_id', None)) + " " + repr(self.address) + str([repr(ph) for ph in getattr(self, 'phones', [])])

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

    def testone(self):
        """tests eager load of a many-to-one attached to a one-to-many.  this testcase illustrated
        the bug, which is that when the single Company is loaded, no further processing of the rows
        occurred in order to load the Company's second Address object."""

        mapper(Address, addresses_table, properties={
            }, extension=ctx.extension)
        mapper(Company, companies_table, properties={
            'addresses' : relation(Address, lazy=False),
            }, extension=ctx.extension)
        mapper(Invoice, invoice_table, properties={
            'company': relation(Company, lazy=False, )
            }, extension=ctx.extension)

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

        ctx.flush()

        company_id = c1.company_id
        invoice_id = i1.invoice_id

        ctx.clear()

        c = ctx.query(Company).get(company_id)

        ctx.clear()

        i = ctx.query(Invoice).get(invoice_id)

        print repr(c)
        print repr(i.company)
        self.assert_(repr(c) == repr(i.company))

    def testtwo(self):
        """this is the original testcase that includes various complicating factors"""

        mapper(Phone, phones_table, extension=ctx.extension)

        mapper(Address, addresses_table, properties={
            'phones': relation(Phone, lazy=False, backref='address', order_by=phones_table.default_order_by())
            }, extension=ctx.extension)

        mapper(Company, companies_table, properties={
            'addresses' : relation(Address, lazy=False, backref='company', order_by=addresses_table.default_order_by()),
            }, extension=ctx.extension)

        mapper(Item, items_table, extension=ctx.extension)

        mapper(Invoice, invoice_table, properties={
            'items': relation(Item, lazy=False, backref='invoice', order_by=items_table.default_order_by()),
            'company': relation(Company, lazy=False, backref='invoices')
            }, extension=ctx.extension)

        ctx.clear()
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

        ctx.flush()

        company_id = c1.company_id

        ctx.clear()

        a = ctx.query(Company).get(company_id)
        print repr(a)

        # set up an invoice
        i1 = Invoice()
        i1.date = datetime.datetime.now()
        i1.company = a

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

        ctx.flush()

        invoice_id = i1.invoice_id

        ctx.clear()

        c = ctx.query(Company).get(company_id)
        print repr(c)

        ctx.clear()

        i = ctx.query(Invoice).get(invoice_id)

        assert repr(i.company) == repr(c), repr(i.company) +  " does not match " + repr(c)

class EagerTest8(_base.MappedTest):
    def define_tables(self, metadata):
        global project_t, task_t, task_status_t, task_type_t, message_t, message_type_t

        project_t = Table('prj', metadata,
                          Column('id',            Integer,      primary_key=True),
                          Column('created',       DateTime ,    ),
                          Column('title',         Unicode(100)),
                          )

        task_t = Table('task', metadata,
                          Column('id',            Integer,      primary_key=True),
                          Column('status_id',     Integer,      ForeignKey('task_status.id'), nullable=False),
                          Column('title',         Unicode(100)),
                          Column('task_type_id',  Integer ,     ForeignKey('task_type.id'), nullable=False),
                          Column('prj_id',        Integer ,     ForeignKey('prj.id'), nullable=False),
                          )

        task_status_t = Table('task_status', metadata,
                                Column('id',                Integer,      primary_key=True),
                                )

        task_type_t = Table('task_type', metadata,
                            Column('id',   Integer,    primary_key=True),
                            )

        message_t  = Table('msg', metadata,
                            Column('id', Integer,  primary_key=True),
                            Column('posted',    DateTime, index=True,),
                            Column('type_id',   Integer, ForeignKey('msg_type.id')),
                            Column('task_id',   Integer, ForeignKey('task.id')),
                            )

        message_type_t = Table('msg_type', metadata,
                                Column('id',                Integer,      primary_key=True),
                                Column('name',              Unicode(20)),
                                Column('display_name',      Unicode(20)),
                                )

    def setUp(self):
        testing.db.execute(project_t.insert(), {'id':1})
        testing.db.execute(task_status_t.insert(), {'id':1})
        testing.db.execute(task_type_t.insert(), {'id':1})
        testing.db.execute(task_t.insert(), {'title':u'task 1', 'task_type_id':1, 'status_id':1, 'prj_id':1})

    @testing.fails_on('maxdb')
    def test_nested_joins(self):
        # this is testing some subtle column resolution stuff,
        # concerning corresponding_column() being extremely accurate
        # as well as how mapper sets up its column properties

        class Task(object):pass
        class Task_Type(object):pass
        class Message(object):pass
        class Message_Type(object):pass

        tsk_cnt_join = outerjoin(project_t, task_t, task_t.c.prj_id==project_t.c.id)

        ss = select([project_t.c.id.label('prj_id'), func.count(task_t.c.id).label('tasks_number')],
                    from_obj=[tsk_cnt_join], group_by=[project_t.c.id]).alias('prj_tsk_cnt_s')
        j = join(project_t, ss, project_t.c.id == ss.c.prj_id)

        mapper(Task_Type, task_type_t)

        mapper( Task, task_t,
                              properties=dict(type=relation(Task_Type, lazy=False),
                                             ))

        mapper(Message_Type, message_type_t)

        mapper(Message, message_t,
                         properties=dict(type=relation(Message_Type, lazy=False, uselist=False),
                                         ))

        tsk_cnt_join = outerjoin(project_t, task_t, task_t.c.prj_id==project_t.c.id)
        ss = select([project_t.c.id.label('prj_id'), func.count(task_t.c.id).label('tasks_number')],
                    from_obj=[tsk_cnt_join], group_by=[project_t.c.id]).alias('prj_tsk_cnt_s')
        j = join(project_t, ss, project_t.c.id == ss.c.prj_id)

        j  = outerjoin( task_t, message_t, task_t.c.id==message_t.c.task_id)
        jj = select([ task_t.c.id.label('task_id'),
                      func.count(message_t.c.id).label('props_cnt')],
                      from_obj=[j], group_by=[task_t.c.id]).alias('prop_c_s')
        jjj = join(task_t, jj, task_t.c.id == jj.c.task_id)

        class cls(object):pass

        props =dict(type=relation(Task_Type, lazy=False))
        print [c.key for c in jjj.c]
        cls.mapper = mapper( cls, jjj, properties=props)

        session = create_session()

        for t in session.query(cls.mapper).limit(10).offset(0).all():
            print t.id, t.title, t.props_cnt

class EagerTest9(_base.MappedTest):
    """test the usage of query options to eagerly load specific paths.

    this relies upon the 'path' construct used by PropertyOption to relate
    LoaderStrategies to specific paths, as well as the path state maintained
    throughout the query setup/mapper instances process.
    """

    def define_tables(self, metadata):
        global accounts_table, transactions_table, entries_table
        accounts_table = Table('accounts', metadata,
            Column('account_id', Integer, primary_key=True),
            Column('name', String(40)),
        )
        transactions_table = Table('transactions', metadata,
            Column('transaction_id', Integer, primary_key=True),
            Column('name', String(40)),
        )
        entries_table = Table('entries', metadata,
            Column('entry_id', Integer, primary_key=True),
            Column('name', String(40)),
            Column('account_id', Integer, ForeignKey(accounts_table.c.account_id)),
            Column('transaction_id', Integer, ForeignKey(transactions_table.c.transaction_id)),
        )

    @testing.fails_on('maxdb')
    def test_eagerload_on_path(self):
        class Account(_base.BasicEntity):
            pass

        class Transaction(_base.BasicEntity):
            pass

        class Entry(_base.BasicEntity):
            pass

        mapper(Account, accounts_table)
        mapper(Transaction, transactions_table)
        mapper(Entry, entries_table, properties = dict(
            account = relation(Account, uselist=False, backref=backref('entries', lazy=True, order_by=entries_table.c.entry_id)),
            transaction = relation(Transaction, uselist=False, backref=backref('entries', lazy=False, order_by=entries_table.c.entry_id)),
        ))

        session = create_session()

        tx1 = Transaction(name='tx1')
        tx2 = Transaction(name='tx2')

        acc1 = Account(name='acc1')
        ent11 = Entry(name='ent11', account=acc1, transaction=tx1)
        ent12 = Entry(name='ent12', account=acc1, transaction=tx2)

        acc2 = Account(name='acc2')
        ent21 = Entry(name='ent21', account=acc2, transaction=tx1)
        ent22 = Entry(name='ent22', account=acc2, transaction=tx2)

        session.add(acc1)
        session.flush()
        session.clear()

        def go():
            # load just the first Account.  eager loading will actually load all objects saved thus far,
            # but will not eagerly load the "accounts" off the immediate "entries"; only the
            # "accounts" off the entries->transaction->entries
            acc = session.query(Account).options(eagerload_all('entries.transaction.entries.account')).order_by(Account.account_id).first()

            # no sql occurs
            assert acc.name == 'acc1'
            assert acc.entries[0].transaction.entries[0].account.name == 'acc1'
            assert acc.entries[0].transaction.entries[1].account.name == 'acc2'

            # lazyload triggers but no sql occurs because many-to-one uses cached query.get()
            for e in acc.entries:
                assert e.account is acc

        self.assert_sql_count(testing.db, go, 1)



if __name__ == "__main__":
    testenv.main()
