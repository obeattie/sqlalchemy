import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.orm import exc as orm_exc
from testlib import *
from testlib import sa, testing
from orm import _base
from sqlalchemy.orm import attributes
from testlib.testing import eq_

class Employee(object):
    def __init__(self, name):
        self.name = name
    def __repr__(self):
        return self.__class__.__name__ + " " + self.name

class Manager(Employee):
    def __init__(self, name, manager_data):
        self.name = name
        self.manager_data = manager_data
    def __repr__(self):
        return self.__class__.__name__ + " " + self.name + " " +  self.manager_data

class Engineer(Employee):
    def __init__(self, name, engineer_info):
        self.name = name
        self.engineer_info = engineer_info
    def __repr__(self):
        return self.__class__.__name__ + " " + self.name + " " +  self.engineer_info

class Hacker(Engineer):
    def __init__(self, name, nickname, engineer_info):
        self.name = name
        self.nickname = nickname
        self.engineer_info = engineer_info
    def __repr__(self):
        return self.__class__.__name__ + " " + self.name + " '" + \
               self.nickname + "' " +  self.engineer_info

class Company(object):
   pass


class ConcreteTest(_base.MappedTest):
    def define_tables(self, metadata):
        global managers_table, engineers_table, hackers_table, companies, employees_table

        companies = Table('companies', metadata,
           Column('id', Integer, primary_key=True),
           Column('name', String(50)))

        employees_table = Table('employees', metadata,
            Column('employee_id', Integer, primary_key=True),
            Column('name', String(50)),
            Column('company_id', Integer, ForeignKey('companies.id'))
        )
        
        managers_table = Table('managers', metadata,
            Column('employee_id', Integer, primary_key=True),
            Column('name', String(50)),
            Column('manager_data', String(50)),
            Column('company_id', Integer, ForeignKey('companies.id'))
        )

        engineers_table = Table('engineers', metadata,
            Column('employee_id', Integer, primary_key=True),
            Column('name', String(50)),
            Column('engineer_info', String(50)),
            Column('company_id', Integer, ForeignKey('companies.id'))
        )

        hackers_table = Table('hackers', metadata,
            Column('employee_id', Integer, primary_key=True),
            Column('name', String(50)),
            Column('engineer_info', String(50)),
            Column('company_id', Integer, ForeignKey('companies.id')),
            Column('nickname', String(50))
        )
        
        

    def test_basic(self):
        pjoin = polymorphic_union({
            'manager':managers_table,
            'engineer':engineers_table
        }, 'type', 'pjoin')

        employee_mapper = mapper(Employee, pjoin, polymorphic_on=pjoin.c.type)
        manager_mapper = mapper(Manager, managers_table, inherits=employee_mapper, 
            concrete=True, polymorphic_identity='manager')
        engineer_mapper = mapper(Engineer, engineers_table, inherits=employee_mapper, 
            concrete=True, polymorphic_identity='engineer')

        session = create_session()
        session.save(Manager('Tom', 'knows how to manage things'))
        session.save(Engineer('Kurt', 'knows how to hack'))
        session.flush()
        session.clear()

        assert set([repr(x) for x in session.query(Employee)]) == set(["Engineer Kurt knows how to hack", "Manager Tom knows how to manage things"])
        assert set([repr(x) for x in session.query(Manager)]) == set(["Manager Tom knows how to manage things"])
        assert set([repr(x) for x in session.query(Engineer)]) == set(["Engineer Kurt knows how to hack"])

        manager = session.query(Manager).one()
        session.expire(manager, ['manager_data'])
        self.assertEquals(manager.manager_data, "knows how to manage things")

    def test_multi_level_no_base(self):
        pjoin = polymorphic_union({
            'manager': managers_table,
            'engineer': engineers_table,
            'hacker': hackers_table
        }, 'type', 'pjoin')

        pjoin2 = polymorphic_union({
            'engineer': engineers_table,
            'hacker': hackers_table
        }, 'type', 'pjoin2')

        employee_mapper = mapper(Employee, pjoin, polymorphic_on=pjoin.c.type)
        manager_mapper = mapper(Manager, managers_table, 
                                inherits=employee_mapper, concrete=True, 
                                polymorphic_identity='manager')
        engineer_mapper = mapper(Engineer, engineers_table, 
                                 with_polymorphic=('*', pjoin2), 
                                 polymorphic_on=pjoin2.c.type,
                                 inherits=employee_mapper, concrete=True,
                                 polymorphic_identity='engineer')
        hacker_mapper = mapper(Hacker, hackers_table, 
                               inherits=engineer_mapper,
                               concrete=True, polymorphic_identity='hacker')

        session = create_session()
        tom = Manager('Tom', 'knows how to manage things')
        jerry = Engineer('Jerry', 'knows how to program')
        hacker = Hacker('Kurt', 'Badass', 'knows how to hack')
        session.add_all((tom, jerry, hacker))
        session.flush()

        # ensure "readonly" on save logic didn't pollute the expired_attributes
        # collection
        assert 'nickname' not in attributes.instance_state(jerry).expired_attributes
        assert 'name' not in attributes.instance_state(jerry).expired_attributes
        assert 'name' not in attributes.instance_state(hacker).expired_attributes
        assert 'nickname' not in attributes.instance_state(hacker).expired_attributes
        def go():
            self.assertEquals(jerry.name, "Jerry")
            self.assertEquals(hacker.nickname, "Badass")
        self.assert_sql_count(testing.db, go, 0)
        
        session.clear()

        assert repr(session.query(Employee).filter(Employee.name=='Tom').one()) == "Manager Tom knows how to manage things"
        assert repr(session.query(Manager).filter(Manager.name=='Tom').one()) == "Manager Tom knows how to manage things"
        
        
        assert set([repr(x) for x in session.query(Employee).all()]) == set(["Engineer Jerry knows how to program", "Manager Tom knows how to manage things", "Hacker Kurt 'Badass' knows how to hack"])
        assert set([repr(x) for x in session.query(Manager).all()]) == set(["Manager Tom knows how to manage things"])
        assert set([repr(x) for x in session.query(Engineer).all()]) == set(["Engineer Jerry knows how to program", "Hacker Kurt 'Badass' knows how to hack"])
        assert set([repr(x) for x in session.query(Hacker).all()]) == set(["Hacker Kurt 'Badass' knows how to hack"])

    def test_multi_level_with_base(self):
        pjoin = polymorphic_union({
            'employee':employees_table,
            'manager': managers_table,
            'engineer': engineers_table,
            'hacker': hackers_table
        }, 'type', 'pjoin')

        pjoin2 = polymorphic_union({
            'engineer': engineers_table,
            'hacker': hackers_table
        }, 'type', 'pjoin2')

        employee_mapper = mapper(Employee, employees_table, 
                with_polymorphic=('*', pjoin), polymorphic_on=pjoin.c.type)
        manager_mapper = mapper(Manager, managers_table, 
                                inherits=employee_mapper, concrete=True, 
                                polymorphic_identity='manager')
        engineer_mapper = mapper(Engineer, engineers_table, 
                                 with_polymorphic=('*', pjoin2), 
                                 polymorphic_on=pjoin2.c.type,
                                 inherits=employee_mapper, concrete=True,
                                 polymorphic_identity='engineer')
        hacker_mapper = mapper(Hacker, hackers_table, 
                               inherits=engineer_mapper,
                               concrete=True, polymorphic_identity='hacker')

        session = create_session()
        tom = Manager('Tom', 'knows how to manage things')
        jerry = Engineer('Jerry', 'knows how to program')
        hacker = Hacker('Kurt', 'Badass', 'knows how to hack')
        session.add_all((tom, jerry, hacker))
        session.flush()

        def go():
            self.assertEquals(jerry.name, "Jerry")
            self.assertEquals(hacker.nickname, "Badass")
        self.assert_sql_count(testing.db, go, 0)

        session.clear()

        # check that we aren't getting a cartesian product in the raw SQL.
        # this requires that Engineer's polymorphic discriminator is not rendered
        # in the statement which is only against Employee's "pjoin"
        assert len(testing.db.execute(session.query(Employee).with_labels().statement).fetchall()) == 3
        
        assert set([repr(x) for x in session.query(Employee)]) == set(["Engineer Jerry knows how to program", "Manager Tom knows how to manage things", "Hacker Kurt 'Badass' knows how to hack"])
        assert set([repr(x) for x in session.query(Manager)]) == set(["Manager Tom knows how to manage things"])
        assert set([repr(x) for x in session.query(Engineer)]) == set(["Engineer Jerry knows how to program", "Hacker Kurt 'Badass' knows how to hack"])
        assert set([repr(x) for x in session.query(Hacker)]) == set(["Hacker Kurt 'Badass' knows how to hack"])

    
    def test_without_default_polymorphic(self):
        pjoin = polymorphic_union({
            'employee':employees_table,
            'manager': managers_table,
            'engineer': engineers_table,
            'hacker': hackers_table
        }, 'type', 'pjoin')

        pjoin2 = polymorphic_union({
            'engineer': engineers_table,
            'hacker': hackers_table
        }, 'type', 'pjoin2')

        employee_mapper = mapper(Employee, employees_table, 
                                polymorphic_identity='employee')
        manager_mapper = mapper(Manager, managers_table, 
                                inherits=employee_mapper, concrete=True, 
                                polymorphic_identity='manager')
        engineer_mapper = mapper(Engineer, engineers_table, 
                                 inherits=employee_mapper, concrete=True,
                                 polymorphic_identity='engineer')
        hacker_mapper = mapper(Hacker, hackers_table, 
                               inherits=engineer_mapper,
                               concrete=True, polymorphic_identity='hacker')

        session = create_session()
        jdoe = Employee('Jdoe')
        tom = Manager('Tom', 'knows how to manage things')
        jerry = Engineer('Jerry', 'knows how to program')
        hacker = Hacker('Kurt', 'Badass', 'knows how to hack')
        session.add_all((jdoe, tom, jerry, hacker))
        session.flush()

        eq_(
            len(testing.db.execute(session.query(Employee).with_polymorphic('*', pjoin, pjoin.c.type).with_labels().statement).fetchall()),
            4
        )
        
        eq_(
            session.query(Employee).get(jdoe.employee_id), jdoe
        )
        eq_(
            session.query(Engineer).get(jerry.employee_id), jerry
        )
        eq_(
            set([repr(x) for x in session.query(Employee).with_polymorphic('*', pjoin, pjoin.c.type)]),
            set(["Employee Jdoe", "Engineer Jerry knows how to program", "Manager Tom knows how to manage things", "Hacker Kurt 'Badass' knows how to hack"])
        )
        eq_(
            set([repr(x) for x in session.query(Manager)]),
            set(["Manager Tom knows how to manage things"])
        )
        eq_(
            set([repr(x) for x in session.query(Engineer).with_polymorphic('*', pjoin2, pjoin2.c.type)]),
            set(["Engineer Jerry knows how to program", "Hacker Kurt 'Badass' knows how to hack"])
        )
        eq_(
            set([repr(x) for x in session.query(Hacker)]),
            set(["Hacker Kurt 'Badass' knows how to hack"])
        )
        # test adaption of the column by wrapping the query in a subquery
        eq_(
            len(testing.db.execute(
                session.query(Engineer).with_polymorphic('*', pjoin2, pjoin2.c.type).from_self().statement
            ).fetchall()),
            2
        )
        eq_(
            set([repr(x) for x in session.query(Engineer).with_polymorphic('*', pjoin2, pjoin2.c.type).from_self()]),
            set(["Engineer Jerry knows how to program", "Hacker Kurt 'Badass' knows how to hack"])
        )
        
    def test_relation(self):
        pjoin = polymorphic_union({
            'manager':managers_table,
            'engineer':engineers_table
        }, 'type', 'pjoin')

        mapper(Company, companies, properties={
            'employees':relation(Employee)
        })
        employee_mapper = mapper(Employee, pjoin, polymorphic_on=pjoin.c.type)
        manager_mapper = mapper(Manager, managers_table, inherits=employee_mapper, concrete=True, polymorphic_identity='manager')
        engineer_mapper = mapper(Engineer, engineers_table, inherits=employee_mapper, concrete=True, polymorphic_identity='engineer')

        session = create_session()
        c = Company()
        c.employees.append(Manager('Tom', 'knows how to manage things'))
        c.employees.append(Engineer('Kurt', 'knows how to hack'))
        session.save(c)
        session.flush()
        session.clear()

        def go():
            c2 = session.query(Company).get(c.id)
            assert set([repr(x) for x in c2.employees]) == set(["Engineer Kurt knows how to hack", "Manager Tom knows how to manage things"])
        self.assert_sql_count(testing.db, go, 2)
        session.clear()
        def go():
            c2 = session.query(Company).options(eagerload(Company.employees)).get(c.id)
            assert set([repr(x) for x in c2.employees]) == set(["Engineer Kurt knows how to hack", "Manager Tom knows how to manage things"])
        self.assert_sql_count(testing.db, go, 1)

class PropertyInheritanceTest(_base.MappedTest):
    def define_tables(self, metadata):
        Table('a_table', metadata,
            Column('id', Integer, primary_key=True),
            Column('some_c_id', Integer, ForeignKey('c_table.id')),
            Column('aname', String(50)),
        )
        Table('b_table', metadata,
            Column('id', Integer, primary_key=True),
            Column('some_c_id', Integer, ForeignKey('c_table.id')),
            Column('bname', String(50)),
        )
        Table('c_table', metadata,
            Column('id', Integer, primary_key=True),
            Column('cname', String(50)),
            
        )
        
    def setup_classes(self):
        class A(_base.ComparableEntity):
            pass

        class B(A):
            pass

        class C(_base.ComparableEntity):
            pass
    
    @testing.resolve_artifact_names    
    def test_noninherited_warning(self):
        mapper(A, a_table, properties={
            'some_c':relation(C)
        })
        mapper(B, b_table,inherits=A, concrete=True)
        mapper(C, c_table)

        b = B()
        c = C()
        self.assertRaises(AttributeError, setattr, b, 'some_c', c)

        clear_mappers()
        mapper(A, a_table, properties={
            'a_id':a_table.c.id
        })
        mapper(B, b_table,inherits=A, concrete=True)
        mapper(C, c_table)
        b = B()
        self.assertRaises(AttributeError, setattr, b, 'a_id', 3)

        clear_mappers()
        mapper(A, a_table, properties={
            'a_id':a_table.c.id
        })
        mapper(B, b_table,inherits=A, concrete=True)
        mapper(C, c_table)
        
    @testing.resolve_artifact_names    
    def test_inheriting(self):
        mapper(A, a_table, properties={
            'some_c':relation(C, back_populates='many_a')
        })
        mapper(B, b_table,inherits=A, concrete=True, properties={
            'some_c':relation(C, back_populates='many_b')
        })
        mapper(C, c_table, properties={
            'many_a':relation(A, back_populates='some_c'),
            'many_b':relation(B, back_populates='some_c'),
        })
        
        sess = sessionmaker()()
        
        c1 = C(cname='c1')
        c2 = C(cname='c2')
        a1 = A(some_c=c1, aname='a1')
        a2 = A(some_c=c2, aname='a2')
        b1 = B(some_c=c1, bname='b1')
        b2 = B(some_c=c1, bname='b2')
        
        self.assertRaises(AttributeError, setattr, b1, 'aname', 'foo')
        self.assertRaises(AttributeError, getattr, A, 'bname')
        
        assert c2.many_a == [a2]
        assert c1.many_a == [a1]
        assert c1.many_b == [b1, b2]
        
        sess.add_all([c1, c2])
        sess.commit()

        assert sess.query(C).filter(C.many_a.contains(a2)).one() is c2
        assert c2.many_a == [a2]
        assert c1.many_a == [a1]
        assert c1.many_b == [b1, b2]

        assert sess.query(B).filter(B.bname=='b1').one() is b1
        
    @testing.resolve_artifact_names    
    def test_polymorphic_backref(self):
        """test multiple backrefs to the same polymorphically-loading attribute."""
        
        ajoin = polymorphic_union(
            {'a':a_table,
            'b':b_table
            }, 'type', 'ajoin'
        )
        mapper(A, a_table, with_polymorphic=('*', ajoin), 
            polymorphic_on=ajoin.c.type, polymorphic_identity='a', 
            properties={
            'some_c':relation(C, back_populates='many_a')
        })
        mapper(B, b_table,inherits=A, concrete=True, 
            polymorphic_identity='b', 
            properties={
            'some_c':relation(C, back_populates='many_a')
        })
        mapper(C, c_table, properties={
            'many_a':relation(A, back_populates='some_c', order_by=ajoin.c.id),
        })
        
        sess = sessionmaker()()
        
        c1 = C(cname='c1')
        c2 = C(cname='c2')
        a1 = A(some_c=c1, aname='a1')
        a2 = A(some_c=c2, aname='a2')
        b1 = B(some_c=c1, bname='b1')
        b2 = B(some_c=c1, bname='b2')
        
        eq_([a2], c2.many_a)
        eq_([a1, b1, b2], c1.many_a)
        
        sess.add_all([c1, c2])
        sess.commit()

        assert sess.query(C).filter(C.many_a.contains(a2)).one() is c2
        assert sess.query(C).filter(C.many_a.contains(b1)).one() is c1
        eq_([A(aname='a2')], c2.many_a)
        eq_([A(aname='a1'), B(bname='b1'), B(bname='b2')], c1.many_a)
        
        sess.expire_all()
        
        def go():
            eq_(
                [C(many_a=[A(aname='a1'), B(bname='b1'), B(bname='b2')]), C(many_a=[A(aname='a2')])],
                sess.query(C).options(eagerload(C.many_a)).order_by(C.id).all(),
            )
        self.assert_sql_count(testing.db, go, 1)
        
    
class ColKeysTest(_base.MappedTest):
    def define_tables(self, metadata):
        global offices_table, refugees_table
        refugees_table = Table('refugee', metadata,
           Column('refugee_fid', Integer, primary_key=True),
           Column('refugee_name', Unicode(30), key='name'))

        offices_table = Table('office', metadata,
           Column('office_fid', Integer, primary_key=True),
           Column('office_name', Unicode(30), key='name'))
    
    def insert_data(self):
        refugees_table.insert().execute(
            dict(refugee_fid=1, name=u"refugee1"),
            dict(refugee_fid=2, name=u"refugee2")
        )
        offices_table.insert().execute(
            dict(office_fid=1, name=u"office1"),
            dict(office_fid=2, name=u"office2")
        )
    
    def test_keys(self):
        pjoin = polymorphic_union({
           'refugee': refugees_table,
           'office': offices_table
        }, 'type', 'pjoin')
        class Location(object):
           pass

        class Refugee(Location):
           pass

        class Office(Location):
           pass

        location_mapper = mapper(Location, pjoin, polymorphic_on=pjoin.c.type,
                                polymorphic_identity='location')
        office_mapper   = mapper(Office, offices_table, inherits=location_mapper,
                                concrete=True, polymorphic_identity='office')
        refugee_mapper  = mapper(Refugee, refugees_table, inherits=location_mapper,
                                concrete=True, polymorphic_identity='refugee')

        sess = create_session()
        eq_(sess.query(Refugee).get(1).name, "refugee1")
        eq_(sess.query(Refugee).get(2).name, "refugee2")

        eq_(sess.query(Office).get(1).name, "office1")
        eq_(sess.query(Office).get(2).name, "office2")

if __name__ == '__main__':
    testenv.main()
