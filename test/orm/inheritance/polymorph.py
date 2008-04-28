"""tests basic polymorphic mapper loading/saving, minimal relations"""

import testenv; testenv.configure_for_tests()
import sets
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.orm import exc as orm_exc
from testlib import *
from testlib import fixtures

class Person(fixtures.Base):
    pass
class Engineer(Person):
    pass
class Manager(Person):
    pass
class Boss(Manager):
    pass
class Company(fixtures.Base):
    pass

class PolymorphTest(ORMTest):
    def define_tables(self, metadata):
        global companies, people, engineers, managers, boss

        companies = Table('companies', metadata,
           Column('company_id', Integer, primary_key=True, test_needs_autoincrement=True),
           Column('name', String(50)))

        people = Table('people', metadata,
           Column('person_id', Integer, primary_key=True, test_needs_autoincrement=True),
           Column('company_id', Integer, ForeignKey('companies.company_id')),
           Column('name', String(50)),
           Column('type', String(30)))

        engineers = Table('engineers', metadata,
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('status', String(30)),
           Column('engineer_name', String(50)),
           Column('primary_language', String(50)),
          )

        managers = Table('managers', metadata,
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('status', String(30)),
           Column('manager_name', String(50))
           )

        boss = Table('boss', metadata,
            Column('boss_id', Integer, ForeignKey('managers.person_id'), primary_key=True),
            Column('golf_swing', String(30)),
            )

        metadata.create_all()

class InsertOrderTest(PolymorphTest):
    def test_insert_order(self):
        """test that classes of multiple types mix up mapper inserts
        so that insert order of individual tables is maintained"""
        person_join = polymorphic_union(
            {
                'engineer':people.join(engineers),
                'manager':people.join(managers),
                'person':people.select(people.c.type=='person'),
            }, None, 'pjoin')

        person_mapper = mapper(Person, people, select_table=person_join, polymorphic_on=person_join.c.type, polymorphic_identity='person')

        mapper(Engineer, engineers, inherits=person_mapper, polymorphic_identity='engineer')
        mapper(Manager, managers, inherits=person_mapper, polymorphic_identity='manager')
        mapper(Company, companies, properties={
            'employees': relation(Person,
                                  backref='company',
                                  order_by=person_join.c.person_id)
        })

        session = create_session()
        c = Company(name='company1')
        c.employees.append(Manager(status='AAB', manager_name='manager1', name='pointy haired boss'))
        c.employees.append(Engineer(status='BBA', engineer_name='engineer1', primary_language='java', name='dilbert'))
        c.employees.append(Person(status='HHH', name='joesmith'))
        c.employees.append(Engineer(status='CGG', engineer_name='engineer2', primary_language='python', name='wally'))
        c.employees.append(Manager(status='ABA', manager_name='manager2', name='jsmith'))
        session.save(c)
        session.flush()
        session.clear()
        self.assertEquals(session.query(Company).get(c.company_id), c)

class RelationToSubclassTest(PolymorphTest):
    def test_basic(self):
        """test a relation to an inheriting mapper where the relation is to a subclass
        but the join condition is expressed by the parent table.

        also test that backrefs work in this case.

        this test touches upon a lot of the join/foreign key determination code in properties.py
        and creates the need for properties.py to search for conditions individually within
        the mapper's local table as well as the mapper's 'mapped' table, so that relations
        requiring lots of specificity (like self-referential joins) as well as relations requiring
        more generalization (like the example here) both come up with proper results."""

        mapper(Person, people)

        mapper(Engineer, engineers, inherits=Person)
        mapper(Manager, managers, inherits=Person)

        mapper(Company, companies, properties={
            'managers': relation(Manager, backref="company")
        })

        sess = create_session()

        c = Company(name='company1')
        c.managers.append(Manager(status='AAB', manager_name='manager1', name='pointy haired boss'))
        sess.save(c)
        sess.flush()
        sess.clear()

        self.assertEquals(sess.query(Company).filter_by(company_id=c.company_id).one(), c)
        assert c.managers[0].company is c

class RoundTripTest(PolymorphTest):
    pass

def generate_round_trip_test(include_base, lazy_relation, redefine_colprop, with_polymorphic):
    """generates a round trip test.

    include_base - whether or not to include the base 'person' type in the union.
    lazy_relation - whether or not the Company relation to People is lazy or eager.
    redefine_colprop - if we redefine the 'name' column to be 'people_name' on the base Person class
    use_literal_join - primary join condition is explicitly specified
    """
    def test_roundtrip(self):
        if with_polymorphic == 'unions':
            if include_base:
                person_join = polymorphic_union(
                    {
                        'engineer':people.join(engineers),
                        'manager':people.join(managers),
                        'person':people.select(people.c.type=='person'),
                    }, None, 'pjoin')
            else:
                person_join = polymorphic_union(
                    {
                        'engineer':people.join(engineers),
                        'manager':people.join(managers),
                    }, None, 'pjoin')
                
            manager_join = people.join(managers).outerjoin(boss)
            person_with_polymorphic = ['*', person_join]
            manager_with_polymorphic = ['*', manager_join]
        elif with_polymorphic == 'joins':
            person_join = people.outerjoin(engineers).outerjoin(managers).outerjoin(boss)
            manager_join = people.join(managers).outerjoin(boss)
            person_with_polymorphic = ['*', person_join]
            manager_with_polymorphic = ['*', manager_join]
        elif with_polymorphic == 'auto':
            person_with_polymorphic = '*'
            manager_with_polymorphic = '*'
        else:
            person_with_polymorphic = None
            manager_with_polymorphic = None

        if redefine_colprop:
            person_mapper = mapper(Person, people, with_polymorphic=person_with_polymorphic, polymorphic_on=people.c.type, polymorphic_identity='person', properties= {'person_name':people.c.name})
        else:
            person_mapper = mapper(Person, people, with_polymorphic=person_with_polymorphic, polymorphic_on=people.c.type, polymorphic_identity='person')

        mapper(Engineer, engineers, inherits=person_mapper, polymorphic_identity='engineer')
        mapper(Manager, managers, inherits=person_mapper, with_polymorphic=manager_with_polymorphic, polymorphic_identity='manager')

        mapper(Boss, boss, inherits=Manager, polymorphic_identity='boss')

        mapper(Company, companies, properties={
            'employees': relation(Person, lazy=lazy_relation,
                                  cascade="all, delete-orphan",
            backref="company", order_by=people.c.person_id
            )
        })

        if redefine_colprop:
            person_attribute_name = 'person_name'
        else:
            person_attribute_name = 'name'

        employees = [
                Manager(status='AAB', manager_name='manager1', **{person_attribute_name:'pointy haired boss'}),
                Engineer(status='BBA', engineer_name='engineer1', primary_language='java', **{person_attribute_name:'dilbert'}),
            ]
        if include_base:
            employees.append(Person(**{person_attribute_name:'joesmith'}))
        employees += [
            Engineer(status='CGG', engineer_name='engineer2', primary_language='python', **{person_attribute_name:'wally'}),
            Manager(status='ABA', manager_name='manager2', **{person_attribute_name:'jsmith'})
        ]
        
        pointy = employees[0]
        jsmith = employees[-1]
        dilbert = employees[1]
        
        session = create_session()
        c = Company(name='company1')
        c.employees = employees
        session.save(c)

        session.flush()
        session.clear()
        
        self.assertEquals(session.query(Person).get(dilbert.person_id), dilbert)
        session.clear()

        self.assertEquals(session.query(Person).filter(Person.person_id==dilbert.person_id).one(), dilbert)
        session.clear()

        def go():
            cc = session.query(Company).get(c.company_id)
            self.assertEquals(cc.employees, employees)
            
        if not lazy_relation:
            if with_polymorphic != 'none':
                self.assert_sql_count(testing.db, go, 1)
            else:
                self.assert_sql_count(testing.db, go, 5)

        else:
            if with_polymorphic != 'none':
                self.assert_sql_count(testing.db, go, 2)
            else:
                self.assert_sql_count(testing.db, go, 6)
        
        # test selecting from the query, using the base mapped table (people) as the selection criterion.
        # in the case of the polymorphic Person query, the "people" selectable should be adapted to be "person_join"
        self.assertEquals(
            session.query(Person).filter(getattr(Person, person_attribute_name)=='dilbert').first(),
            dilbert
        )
        self.assertEquals(
            session.query(Engineer).filter(getattr(Person, person_attribute_name)=='dilbert').first(),
            dilbert
        )
        
        # test selecting from the query, joining against an alias of the base "people" table.  test that
        # the "palias" alias does *not* get sucked up into the "person_join" conversion.
        palias = people.alias("palias")
        dilbert = session.query(Person).get(dilbert.person_id)
        assert dilbert is session.query(Person).filter((palias.c.name=='dilbert') & (palias.c.person_id==Person.person_id)).first()
        assert dilbert is session.query(Engineer).filter((palias.c.name=='dilbert') & (palias.c.person_id==Person.person_id)).first()
        assert dilbert is session.query(Person).filter((Engineer.engineer_name=="engineer1") & (engineers.c.person_id==people.c.person_id)).first()
        assert dilbert is session.query(Engineer).filter(Engineer.engineer_name=="engineer1")[0]
        
        dilbert.engineer_name = 'hes dibert!'

        session.flush()
        session.clear()
        
        def go():
            session.query(Person).filter(getattr(Person, person_attribute_name)=='dilbert').first()
        self.assert_sql_count(testing.db, go, 1)
        session.clear()
        dilbert = session.query(Person).filter(getattr(Person, person_attribute_name)=='dilbert').first()
        def go():
            # assert that only primary table is queried for already-present-in-session
            d = session.query(Person).filter(getattr(Person, person_attribute_name)=='dilbert').first()
        self.assert_sql_count(testing.db, go, 1)

        # test standalone orphans
        daboss = Boss(status='BBB', manager_name='boss', golf_swing='fore', **{person_attribute_name:'daboss'})
        session.save(daboss)
        self.assertRaises(orm_exc.FlushError, session.flush)
        c = session.query(Company).first()
        daboss.company = c
        manager_list = [e for e in c.employees if isinstance(e, Manager)]
        session.flush()
        session.clear()

        self.assertEquals(session.query(Manager).order_by(Manager.person_id).all(), manager_list)
        c = session.query(Company).first()
        
        session.delete(c)
        session.flush()
        
        self.assertEquals(people.count().scalar(), 0)
        
    test_roundtrip = _function_named(
        test_roundtrip, "test_%s%s%s_%s" % (
          (lazy_relation and "lazy" or "eager"),
          (include_base and "_inclbase" or ""),
          (redefine_colprop and "_redefcol" or ""),
          with_polymorphic))
    setattr(RoundTripTest, test_roundtrip.__name__, test_roundtrip)

for lazy_relation in [True, False]:
    for redefine_colprop in [True, False]:
        for with_polymorphic in ['unions', 'joins', 'auto', 'none']:
            if with_polymorphic == 'unions':
                for include_base in [True, False]:
                    generate_round_trip_test(include_base, lazy_relation, redefine_colprop, with_polymorphic)
            else:
                generate_round_trip_test(False, lazy_relation, redefine_colprop, with_polymorphic)

if __name__ == "__main__":
    testenv.main()
