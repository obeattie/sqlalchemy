from sqlalchemy import *
import sys

# this example illustrates a polymorphic load of two classes, where each class has a very 
# different set of properties

db = create_engine('sqlite://', echo=True, echo_uow=False)

# a table to store companies
companies = Table('companies', db, 
   Column('company_id', Integer, primary_key=True),
   Column('name', String(50))).create()

# we will define an inheritance relationship between the table "people" and "engineers",
# and a second inheritance relationship between the table "people" and "managers"
people = Table('people', db, 
   Column('person_id', Integer, primary_key=True),
   Column('company_id', Integer, ForeignKey('companies.company_id')),
   Column('name', String(50))).create()
   
engineers = Table('engineers', db, 
   Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
   Column('status', String(30)),
   Column('engineer_name', String(50)),
   Column('primary_language', String(50)),
  ).create()
   
managers = Table('managers', db, 
   Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
   Column('status', String(30)),
   Column('manager_name', String(50))
   ).create()

  
# create our classes.  The Engineer and Manager classes extend from Person.
class Person(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.iteritems():
            setattr(self, key, value)
    def __repr__(self):
        return "Ordinary person %s" % self.name
class Engineer(Person):
    def __repr__(self):
        return "Engineer %s, status %s, engineer_name %s, primary_language %s" % (self.name, self.status, self.engineer_name, self.primary_language)
class Manager(Person):
    def __repr__(self):
        return "Manager %s, status %s, manager_name %s" % (self.name, self.status, self.manager_name)
class Company(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.iteritems():
            setattr(self, key, value)
    def __repr__(self):
        return "Company %s" % self.name


# create a union that represents both types of joins.  we have to use
# nulls to pad out the disparate columns.
person_join = select(
                [
                    people, 
                    managers.c.status, 
                    managers.c.manager_name,
                    null().label('engineer_name'),
                    null().label('primary_language'),
                    column("'manager'").label('type')
                ], 
                people.c.person_id==managers.c.person_id).union_all(
            select(
                [
                    people, 
                    engineers.c.status, 
                    null().label('').label('manager_name'),
                    engineers.c.engineer_name,
                    engineers.c.primary_language, 
                    column("'engineer'").label('type')
                ],
            people.c.person_id==engineers.c.person_id)).alias('pjoin')


mapper(Person, people)
mapper(Engineer, engineers, inherits=Person)
mapper(Manager, managers, inherits=Person)
    
people_mapper = mapper(Person, person_join, polymorphic_on=person_join.c.type, polymorphic_map={'engineer':Engineer, 'manager':Manager}, non_primary=True)



mapper(Company, companies, properties={
    'employees': relation(people_mapper, lazy=False, private=True, backref='company')
})

session = create_session()
c = Company(name='company1')
c.employees.append(Manager(name='pointy haired boss', status='AAB', manager_name='manager1'))
c.employees.append(Engineer(name='dilbert', status='BBA', engineer_name='engineer1', primary_language='java'))
c.employees.append(Engineer(name='wally', status='CGG', engineer_name='engineer2', primary_language='python'))
c.employees.append(Manager(name='jsmith', status='ABA', manager_name='manager2'))
session.save(c)
session.flush()

session.clear()

c = session.query(Company).get(1)
for e in c.employees:
    print e, e._instance_key, e.company

print "\n"

dilbert = session.query(people_mapper).get_by(name='dilbert')
print "DILBERT1", dilbert
dilbert2 = session.query(Engineer).get_by(name='dilbert')
print "DILBERT2", dilbert2
assert dilbert is dilbert2

dilbert.engineer_name = 'hes dibert!'

session.flush()
session.clear()

c = session.query(Company).get(1)
for e in c.employees:
    print e, e._instance_key

session.delete(c)
session.flush()


managers.drop()
engineers.drop()
people.drop()
companies.drop()
