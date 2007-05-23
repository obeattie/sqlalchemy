from sqlalchemy import *
import sets

import logging
logging.basicConfig()
logging.getLogger('sqlalchemy.orm').setLevel(logging.DEBUG)
logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)

# this example illustrates a polymorphic load of two classes, where each class has a  
# different set of properties

metadata = BoundMetaData('sqlite://')

# a table to store companies
companies = Table('companies', metadata, 
   Column('company_id', Integer, primary_key=True),
   Column('name', String(50)))

# we will define an inheritance relationship between the table "people" and "engineers",
# and a second inheritance relationship between the table "people" and "managers"
people = Table('people', metadata, 
   Column('person_id', Integer, primary_key=True),
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
   
metadata.create_all()

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


# create a union that represents both types of joins.  
person_join = polymorphic_union(
    {
        'engineer':people.join(engineers),
        'manager':people.join(managers),
        'person':people.select(people.c.type=='person'),
    }, None, 'pjoin')

#person_mapper = mapper(Person, people, select_table=person_join,polymorphic_on=person_join.c.type, polymorphic_identity='person')
person_mapper = mapper(Person, people, polymorphic_on=people.c.type, polymorphic_identity='person')
mapper(Engineer, engineers, inherits=person_mapper, polymorphic_identity='engineer')
mapper(Manager, managers, inherits=person_mapper, polymorphic_identity='manager')

mapper(Company, companies, properties={
    'employees': relation(Person, lazy=False, private=True, backref='company')
})

session = create_session(echo_uow=False)
c = Company(name='company1')
c.employees.append(Manager(name='pointy haired boss', status='AAB', manager_name='manager1'))
c.employees.append(Engineer(name='dilbert', status='BBA', engineer_name='engineer1', primary_language='java'))
c.employees.append(Person(name='joesmith', status='HHH'))
c.employees.append(Engineer(name='wally', status='CGG', engineer_name='engineer2', primary_language='python'))
c.employees.append(Manager(name='jsmith', status='ABA', manager_name='manager2'))
session.save(c)

print session.new
session.flush()
session.clear()

c = session.query(Company).get(1)
for e in c.employees:
    print e, e._instance_key, e.company
assert sets.Set([e.name for e in c.employees]) == sets.Set(['pointy haired boss', 'dilbert', 'joesmith', 'wally', 'jsmith'])
print "\n"

dilbert = session.query(Person).get_by(name='dilbert')
dilbert2 = session.query(Engineer).get_by(name='dilbert')
assert dilbert is dilbert2

dilbert.engineer_name = 'hes dibert!'

session.flush()
session.clear()

c = session.query(Company).get(1)
for e in c.employees:
    print e, e._instance_key

session.delete(c)
session.flush()

metadata.drop_all()
