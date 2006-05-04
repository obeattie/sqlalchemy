from sqlalchemy import *

metadata = MetaData()

employees_table = Table('employees', metadata, 
    Column('employee_id', Integer, primary_key=True),
    Column('name', String(50)),
    Column('type', String(20))
)

engine = create_engine('sqlite:///')
metadata.create_all(engine)

class Employee(object):
    pass
    
class Manager(Employee):
    pass
    
class Engineer(Employee):
    pass

employee_mapper = mapper(Employee, employees_table, polymorphic_on=employees_table.c.type)
manager_mapper = mapper(Manager, inherits=employee_mapper, polymorphic_identity='manager')
engineer_mapper = mapper(Engineer, inherits=employee_mapper, polymorphic_identity='engineer')
