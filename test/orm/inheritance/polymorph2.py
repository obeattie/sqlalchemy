from sqlalchemy import *
import testbase

class AttrSettable(object):
    def __init__(self, **kwargs):
        [setattr(self, k, v) for k, v in kwargs.iteritems()]
    def __repr__(self):
        return self.__class__.__name__ + "(%s)" % (hex(id(self)))


class RelationTest1(testbase.ORMTest):
    """test self-referential relationships on polymorphic mappers"""
    def define_tables(self, metadata):
        global people, managers

        people = Table('people', metadata, 
           Column('person_id', Integer, Sequence('person_id_seq', optional=True), primary_key=True),
           Column('manager_id', Integer, ForeignKey('managers.person_id', use_alter=True, name="mpid_fq")),
           Column('name', String(50)),
           Column('type', String(30)))

        managers = Table('managers', metadata, 
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('status', String(30)),
           Column('manager_name', String(50))
           )

    def tearDown(self):
        people.update(values={people.c.manager_id:None}).execute()
        super(RelationTest1, self).tearDown()
        
    def testparentrefsdescendant(self):
        class Person(AttrSettable):
            pass
        class Manager(Person):
            pass

        mapper(Person, people, properties={
            'manager':relation(Manager, primaryjoin=people.c.manager_id==managers.c.person_id, uselist=False)
        })
        mapper(Manager, managers, inherits=Person, inherit_condition=people.c.person_id==managers.c.person_id)
        try:
            compile_mappers()
        except exceptions.ArgumentError, ar:
            assert str(ar) == "Can't determine relation direction for relationship 'Person.manager (Manager)' - foreign key columns are present in both the parent and the child's mapped tables.  Specify 'foreign_keys' argument.", str(ar)

        clear_mappers()

        mapper(Person, people, properties={
            'manager':relation(Manager, primaryjoin=people.c.manager_id==managers.c.person_id, foreignkey=people.c.manager_id, uselist=False, post_update=True)
        })
        mapper(Manager, managers, inherits=Person, inherit_condition=people.c.person_id==managers.c.person_id)

        session = create_session()
        p = Person(name='some person')
        m = Manager(name='some manager')
        p.manager = m
        session.save(p)
        session.flush()
        session.clear()

        p = session.query(Person).get(p.person_id)
        m = session.query(Manager).get(m.person_id)
        print p, m, p.manager
        assert p.manager is m

    def testdescendantrefsparent(self):
        class Person(AttrSettable):
            pass
        class Manager(Person):
            pass

        mapper(Person, people)
        mapper(Manager, managers, inherits=Person, inherit_condition=people.c.person_id==managers.c.person_id, properties={
            'employee':relation(Person, primaryjoin=people.c.manager_id==managers.c.person_id, foreignkey=people.c.manager_id, uselist=False, post_update=True)
        })

        session = create_session()
        p = Person(name='some person')
        m = Manager(name='some manager')
        m.employee = p
        session.save(m)
        session.flush()
        session.clear()

        p = session.query(Person).get(p.person_id)
        m = session.query(Manager).get(m.person_id)
        print p, m, m.employee
        assert m.employee is p
            
class RelationTest2(testbase.ORMTest):
    """test self-referential relationships on polymorphic mappers"""
    def define_tables(self, metadata):
        global people, managers, data
        people = Table('people', metadata, 
           Column('person_id', Integer, Sequence('person_id_seq', optional=True), primary_key=True),
           Column('name', String(50)),
           Column('type', String(30)))

        managers = Table('managers', metadata, 
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('manager_id', Integer, ForeignKey('people.person_id')),
           Column('status', String(30)),
           )
        
        data = Table('data', metadata,
            Column('person_id', Integer, ForeignKey('managers.person_id'), primary_key=True),
            Column('data', String(30))
            )
            
    def testrelationonsubclass_j1_nodata(self):
        self.do_test("join1", False)
    def testrelationonsubclass_j2_nodata(self):
        self.do_test("join2", False)
    def testrelationonsubclass_j1_data(self):
        self.do_test("join1", True)
    def testrelationonsubclass_j2_data(self):
        self.do_test("join2", True)
                
    def do_test(self, jointype="join1", usedata=False):
        class Person(AttrSettable):
            pass
        class Manager(Person):
            pass

        if jointype == "join1":
            poly_union = polymorphic_union({
                'person':people.select(people.c.type=='person'),
                'manager':join(people, managers, people.c.person_id==managers.c.person_id)
            }, None)
        elif jointype == "join2":
            poly_union = polymorphic_union({
                'person':people.select(people.c.type=='person'),
                'manager':managers.join(people, people.c.person_id==managers.c.person_id)
            }, None)

        if usedata:
            class Data(object):
                def __init__(self, data):
                    self.data = data
            mapper(Data, data)
            
        mapper(Person, people, select_table=poly_union, polymorphic_identity='person', polymorphic_on=poly_union.c.type)

        if usedata:
            mapper(Manager, managers, inherits=Person, inherit_condition=people.c.person_id==managers.c.person_id, polymorphic_identity='manager',
                  properties={
                    'colleague':relation(Person, primaryjoin=managers.c.manager_id==people.c.person_id, lazy=True, uselist=False),
                    'data':relation(Data, uselist=False)
                 }
            )
        else:
            mapper(Manager, managers, inherits=Person, inherit_condition=people.c.person_id==managers.c.person_id, polymorphic_identity='manager',
                  properties={
                    'colleague':relation(Person, primaryjoin=managers.c.manager_id==people.c.person_id, lazy=True, uselist=False)
                 }
            )

        sess = create_session()
        p = Person(name='person1')
        m = Manager(name='manager1')
        m.colleague = p
        if usedata:
            m.data = Data('ms data')
        sess.save(m)
        sess.flush()
        
        sess.clear()
        p = sess.query(Person).get(p.person_id)
        m = sess.query(Manager).get(m.person_id)
        print p
        print m
        assert m.colleague is p
        if usedata:
            assert m.data.data == 'ms data'

class RelationTest3(testbase.ORMTest):
    """test self-referential relationships on polymorphic mappers"""
    def define_tables(self, metadata):
        global people, managers, data
        people = Table('people', metadata, 
           Column('person_id', Integer, Sequence('person_id_seq', optional=True), primary_key=True),
           Column('colleague_id', Integer, ForeignKey('people.person_id')),
           Column('name', String(50)),
           Column('type', String(30)))

        managers = Table('managers', metadata, 
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('status', String(30)),
           )

        data = Table('data', metadata,
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('data', String(30))
           )

    def testrelationonbaseclass_j1_nodata(self):
       self.do_test("join1", False)
    def testrelationonbaseclass_j2_nodata(self):
       self.do_test("join2", False)
    def testrelationonbaseclass_j1_data(self):
       self.do_test("join1", True)
    def testrelationonbaseclass_j2_data(self):
       self.do_test("join2", True)

    def do_test(self, jointype="join1", usedata=False):
        class Person(AttrSettable):
            pass
        class Manager(Person):
            pass

        if usedata:
            class Data(object):
                def __init__(self, data):
                    self.data = data

        if jointype == "join1":
            poly_union = polymorphic_union({
                'manager':managers.join(people, people.c.person_id==managers.c.person_id),
                'person':people.select(people.c.type=='person')
            }, None)
        elif jointype =="join2":
            poly_union = polymorphic_union({
                'manager':join(people, managers, people.c.person_id==managers.c.person_id),
                'person':people.select(people.c.type=='person')
            }, None)
            
        if usedata:
            mapper(Data, data)
        
        mapper(Manager, managers, inherits=Person, inherit_condition=people.c.person_id==managers.c.person_id, polymorphic_identity='manager')
        if usedata:
            mapper(Person, people, select_table=poly_union, polymorphic_identity='person', polymorphic_on=people.c.type,
                  properties={
                    'colleagues':relation(Person, primaryjoin=people.c.colleague_id==people.c.person_id, remote_side=people.c.colleague_id, uselist=True),
                    'data':relation(Data, uselist=False)
                    }        
            )
        else:
            mapper(Person, people, select_table=poly_union, polymorphic_identity='person', polymorphic_on=people.c.type,
                  properties={
                    'colleagues':relation(Person, primaryjoin=people.c.colleague_id==people.c.person_id, 
                        remote_side=people.c.colleague_id, uselist=True)
                    }        
            )

        sess = create_session()
        p = Person(name='person1')
        p2 = Person(name='person2')
        p3 = Person(name='person3')
        m = Manager(name='manager1')
        p.colleagues.append(p2)
        m.colleagues.append(p3)
        if usedata:
            p.data = Data('ps data')
            m.data = Data('ms data')

        sess.save(m)
        sess.save(p)
        sess.flush()
        
        sess.clear()
        p = sess.query(Person).get(p.person_id)
        p2 = sess.query(Person).get(p2.person_id)
        p3 = sess.query(Person).get(p3.person_id)
        m = sess.query(Person).get(m.person_id)
        print p, p2, p.colleagues, m.colleagues
        assert len(p.colleagues) == 1
        assert p.colleagues == [p2]
        assert m.colleagues == [p3]
        if usedata:
            assert p.data.data == 'ps data'
            assert m.data.data == 'ms data'

        
class RelationTest4(testbase.ORMTest):
    def define_tables(self, metadata):
        global people, engineers, managers, cars
        people = Table('people', metadata, 
           Column('person_id', Integer, primary_key=True),
           Column('name', String(50)))

        engineers = Table('engineers', metadata, 
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('status', String(30)))

        managers = Table('managers', metadata, 
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('longer_status', String(70)))

        cars = Table('cars', metadata, 
           Column('car_id', Integer, primary_key=True),
           Column('owner', Integer, ForeignKey('people.person_id')))
    
    def testmanytoonepolymorphic(self):
        """in this test, the polymorphic union is between two subclasses, but does not include the base table by itself
         in the union.  however, the primaryjoin condition is going to be against the base table, and its a many-to-one
         relationship (unlike the test in polymorph.py) so the column in the base table is explicit.  Can the ClauseAdapter
         figure out how to alias the primaryjoin to the polymorphic union ?"""
        # class definitions
        class Person(object):
            def __init__(self, **kwargs):
                for key, value in kwargs.iteritems():
                    setattr(self, key, value)
            def __repr__(self):
                return "Ordinary person %s" % self.name
        class Engineer(Person):
            def __repr__(self):
                return "Engineer %s, status %s" % (self.name, self.status)
        class Manager(Person):
            def __repr__(self):
                return "Manager %s, status %s" % (self.name, self.longer_status)
        class Car(object):
            def __init__(self, **kwargs):
                for key, value in kwargs.iteritems():
                    setattr(self, key, value)
            def __repr__(self):
                return "Car number %d" % self.car_id

        # create a union that represents both types of joins.  
        employee_join = polymorphic_union(
            {
                'engineer':people.join(engineers),
                'manager':people.join(managers),
            }, "type", 'employee_join')
            
        person_mapper   = mapper(Person, people, select_table=employee_join,polymorphic_on=employee_join.c.type, polymorphic_identity='person')
        engineer_mapper = mapper(Engineer, engineers, inherits=person_mapper, polymorphic_identity='engineer')
        manager_mapper  = mapper(Manager, managers, inherits=person_mapper, polymorphic_identity='manager')
        car_mapper      = mapper(Car, cars, properties= {'employee':relation(person_mapper)})
        
        print class_mapper(Person).primary_key
        print person_mapper.get_select_mapper().primary_key
        
        # so the primaryjoin is "people.c.person_id==cars.c.owner".  the "lazy" clause will be
        # "people.c.person_id=?".  the employee_join is two selects union'ed together, one of which 
        # will contain employee.c.person_id the other contains manager.c.person_id.  people.c.person_id is not explicitly in 
        # either column clause in this case.  we can modify polymorphic_union to always put the "base" column in which would fix this,
        # but im not sure if that really fixes the issue in all cases and its too far from the problem.
        # instead, when the primaryjoin is adapted to point to the polymorphic union and is targeting employee_join.c.person_id, 
        # it has to use not just straight column correspondence but also "keys_ok=True", meaning it will link up to any column 
        # with the name "person_id", as opposed to columns that descend directly from people.c.person_id.  polymorphic unions
        # require the cols all match up on column name which then determine the top selectable names, so matching by name is OK.

        session = create_session()

        # creating 5 managers named from M1 to E5
        for i in range(1,5):
            session.save(Manager(name="M%d" % i,longer_status="YYYYYYYYY"))
        # creating 5 engineers named from E1 to E5
        for i in range(1,5):
            session.save(Engineer(name="E%d" % i,status="X"))

        session.flush()

        engineer4 = session.query(Engineer).selectfirst_by(name="E4")
        manager3 = session.query(Manager).selectfirst_by(name="M3")
        
        car1 = Car(employee=engineer4)
        session.save(car1)
        car2 = Car(employee=manager3)
        session.save(car2)
        session.flush()

        session.clear()
        
        print "----------------------------"
        car1 = session.query(Car).get(car1.car_id)
        print "----------------------------"
        usingGet = session.query(person_mapper).get(car1.owner)
        print "----------------------------"
        usingProperty = car1.employee
        print "----------------------------"

        # All print should output the same person (engineer E4)
        assert str(engineer4) == "Engineer E4, status X"
        print str(usingGet)
        assert str(usingGet) == "Engineer E4, status X"
        assert str(usingProperty) == "Engineer E4, status X"

        session.clear()
        
        # and now for the lightning round, eager !
        car1 = session.query(Car).options(eagerload('employee')).get(car1.car_id)
        assert str(car1.employee) == "Engineer E4, status X"

        session.clear()
        s = session.query(Car)
        c = s.join("employee").select(employee_join.c.name=="E4")[0]
        assert c.car_id==car1.car_id

class RelationTest5(testbase.ORMTest):
    def define_tables(self, metadata):
        global people, engineers, managers, cars
        people = Table('people', metadata, 
           Column('person_id', Integer, primary_key=True),
           Column('name', String(50)),
           Column('type', String(50)))

        engineers = Table('engineers', metadata, 
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('status', String(30)))

        managers = Table('managers', metadata, 
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('longer_status', String(70)))

        cars = Table('cars', metadata, 
           Column('car_id', Integer, primary_key=True),
           Column('owner', Integer, ForeignKey('people.person_id')))
    
    def testeagerempty(self):
        """an easy one...test parent object with child relation to an inheriting mapper, using eager loads,
        works when there are no child objects present"""
        class Person(object):
            def __init__(self, **kwargs):
                for key, value in kwargs.iteritems():
                    setattr(self, key, value)
            def __repr__(self):
                return "Ordinary person %s" % self.name
        class Engineer(Person):
            def __repr__(self):
                return "Engineer %s, status %s" % (self.name, self.status)
        class Manager(Person):
            def __repr__(self):
                return "Manager %s, status %s" % (self.name, self.longer_status)
        class Car(object):
            def __init__(self, **kwargs):
                for key, value in kwargs.iteritems():
                    setattr(self, key, value)
            def __repr__(self):
                return "Car number %d" % self.car_id

        person_mapper   = mapper(Person, people, polymorphic_on=people.c.type, polymorphic_identity='person')
        engineer_mapper = mapper(Engineer, engineers, inherits=person_mapper, polymorphic_identity='engineer')
        manager_mapper  = mapper(Manager, managers, inherits=person_mapper, polymorphic_identity='manager')
        car_mapper      = mapper(Car, cars, properties= {'manager':relation(manager_mapper, lazy=False)})

        sess = create_session()
        car1 = Car()
        car2 = Car()
        car2.manager = Manager()
        sess.save(car1)
        sess.save(car2)
        sess.flush()
        sess.clear()
        
        carlist = sess.query(Car).select()
        assert carlist[0].manager is None
        assert carlist[1].manager.person_id == car2.manager.person_id

class RelationTest6(testbase.ORMTest):
    """test self-referential relationships on a single joined-table inheritance mapper"""
    def define_tables(self, metadata):
        global people, managers, data
        people = Table('people', metadata, 
           Column('person_id', Integer, Sequence('person_id_seq', optional=True), primary_key=True),
           Column('name', String(50)),
           )

        managers = Table('managers', metadata, 
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('colleague_id', Integer, ForeignKey('managers.person_id')),
           Column('status', String(30)),
           )

    def testbasic(self):
        class Person(AttrSettable):
            pass
        class Manager(Person):
            pass

        mapper(Person, people)
        # relationship is from people.join(managers) -> people.join(managers).  self referential logic
        # needs to be used to figure out the lazy clause, meaning create_lazy_clause must go from parent.mapped_table
        # to parent.mapped_table
        mapper(Manager, managers, inherits=Person, inherit_condition=people.c.person_id==managers.c.person_id,
              properties={
                'colleague':relation(Manager, primaryjoin=managers.c.colleague_id==managers.c.person_id, lazy=True, uselist=False)
             }
        )

        sess = create_session()
        m = Manager(name='manager1')
        m2 =Manager(name='manager2')
        m.colleague = m2
        sess.save(m)
        sess.flush()

        sess.clear()
        m = sess.query(Manager).get(m.person_id)
        m2 = sess.query(Manager).get(m2.person_id)
        assert m.colleague is m2

class RelationTest7(testbase.ORMTest):
    def define_tables(self, metadata):
        global people, engineers, managers, cars, offroad_cars
        cars = Table('cars', metadata,
                Column('car_id', Integer, primary_key=True),
                Column('name', String(30)))

        offroad_cars = Table('offroad_cars', metadata,
                Column('car_id',Integer, ForeignKey('cars.car_id'),nullable=False,primary_key=True))

        people = Table('people', metadata,
                Column('person_id', Integer, primary_key=True),
                Column('car_id', Integer, ForeignKey('cars.car_id'), nullable=False),
                Column('name', String(50)))

        engineers = Table('engineers', metadata,
                Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
                Column('field', String(30)))


        managers = Table('managers', metadata,
                Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
                Column('category', String(70)))

    def test_manytoone_lazyload(self):
        """test that lazy load clause to a polymorphic child mapper generates correctly [ticket:493]"""
        class PersistentObject(object):
            def __init__(self, **kwargs):
                for key, value in kwargs.iteritems():
                    setattr(self, key, value)

        class Status(PersistentObject):
            def __repr__(self):
                return "Status %s" % self.name

        class Person(PersistentObject):
            def __repr__(self):
                return "Ordinary person %s" % self.name

        class Engineer(Person):
            def __repr__(self):
                return "Engineer %s, field %s" % (self.name, self.field)

        class Manager(Person):
            def __repr__(self):
                return "Manager %s, category %s" % (self.name, self.category)

        class Car(PersistentObject):
            def __repr__(self):
                return "Car number %d, name %s" % i(self.car_id, self.name)

        class Offraod_Car(Car):
            def __repr__(self):
                return "Offroad Car number %d, name %s" % (self.car_id,self.name)

        employee_join = polymorphic_union(
                {
                    'engineer':people.join(engineers),
                    'manager':people.join(managers), 
                }, "type", 'employee_join')

        car_join = polymorphic_union(
            {
                'car' : cars.outerjoin(offroad_cars).select(offroad_cars.c.car_id == None, fold_equivalents=True),
                'offroad' : cars.join(offroad_cars)
            }, "type", 'car_join')

        car_mapper  = mapper(Car, cars,
                select_table=car_join,polymorphic_on=car_join.c.type,
                polymorphic_identity='car',
                )
        offroad_car_mapper = mapper(Offraod_Car, offroad_cars, inherits=car_mapper, polymorphic_identity='offroad')
        person_mapper = mapper(Person, people,
                select_table=employee_join,polymorphic_on=employee_join.c.type,
                polymorphic_identity='person', 
                properties={
                    'car':relation(car_mapper)
                    })
        engineer_mapper = mapper(Engineer, engineers, inherits=person_mapper, polymorphic_identity='engineer')
        manager_mapper  = mapper(Manager, managers, inherits=person_mapper, polymorphic_identity='manager')

        session = create_session()
        basic_car=Car(name="basic")
        offroad_car=Offraod_Car(name="offroad")

        for i in range(1,4):
            if i%2:
                car=Car()
            else:
                car=Offraod_Car()
            session.save(Manager(name="M%d" % i,category="YYYYYYYYY",car=car))
            session.save(Engineer(name="E%d" % i,field="X",car=car))
            session.flush()
            session.clear()

        r = session.query(Person).select()
        for p in r:
            assert p.car_id == p.car.car_id
    
class GenerativeTest(testbase.AssertMixin):
    def setUpAll(self):
        #  cars---owned by---  people (abstract) --- has a --- status
        #   |                  ^    ^                            |
        #   |                  |    |                            |
        #   |          engineers    managers                     |
        #   |                                                    |
        #   +--------------------------------------- has a ------+

        global metadata, status, people, engineers, managers, cars
        metadata = BoundMetaData(testbase.db)
        # table definitions
        status = Table('status', metadata, 
           Column('status_id', Integer, primary_key=True),
           Column('name', String(20)))

        people = Table('people', metadata, 
           Column('person_id', Integer, primary_key=True),
           Column('status_id', Integer, ForeignKey('status.status_id'), nullable=False),
           Column('name', String(50)))

        engineers = Table('engineers', metadata, 
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('field', String(30)))

        managers = Table('managers', metadata, 
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('category', String(70)))

        cars = Table('cars', metadata, 
           Column('car_id', Integer, primary_key=True),
           Column('status_id', Integer, ForeignKey('status.status_id'), nullable=False),
           Column('owner', Integer, ForeignKey('people.person_id'), nullable=False))

        metadata.create_all()

    def tearDownAll(self):
        metadata.drop_all()
    def tearDown(self):
        clear_mappers()
        for t in metadata.table_iterator(reverse=True):
            t.delete().execute()
    
    def testjointo(self):
        # class definitions
        class PersistentObject(object):
            def __init__(self, **kwargs):
                for key, value in kwargs.iteritems():
                    setattr(self, key, value)
        class Status(PersistentObject):
            def __repr__(self):
                return "Status %s" % self.name
        class Person(PersistentObject):
            def __repr__(self):
                return "Ordinary person %s" % self.name
        class Engineer(Person):
            def __repr__(self):
                return "Engineer %s, field %s, status %s" % (self.name, self.field, self.status)
        class Manager(Person):
            def __repr__(self):
                return "Manager %s, category %s, status %s" % (self.name, self.category, self.status)
        class Car(PersistentObject):
            def __repr__(self):
                return "Car number %d" % self.car_id

        # create a union that represents both types of joins.  
        employee_join = polymorphic_union(
            {
                'engineer':people.join(engineers),
                'manager':people.join(managers),
            }, "type", 'employee_join')

        status_mapper   = mapper(Status, status)
        person_mapper   = mapper(Person, people, 
            select_table=employee_join,polymorphic_on=employee_join.c.type, 
            polymorphic_identity='person', properties={'status':relation(status_mapper)})
        engineer_mapper = mapper(Engineer, engineers, inherits=person_mapper, polymorphic_identity='engineer')
        manager_mapper  = mapper(Manager, managers, inherits=person_mapper, polymorphic_identity='manager')
        car_mapper      = mapper(Car, cars, properties= {'employee':relation(person_mapper), 'status':relation(status_mapper)})

        session = create_session(echo_uow=False)

        active = Status(name="active")
        dead = Status(name="dead")

        session.save(active)
        session.save(dead)
        session.flush()

        # TODO: we haven't created assertions for all the data combinations created here
        
        # creating 5 managers named from M1 to M5 and 5 engineers named from E1 to E5
        # M4, M5, E4 and E5 are dead
        for i in range(1,5):
            if i<4:
                st=active
            else:
                st=dead
            session.save(Manager(name="M%d" % i,category="YYYYYYYYY",status=st))
            session.save(Engineer(name="E%d" % i,field="X",status=st))

        session.flush()

        # get E4
        engineer4 = session.query(engineer_mapper).get_by(name="E4")

        # create 2 cars for E4, one active and one dead
        car1 = Car(employee=engineer4,status=active)
        car2 = Car(employee=engineer4,status=dead)
        session.save(car1)
        session.save(car2)
        session.flush()

        # test these twice because theres caching involved, as well previous issues that modified the polymorphic union
        for x in range(0, 2):
            r = session.query(Person).filter_by(people.c.name.like('%2')).join('status').filter_by(name="active")
            assert str(list(r)) == "[Manager M2, category YYYYYYYYY, status Status active, Engineer E2, field X, status Status active]"
            r = session.query(Engineer).join('status').filter(people.c.name.in_('E2', 'E3', 'E4', 'M4', 'M2', 'M1') & (status.c.name=="active"))
            assert str(list(r)) == "[Engineer E2, field X, status Status active, Engineer E3, field X, status Status active]"
            # this test embeds the original polymorphic union (employee_join) fully 
            # into the WHERE criterion, using a correlated select. ticket #577 tracks 
            # that Query's adaptation of the WHERE clause does not dig into the 
            # mapped selectable itself, which permanently breaks the mapped selectable.
            r = session.query(Person).filter(Car.c.owner == select([Car.c.owner], Car.c.owner==employee_join.c.person_id))
            assert str(list(r)) == "[Engineer E4, field X, status Status dead]"
        
class MultiLevelTest(testbase.ORMTest):
    def define_tables(self, metadata):
        global table_Employee, table_Engineer, table_Manager
        table_Employee = Table( 'Employee', metadata,
            Column( 'name', type= String(100), ),
            Column( 'id', primary_key= True, type= Integer, ),
            Column( 'atype', type= String(100), ),
        )

        table_Engineer = Table( 'Engineer', metadata,
            Column( 'machine', type= String(100), ),
            Column( 'id', Integer, ForeignKey( 'Employee.id', ), primary_key= True, ),
        )

        table_Manager = Table( 'Manager', metadata,
            Column( 'duties', type= String(100), ),
            Column( 'id', Integer, ForeignKey( 'Engineer.id', ), primary_key= True, ),
        )
    def test_threelevels(self):
        class Employee( object):
            def set( me, **kargs):
                for k,v in kargs.iteritems(): setattr( me, k, v)
                return me
            def __str__(me): return str(me.__class__.__name__)+':'+str(me.name)
            __repr__ = __str__
        class Engineer( Employee): pass
        class Manager( Engineer): pass

        pu_Employee = polymorphic_union( {
                    'Manager':  table_Employee.join( table_Engineer).join( table_Manager),
                    'Engineer': select([table_Employee, table_Engineer.c.machine], table_Employee.c.atype == 'Engineer', from_obj=[table_Employee.join(table_Engineer)]),
                    'Employee': table_Employee.select( table_Employee.c.atype == 'Employee'),
                }, None, 'pu_employee', )

#        pu_Employee = polymorphic_union( {
#                    'Manager':  table_Employee.join( table_Engineer).join( table_Manager),
#                    'Engineer': table_Employee.join(table_Engineer).select(table_Employee.c.atype == 'Engineer'),
#                    'Employee': table_Employee.select( table_Employee.c.atype == 'Employee'),
#                }, None, 'pu_employee', )
        
        mapper_Employee = mapper( Employee, table_Employee,
                    polymorphic_identity= 'Employee',
                    polymorphic_on= pu_Employee.c.atype,
                    select_table= pu_Employee,
                )

        pu_Engineer = polymorphic_union( {
                    'Manager':  table_Employee.join( table_Engineer).join( table_Manager),
                    'Engineer': select([table_Employee, table_Engineer.c.machine], table_Employee.c.atype == 'Engineer', from_obj=[table_Employee.join(table_Engineer)]),
                }, None, 'pu_engineer', )
        mapper_Engineer = mapper( Engineer, table_Engineer,
                    inherit_condition= table_Engineer.c.id == table_Employee.c.id,
                    inherits= mapper_Employee,
                    polymorphic_identity= 'Engineer',
                    polymorphic_on= pu_Engineer.c.atype,
                    select_table= pu_Engineer,
                )

        mapper_Manager = mapper( Manager, table_Manager,
                    inherit_condition= table_Manager.c.id == table_Engineer.c.id,
                    inherits= mapper_Engineer,
                    polymorphic_identity= 'Manager',
                )

        a = Employee().set( name= 'one')
        b = Engineer().set( egn= 'two', machine= 'any')
        c = Manager().set( name= 'head', machine= 'fast', duties= 'many')

        session = create_session()
        session.save(a)
        session.save(b)
        session.save(c)
        session.flush()
        assert set(session.query(Employee).select()) == set([a,b,c])
        assert set(session.query( Engineer).select()) == set([b,c])
        assert session.query( Manager).select() == [c]

class ManyToManyPolyTest(testbase.ORMTest):
    def define_tables(self, metadata):
        global base_item_table, item_table, base_item_collection_table, collection_table
        base_item_table = Table(
            'base_item', metadata,
            Column('id', Integer, primary_key=True),
            Column('child_name', String(255), default=None))

        item_table = Table(
            'item', metadata,
            Column('id', Integer, ForeignKey('base_item.id'), primary_key=True),
            Column('dummy', Integer, default=0)) # Dummy column to avoid weird insert problems

        base_item_collection_table = Table(
            'base_item_collection', metadata,
            Column('item_id', Integer, ForeignKey('base_item.id')),
            Column('collection_id', Integer, ForeignKey('collection.id')))

        collection_table = Table(
            'collection', metadata,
            Column('id', Integer, primary_key=True),
            Column('name', Unicode(255)))
            
    def test_pjoin_compile(self):
        """test that remote_side columns in the secondary join table arent attempted to be 
        matched to the target polymorphic selectable"""
        class BaseItem(object): pass
        class Item(BaseItem): pass
        class Collection(object): pass
        item_join = polymorphic_union( {
            'BaseItem':base_item_table.select(base_item_table.c.child_name=='BaseItem'),
            'Item':base_item_table.join(item_table),
            }, None, 'item_join')

        mapper(
            BaseItem, base_item_table,
            select_table=item_join,
            polymorphic_on=base_item_table.c.child_name,
            polymorphic_identity='BaseItem',
            properties=dict(collections=relation(Collection, secondary=base_item_collection_table, backref="items")))

        mapper(
            Item, item_table,
            inherits=BaseItem,
            polymorphic_identity='Item')

        mapper(Collection, collection_table)
        
        class_mapper(BaseItem)

class CustomPKTest(testbase.ORMTest):
    def define_tables(self, metadata):
        global t1, t2
        t1 = Table('t1', metadata, 
            Column('id', Integer, primary_key=True),
            Column('type', String(30), nullable=False),
            Column('data', String(30)))
        t2 = Table('t2', metadata,
            Column('t2id', Integer, ForeignKey('t1.id'), primary_key=True),
            Column('t2data', String(30)))
    def test_custompk(self):
        """test that the primary_key attribute is propigated to the polymorphic mapper"""
        
        class T1(object):pass
        class T2(T1):pass
        
        # create a polymorphic union with the select against the base table first.
        # with the join being second, the alias of the union will 
        # pick up two "primary key" columns.  technically the alias should have a
        # 2-col pk in any case but the leading select has a NULL for the "t2id" column
        d = util.OrderedDict()
        d['t1'] = t1.select(t1.c.type=='t1')
        d['t2'] = t1.join(t2)
        pjoin = polymorphic_union(d, None, 'pjoin')
        
        #print pjoin.original.primary_key
        #print pjoin.primary_key
        assert len(pjoin.primary_key) == 2
        
        mapper(T1, t1, polymorphic_on=t1.c.type, polymorphic_identity='t1', select_table=pjoin, primary_key=[pjoin.c.id])
        mapper(T2, t2, inherits=T1, polymorphic_identity='t2')
        print [str(c) for c in class_mapper(T1).primary_key]
        ot1 = T1()
        ot2 = T2()
        sess = create_session()
        sess.save(ot1)
        sess.save(ot2)
        sess.flush()
        sess.clear()
        
        # query using get(), using only one value.  this requires the select_table mapper
        # has the same single-col primary key.
        assert sess.query(T1).get(ot1.id).id is ot1.id
        
        ot1 = sess.query(T1).get(ot1.id)
        ot1.data = 'hi'
        sess.flush()
        
if __name__ == "__main__":    
    testbase.main()
        
