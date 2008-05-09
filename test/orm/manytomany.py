import testenv; testenv.configure_for_tests()
from testlib import sa, testing
from testlib.sa import Table, Column, Integer, String, ForeignKey
from testlib.sa.orm import mapper, relation, create_session
from orm import _base


class M2MTest(_base.MappedTest):
    def define_tables(self, metadata):
        Table('place', metadata,
            Column('place_id', Integer, sa.Sequence('pid_seq', optional=True),
                   primary_key=True),
            Column('name', String(30), nullable=False))

        Table('transition', metadata,
            Column('transition_id', Integer,
                   sa.Sequence('tid_seq', optional=True), primary_key=True),
            Column('name', String(30), nullable=False))

        Table('place_thingy', metadata,
            Column('thingy_id', Integer, sa.Sequence('thid_seq', optional=True),
                   primary_key=True),
            Column('place_id', Integer, ForeignKey('place.place_id'),
                   nullable=False),
            Column('name', String(30), nullable=False))

        # association table #1
        Table('place_input', metadata,
            Column('place_id', Integer, ForeignKey('place.place_id')),
            Column('transition_id', Integer,
                   ForeignKey('transition.transition_id')))

        # association table #2
        Table('place_output', metadata,
            Column('place_id', Integer, ForeignKey('place.place_id')),
            Column('transition_id', Integer,
                   ForeignKey('transition.transition_id')))

        Table('place_place', metadata,
              Column('pl1_id', Integer, ForeignKey('place.place_id')),
              Column('pl2_id', Integer, ForeignKey('place.place_id')))

    def setup_classes(self):
        class Place(_base.BasicEntity):
            def __init__(self, name=None):
                self.name = name
            def __str__(self):
                return "(Place '%s')" % self.name
            __repr__ = __str__

        class PlaceThingy(_base.BasicEntity):
            def __init__(self, name=None):
                self.name = name

        class Transition(_base.BasicEntity):
            def __init__(self, name=None):
                self.name = name
                self.inputs = []
                self.outputs = []
            def __repr__(self):
                return ' '.join((object.__repr__(self),
                                 repr(self.inputs),
                                 repr(self.outputs)))

    @testing.resolve_artifact_names
    def testerror(self):
        mapper(Place, place, properties={
            'transitions':relation(Transition, secondary=place_input, backref='places')
        })
        mapper(Transition, transition, properties={
            'places':relation(Place, secondary=place_input, backref='transitions')
        })
        self.assertRaisesMessage(sa.exc.ArgumentError, "Error creating backref",
                                 sa.orm.compile_mappers)

    @testing.resolve_artifact_names
    def testcircular(self):
        """tests a many-to-many relationship from a table to itself."""

        Place.mapper = mapper(Place, place)

        Place.mapper.add_property('places', relation(
            Place.mapper, secondary=place_place, primaryjoin=place.c.place_id==place_place.c.pl1_id,
            secondaryjoin=place.c.place_id==place_place.c.pl2_id,
            order_by=place_place.c.pl2_id,
            lazy=True,
            ))

        sess = create_session()
        p1 = Place('place1')
        p2 = Place('place2')
        p3 = Place('place3')
        p4 = Place('place4')
        p5 = Place('place5')
        p6 = Place('place6')
        p7 = Place('place7')
        [sess.save(x) for x in [p1,p2,p3,p4,p5,p6,p7]]
        p1.places.append(p2)
        p1.places.append(p3)
        p5.places.append(p6)
        p6.places.append(p1)
        p7.places.append(p1)
        p1.places.append(p5)
        p4.places.append(p3)
        p3.places.append(p4)
        sess.flush()

        sess.clear()
        l = sess.query(Place).order_by(place.c.place_id).all()
        (p1, p2, p3, p4, p5, p6, p7) = l
        assert p1.places == [p2,p3,p5]
        assert p5.places == [p6]
        assert p7.places == [p1]
        assert p6.places == [p1]
        assert p4.places == [p3]
        assert p3.places == [p4]
        assert p2.places == []

        for p in l:
            pp = p.places
            print "Place " + str(p) +" places " + repr(pp)

        [sess.delete(p) for p in p1,p2,p3,p4,p5,p6,p7]
        sess.flush()

    @testing.resolve_artifact_names
    def testdouble(self):
        """tests that a mapper can have two eager relations to the same table, via
        two different association tables.  aliases are required."""

        Place.mapper = mapper(Place, place, properties = {
            'thingies':relation(mapper(PlaceThingy, place_thingy), lazy=False)
        })

        Transition.mapper = mapper(Transition, transition, properties = dict(
            inputs = relation(Place.mapper, place_output, lazy=False),
            outputs = relation(Place.mapper, place_input, lazy=False),
            )
        )

        tran = Transition('transition1')
        tran.inputs.append(Place('place1'))
        tran.outputs.append(Place('place2'))
        tran.outputs.append(Place('place3'))
        sess = create_session()
        sess.save(tran)
        sess.flush()

        sess.clear()
        r = sess.query(Transition).all()
        self.assert_unordered_result(r, Transition,
            {'name': 'transition1',
            'inputs': (Place, [{'name':'place1'}]),
            'outputs': (Place, [{'name':'place2'}, {'name':'place3'}])
            })

    @testing.resolve_artifact_names
    def testbidirectional(self):
        """tests a many-to-many backrefs"""
        Place.mapper = mapper(Place, place)
        Transition.mapper = mapper(Transition, transition, properties = dict(
            inputs = relation(Place.mapper, place_output, lazy=True, backref='inputs'),
            outputs = relation(Place.mapper, place_input, lazy=True, backref='outputs'),
            )
        )

        t1 = Transition('transition1')
        t2 = Transition('transition2')
        t3 = Transition('transition3')
        p1 = Place('place1')
        p2 = Place('place2')
        p3 = Place('place3')

        t1.inputs.append(p1)
        t1.inputs.append(p2)
        t1.outputs.append(p3)
        t2.inputs.append(p1)
        p2.inputs.append(t2)
        p3.inputs.append(t2)
        p1.outputs.append(t1)
        sess = create_session()
        [sess.save(x) for x in [t1,t2,t3,p1,p2,p3]]
        sess.flush()

        self.assert_result([t1], Transition, {'outputs': (Place, [{'name':'place3'}, {'name':'place1'}])})
        self.assert_result([p2], Place, {'inputs': (Transition, [{'name':'transition1'},{'name':'transition2'}])})


class M2MTest2(_base.MappedTest):
    def define_tables(self, metadata):
        Table('student', metadata,
              Column('name', String(20), primary_key=True))

        Table('course', metadata,
              Column('name', String(20), primary_key=True))

        Table('enroll', metadata,
              Column('student_id', String(20), ForeignKey('student.name'),
                     primary_key=True),
            Column('course_id', String(20), ForeignKey('course.name'),
                   primary_key=True))

    @testing.resolve_artifact_names
    def testcircular(self):
        class Student(object):
            def __init__(self, name=''):
                self.name = name
        class Course(object):
            def __init__(self, name=''):
                self.name = name

        mapper(Student, student)
        mapper(Course, course, properties={
            'students': relation(Student, enroll, lazy=True, backref='courses')})

        sess = create_session()
        s1 = Student('Student1')
        c1 = Course('Course1')
        c2 = Course('Course2')
        c3 = Course('Course3')
        s1.courses.append(c1)
        s1.courses.append(c2)
        c3.students.append(s1)
        self.assert_(len(s1.courses) == 3)
        self.assert_(len(c1.students) == 1)
        sess.save(s1)
        sess.flush()
        sess.clear()
        s = sess.query(Student).filter_by(name='Student1').one()
        c = sess.query(Course).filter_by(name='Course3').one()
        self.assert_(len(s.courses) == 3)
        del s.courses[1]
        self.assert_(len(s.courses) == 2)

    @testing.resolve_artifact_names
    def test_delete(self):
        """A many-to-many table gets cleared out with deletion from the backref side"""
        class Student(object):
            def __init__(self, name=''):
                self.name = name
        class Course(object):
            def __init__(self, name=''):
                self.name = name

        mapper(Student, student)
        mapper(Course, course, properties = {
            'students': relation(Student, enroll, lazy=True,
                                 backref='courses')})

        sess = create_session()
        s1 = Student('Student1')
        c1 = Course('Course1')
        c2 = Course('Course2')
        c3 = Course('Course3')
        s1.courses.append(c1)
        s1.courses.append(c2)
        c3.students.append(s1)
        sess.save(s1)
        sess.flush()
        sess.delete(s1)
        sess.flush()
        assert enroll.count().scalar() == 0

class M2MTest3(_base.MappedTest):
    def define_tables(self, metadata):
        Table('c', metadata,
            Column('c1', Integer, primary_key = True),
            Column('c2', String(20)))

        Table('a', metadata,
            Column('a1', Integer, primary_key=True),
            Column('a2', String(20)),
            Column('c1', Integer, ForeignKey('c.c1')))

        Table('c2a1', metadata,
            Column('c1', Integer, ForeignKey('c.c1')),
            Column('a1', Integer, ForeignKey('a.a1')))

        Table('c2a2', metadata,
            Column('c1', Integer, ForeignKey('c.c1')),
            Column('a1', Integer, ForeignKey('a.a1')))

        Table('b', metadata,
            Column('b1', Integer, primary_key=True),
            Column('a1', Integer, ForeignKey('a.a1')),
            Column('b2', sa.Boolean))

    @testing.resolve_artifact_names
    def testbasic(self):
        class C(object):pass
        class A(object):pass
        class B(object):pass

        mapper(B, b)

        mapper(A, a, properties={
            'tbs': relation(B, primaryjoin=sa.and_(b.c.a1 == a.c.a1,
                                                   b.c.b2 == True),
                            lazy=False)})

        mapper(C, c, properties={
            'a1s': relation(A, secondary=c2a1, lazy=False),
            'a2s': relation(A, secondary=c2a2, lazy=False)})

        assert create_session().query(C).compile()


if __name__ == "__main__":
    testenv.main()
