"""Tests cyclical mapper relationships.

We might want to try an automated generate of much of this, all combos of
T1<->T2, with o2m or m2o between them, and a third T3 with o2m/m2o to one/both
T1/T2.

"""
import testenv; testenv.configure_for_tests()
from testlib import testing
from testlib.sa import Table, Column, Integer, String, ForeignKey
from testlib.sa.orm import mapper, relation, backref, create_session
from testlib.testing import eq_
from testlib.assertsql import RegexSQL, ExactSQL, CompiledSQL, AllOf
from orm import _base


class SelfReferentialTest(_base.MappedTest):
    """A self-referential mapper with an additional list of child objects."""

    def define_tables(self, metadata):
        Table('t1', metadata,
              Column('c1', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('parent_c1', Integer, ForeignKey('t1.c1')),
              Column('data', String(20)))
        Table('t2', metadata,
              Column('c1', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('c1id', Integer, ForeignKey('t1.c1')),
              Column('data', String(20)))

    def setup_classes(self):
        class C1(_base.BasicEntity):
            def __init__(self, data=None):
                self.data = data

        class C2(_base.BasicEntity):
            def __init__(self, data=None):
                self.data = data

    @testing.resolve_artifact_names
    def testsingle(self):
        mapper(C1, t1, properties = {
            'c1s':relation(C1, cascade="all"),
            'parent':relation(C1,
                              primaryjoin=t1.c.parent_c1 == t1.c.c1,
                              remote_side=t1.c.c1,
                              lazy=True,
                              uselist=False)})
        a = C1('head c1')
        a.c1s.append(C1('another c1'))

        sess = create_session( )
        sess.add(a)
        sess.flush()
        sess.delete(a)
        sess.flush()

    @testing.resolve_artifact_names
    def testmanytooneonly(self):
        """

        test that the circular dependency sort can assemble a many-to-one
        dependency processor when only the object on the "many" side is
        actually in the list of modified objects.  this requires that the
        circular sort add the other side of the relation into the
        UOWTransaction so that the dependency operation can be tacked onto it.

        This also affects inheritance relationships since they rely upon
        circular sort as well.

        """
        mapper(C1, t1, properties={
            'parent':relation(C1,
                              primaryjoin=t1.c.parent_c1 == t1.c.c1,
                              remote_side=t1.c.c1)})

        c1 = C1()

        sess = create_session()
        sess.add(c1)
        sess.flush()
        sess.clear()
        c1 = sess.query(C1).get(c1.c1)
        c2 = C1()
        c2.parent = c1
        sess.add(c2)
        sess.flush()
        assert c2.parent_c1==c1.c1

    @testing.resolve_artifact_names
    def testcycle(self):
        mapper(C1, t1, properties = {
            'c1s' : relation(C1, cascade="all"),
            'c2s' : relation(mapper(C2, t2), cascade="all, delete-orphan")})

        a = C1('head c1')
        a.c1s.append(C1('child1'))
        a.c1s.append(C1('child2'))
        a.c1s[0].c1s.append(C1('subchild1'))
        a.c1s[0].c1s.append(C1('subchild2'))
        a.c1s[1].c2s.append(C2('child2 data1'))
        a.c1s[1].c2s.append(C2('child2 data2'))
        sess = create_session( )
        sess.add(a)
        sess.flush()

        sess.delete(a)
        sess.flush()


class SelfReferentialNoPKTest(_base.MappedTest):
    """A self-referential relationship that joins on a column other than the primary key column"""

    def define_tables(self, metadata):
        Table('item', metadata,
           Column('id', Integer, primary_key=True),
           Column('uuid', String(32), unique=True, nullable=False),
           Column('parent_uuid', String(32), ForeignKey('item.uuid'),
                  nullable=True))

    def setup_classes(self):
        class TT(_base.BasicEntity):
            def __init__(self):
                self.uuid = hex(id(self))

    @testing.resolve_artifact_names
    def setup_mappers(self):
        mapper(TT, item, properties={
            'children': relation(
                TT,
                remote_side=[item.c.parent_uuid],
                backref=backref('parent', remote_side=[item.c.uuid]))})

    @testing.resolve_artifact_names
    def testbasic(self):
        t1 = TT()
        t1.children.append(TT())
        t1.children.append(TT())

        s = create_session()
        s.add(t1)
        s.flush()
        s.clear()
        t = s.query(TT).filter_by(id=t1.id).one()
        eq_(t.children[0].parent_uuid, t1.uuid)

    @testing.resolve_artifact_names
    def testlazyclause(self):
        s = create_session()
        t1 = TT()
        t2 = TT()
        t1.children.append(t2)
        s.add(t1)
        s.flush()
        s.clear()

        t = s.query(TT).filter_by(id=t2.id).one()
        eq_(t.uuid, t2.uuid)
        eq_(t.parent.uuid, t1.uuid)


class InheritTestOne(_base.MappedTest):
    def define_tables(self, metadata):
        Table("parent", metadata,
            Column("id", Integer, primary_key=True),
            Column("parent_data", String(50)),
            Column("type", String(10)))

        Table("child1", metadata,
              Column("id", Integer, ForeignKey("parent.id"),
                     primary_key=True),
              Column("child1_data", String(50)))

        Table("child2", metadata,
            Column("id", Integer, ForeignKey("parent.id"),
                   primary_key=True),
            Column("child1_id", Integer, ForeignKey("child1.id"),
                   nullable=False),
            Column("child2_data", String(50)))

    def setup_classes(self):
        class Parent(_base.BasicEntity):
            pass

        class Child1(Parent):
            pass

        class Child2(Parent):
            pass

    @testing.resolve_artifact_names
    def setup_mappers(self):
        mapper(Parent, parent)
        mapper(Child1, child1, inherits=Parent)
        mapper(Child2, child2, inherits=Parent, properties=dict(
            child1=relation(Child1,
                            primaryjoin=child2.c.child1_id == child1.c.id)))

    @testing.resolve_artifact_names
    def testmanytooneonly(self):
        """test similar to SelfReferentialTest.testmanytooneonly"""

        session = create_session()

        c1 = Child1()
        c1.child1_data = "qwerty"
        session.add(c1)
        session.flush()
        session.clear()

        c1 = session.query(Child1).filter_by(child1_data="qwerty").one()
        c2 = Child2()
        c2.child1 = c1
        c2.child2_data = "asdfgh"
        session.add(c2)

        # the flush will fail if the UOW does not set up a many-to-one DP
        # attached to a task corresponding to c1, since "child1_id" is not
        # nullable
        session.flush()


class InheritTestTwo(_base.MappedTest):
    """

    The fix in BiDirectionalManyToOneTest raised this issue, regarding the
    'circular sort' containing UOWTasks that were still polymorphic, which
    could create duplicate entries in the final sort

    """

    def define_tables(self, metadata):
        Table('a', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30)),
            Column('cid', Integer, ForeignKey('c.id')))

        Table('b', metadata,
            Column('id', Integer, ForeignKey("a.id"), primary_key=True),
            Column('data', String(30)))

        Table('c', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30)),
            Column('aid', Integer,
                   ForeignKey('a.id', use_alter=True, name="foo")))

    def setup_classes(self):
        class A(_base.BasicEntity):
            pass

        class B(A):
            pass

        class C(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def test_flush(self):
        mapper(A, a, properties={
            'cs':relation(C, primaryjoin=a.c.cid==c.c.id)})

        mapper(B, b, inherits=A, inherit_condition=b.c.id == a.c.id)

        mapper(C, c, properties={
            'arel':relation(A, primaryjoin=a.c.id == c.c.aid)})

        sess = create_session()
        bobj = B()
        sess.add(bobj)
        cobj = C()
        sess.add(cobj)
        sess.flush()


class BiDirectionalManyToOneTest(_base.MappedTest):
    run_define_tables = 'each'
    
    def define_tables(self, metadata):
        Table('t1', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30)),
            Column('t2id', Integer, ForeignKey('t2.id')))
        Table('t2', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30)),
            Column('t1id', Integer,
                   ForeignKey('t1.id', use_alter=True, name="foo_fk")))
        Table('t3', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30)),
            Column('t1id', Integer, ForeignKey('t1.id'), nullable=False),
            Column('t2id', Integer, ForeignKey('t2.id'), nullable=False))

    def setup_classes(self):
        class T1(_base.BasicEntity):
            pass
        class T2(_base.BasicEntity):
            pass
        class T3(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def setup_mappers(self):
        mapper(T1, t1, properties={
            't2':relation(T2, primaryjoin=t1.c.t2id == t2.c.id)})
        mapper(T2, t2, properties={
            't1':relation(T1, primaryjoin=t2.c.t1id == t1.c.id)})
        mapper(T3, t3, properties={
            't1':relation(T1),
            't2':relation(T2)})

    @testing.resolve_artifact_names
    def test_reflush(self):
        o1 = T1()
        o1.t2 = T2()
        sess = create_session()
        sess.add(o1)
        sess.flush()

        # the bug here is that the dependency sort comes up with T1/T2 in a
        # cycle, but there are no T1/T2 objects to be saved.  therefore no
        # "cyclical subtree" gets generated, and one or the other of T1/T2
        # gets lost, and processors on T3 dont fire off.  the test will then
        # fail because the FK's on T3 are not nullable.
        o3 = T3()
        o3.t1 = o1
        o3.t2 = o1.t2
        sess.add(o3)
        sess.flush()


    @testing.resolve_artifact_names
    def test_reflush_2(self):
        """A variant on test_reflush()"""
        o1 = T1()
        o1.t2 = T2()
        sess = create_session()
        sess.add(o1)
        sess.flush()

        # in this case, T1, T2, and T3 tasks will all be in the cyclical
        # tree normally.  the dependency processors for T3 are part of the
        # 'extradeps' collection so they all get assembled into the tree
        # as well.
        o1a = T1()
        o2a = T2()
        sess.add(o1a)
        sess.add(o2a)
        o3b = T3()
        o3b.t1 = o1a
        o3b.t2 = o2a
        sess.add(o3b)

        o3 = T3()
        o3.t1 = o1
        o3.t2 = o1.t2
        sess.add(o3)
        sess.flush()


class BiDirectionalOneToManyTest(_base.MappedTest):
    """tests two mappers with a one-to-many relation to each other."""

    run_define_tables = 'each'

    def define_tables(self, metadata):
        Table('t1', metadata,
              Column('c1', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('c2', Integer, ForeignKey('t2.c1')))

        Table('t2', metadata,
              Column('c1', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('c2', Integer,
                     ForeignKey('t1.c1', use_alter=True, name='t1c1_fk')))

    def setup_classes(self):
        class C1(_base.BasicEntity):
            pass

        class C2(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def testcycle(self):
        mapper(C2, t2, properties={
            'c1s': relation(C1,
                            primaryjoin=t2.c.c1 == t1.c.c2,
                            uselist=True)})
        mapper(C1, t1, properties={
            'c2s': relation(C2,
                            primaryjoin=t1.c.c1 == t2.c.c2,
                            uselist=True)})

        a = C1()
        b = C2()
        c = C1()
        d = C2()
        e = C2()
        f = C2()
        a.c2s.append(b)
        d.c1s.append(c)
        b.c1s.append(c)
        sess = create_session()
        sess.add_all((a, b, c, d, e, f))
        sess.flush()


class BiDirectionalOneToManyTest2(_base.MappedTest):
    """Two mappers with a one-to-many relation to each other, with a second one-to-many on one of the mappers"""

    run_define_tables = 'each'

    def define_tables(self, metadata):
        Table('t1', metadata,
              Column('c1', Integer, primary_key=True),
              Column('c2', Integer, ForeignKey('t2.c1')),
              test_needs_autoincrement=True)

        Table('t2', metadata,
              Column('c1', Integer, primary_key=True),
              Column('c2', Integer,
                     ForeignKey('t1.c1', use_alter=True, name='t1c1_fq')),
              test_needs_autoincrement=True)

        Table('t1_data', metadata,
              Column('c1', Integer, primary_key=True),
              Column('t1id', Integer, ForeignKey('t1.c1')),
              Column('data', String(20)),
              test_needs_autoincrement=True)

    def setup_classes(self):
        class C1(_base.BasicEntity):
            pass

        class C2(_base.BasicEntity):
            pass

        class C1Data(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def setup_mappers(self):
        mapper(C2, t2, properties={
            'c1s': relation(C1,
                            primaryjoin=t2.c.c1 == t1.c.c2,
                            uselist=True)})
        mapper(C1, t1, properties={
            'c2s': relation(C2,
                             primaryjoin=t1.c.c1 == t2.c.c2,
                             uselist=True),
            'data': relation(mapper(C1Data, t1_data))})

    @testing.resolve_artifact_names
    def testcycle(self):
        a = C1()
        b = C2()
        c = C1()
        d = C2()
        e = C2()
        f = C2()
        a.c2s.append(b)
        d.c1s.append(c)
        b.c1s.append(c)
        a.data.append(C1Data(data='c1data1'))
        a.data.append(C1Data(data='c1data2'))
        c.data.append(C1Data(data='c1data3'))
        sess = create_session()
        sess.add_all((a, b, c, d, e, f))
        sess.flush()

        sess.delete(d)
        sess.delete(c)
        sess.flush()

class OneToManyManyToOneTest(_base.MappedTest):
    """

    Tests two mappers, one has a one-to-many on the other mapper, the other
    has a separate many-to-one relationship to the first.  two tests will have
    a row for each item that is dependent on the other.  without the
    "post_update" flag, such relationships raise an exception when
    dependencies are sorted.

    """
    run_define_tables = 'each'
    
    def define_tables(self, metadata):
        Table('ball', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('person_id', Integer,
                     ForeignKey('person.id', use_alter=True, name='fk_person_id')),
              Column('data', String(30)))

        Table('person', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('favorite_ball_id', Integer, ForeignKey('ball.id')),
              Column('data', String(30)))

    def setup_classes(self):
        class Person(_base.BasicEntity):
            pass

        class Ball(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def testcycle(self):
        """
        This test has a peculiar aspect in that it doesnt create as many
        dependent relationships as the other tests, and revealed a small
        glitch in the circular dependency sorting.

        """
        mapper(Ball, ball)
        mapper(Person, person, properties=dict(
            balls=relation(Ball,
                           primaryjoin=ball.c.person_id == person.c.id,
                           remote_side=ball.c.person_id),
            favorite=relation(Ball,
                              primaryjoin=person.c.favorite_ball_id == ball.c.id,
                              remote_side=ball.c.id)))

        b = Ball()
        p = Person()
        p.balls.append(b)
        sess = create_session()
        sess.add(p)
        sess.flush()

    @testing.resolve_artifact_names
    def testpostupdate_m2o(self):
        """A cycle between two rows, with a post_update on the many-to-one"""
        mapper(Ball, ball)
        mapper(Person, person, properties=dict(
            balls=relation(Ball,
                           primaryjoin=ball.c.person_id == person.c.id,
                           remote_side=ball.c.person_id,
                           post_update=False,
                           cascade="all, delete-orphan"),
            favorite=relation(Ball,
                              primaryjoin=person.c.favorite_ball_id == ball.c.id,
                              remote_side=person.c.favorite_ball_id,
                              post_update=True)))

        b = Ball(data='some data')
        p = Person(data='some data')
        p.balls.append(b)
        p.balls.append(Ball(data='some data'))
        p.balls.append(Ball(data='some data'))
        p.balls.append(Ball(data='some data'))
        p.favorite = b
        sess = create_session()
        sess.add(b)
        sess.add(p)

        self.assert_sql_execution(
            testing.db,
            sess.flush,
            RegexSQL("^INSERT INTO person", {'data':'some data'}),
            RegexSQL("^INSERT INTO ball", lambda c: {'person_id':p.id, 'data':'some data'}),
            RegexSQL("^INSERT INTO ball", lambda c: {'person_id':p.id, 'data':'some data'}),
            RegexSQL("^INSERT INTO ball", lambda c: {'person_id':p.id, 'data':'some data'}),
            RegexSQL("^INSERT INTO ball", lambda c: {'person_id':p.id, 'data':'some data'}),
            ExactSQL("UPDATE person SET favorite_ball_id=:favorite_ball_id "
                        "WHERE person.id = :person_id",
                        lambda ctx:{'favorite_ball_id':p.favorite.id, 'person_id':p.id}
             ),
        )

        sess.delete(p)

        self.assert_sql_execution(
            testing.db, 
            sess.flush, 
            ExactSQL("UPDATE person SET favorite_ball_id=:favorite_ball_id "
                "WHERE person.id = :person_id",
                lambda ctx: {'person_id': p.id, 'favorite_ball_id': None}),
            ExactSQL("DELETE FROM ball WHERE ball.id = :id", None), # lambda ctx:[{'id': 1L}, {'id': 4L}, {'id': 3L}, {'id': 2L}])
            ExactSQL("DELETE FROM person WHERE person.id = :id", lambda ctx:[{'id': p.id}])
        )

    @testing.resolve_artifact_names
    def testpostupdate_o2m(self):
        """A cycle between two rows, with a post_update on the one-to-many"""

        mapper(Ball, ball)
        mapper(Person, person, properties=dict(
            balls=relation(Ball,
                           primaryjoin=ball.c.person_id == person.c.id,
                           remote_side=ball.c.person_id,
                           cascade="all, delete-orphan",
                           post_update=True,
                           backref='person'),
            favorite=relation(Ball,
                              primaryjoin=person.c.favorite_ball_id == ball.c.id,
                              remote_side=person.c.favorite_ball_id)))

        b = Ball(data='some data')
        p = Person(data='some data')
        p.balls.append(b)
        b2 = Ball(data='some data')
        p.balls.append(b2)
        b3 = Ball(data='some data')
        p.balls.append(b3)
        b4 = Ball(data='some data')
        p.balls.append(b4)
        p.favorite = b
        sess = create_session()
        sess.add_all((b,p,b2,b3,b4))

        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL("INSERT INTO ball (person_id, data) "
             "VALUES (:person_id, :data)",
             {'person_id':None, 'data':'some data'}),

            CompiledSQL("INSERT INTO ball (person_id, data) "
             "VALUES (:person_id, :data)",
             {'person_id':None, 'data':'some data'}),

            CompiledSQL("INSERT INTO ball (person_id, data) "
             "VALUES (:person_id, :data)",
             {'person_id':None, 'data':'some data'}),

            CompiledSQL("INSERT INTO ball (person_id, data) "
             "VALUES (:person_id, :data)",
             {'person_id':None, 'data':'some data'}),

            CompiledSQL("INSERT INTO person (favorite_ball_id, data) "
             "VALUES (:favorite_ball_id, :data)",
             lambda ctx:{'favorite_ball_id':b.id, 'data':'some data'}),

            AllOf(
            CompiledSQL("UPDATE ball SET person_id=:person_id "
             "WHERE ball.id = :ball_id",
             lambda ctx:{'person_id':p.id,'ball_id':b.id}),

            CompiledSQL("UPDATE ball SET person_id=:person_id "
             "WHERE ball.id = :ball_id",
             lambda ctx:{'person_id':p.id,'ball_id':b2.id}),

            CompiledSQL("UPDATE ball SET person_id=:person_id "
             "WHERE ball.id = :ball_id",
             lambda ctx:{'person_id':p.id,'ball_id':b3.id}),

            CompiledSQL("UPDATE ball SET person_id=:person_id "
             "WHERE ball.id = :ball_id",
             lambda ctx:{'person_id':p.id,'ball_id':b4.id})
            ),
        )
        
        sess.delete(p)
        
        self.assert_sql_execution(testing.db, sess.flush, 
            AllOf(CompiledSQL("UPDATE ball SET person_id=:person_id "
             "WHERE ball.id = :ball_id",
             lambda ctx:{'person_id': None, 'ball_id': b.id}),

            CompiledSQL("UPDATE ball SET person_id=:person_id "
             "WHERE ball.id = :ball_id",
             lambda ctx:{'person_id': None, 'ball_id': b2.id}),

            CompiledSQL("UPDATE ball SET person_id=:person_id "
             "WHERE ball.id = :ball_id",
             lambda ctx:{'person_id': None, 'ball_id': b3.id}),

            CompiledSQL("UPDATE ball SET person_id=:person_id "
             "WHERE ball.id = :ball_id",
             lambda ctx:{'person_id': None, 'ball_id': b4.id})),

            CompiledSQL("DELETE FROM person WHERE person.id = :id",
             lambda ctx:[{'id':p.id}]),

            CompiledSQL("DELETE FROM ball WHERE ball.id = :id",
             lambda ctx:[{'id': b.id},
                         {'id': b2.id},
                         {'id': b3.id},
                         {'id': b4.id}])
        )


class SelfReferentialPostUpdateTest(_base.MappedTest):
    """Post_update on a single self-referential mapper"""

    def define_tables(self, metadata):
        Table('node', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('path', String(50), nullable=False),
              Column('parent_id', Integer,
                     ForeignKey('node.id'), nullable=True),
              Column('prev_sibling_id', Integer,
                     ForeignKey('node.id'), nullable=True),
              Column('next_sibling_id', Integer,
                     ForeignKey('node.id'), nullable=True))

    def setup_classes(self):
        class Node(_base.BasicEntity):
            def __init__(self, path=''):
                self.path = path

    @testing.resolve_artifact_names
    def testbasic(self):
        """Post_update only fires off when needed.

        This test case used to produce many superfluous update statements,
        particularly upon delete

        """

        mapper(Node, node, properties={
            'children': relation(
                Node,
                primaryjoin=node.c.id==node.c.parent_id,
                lazy=True,
                cascade="all",
                backref=backref("parent", remote_side=node.c.id)
            ),
            'prev_sibling': relation(
                Node,
                primaryjoin=node.c.prev_sibling_id==node.c.id,
                remote_side=node.c.id,
                lazy=True,
                uselist=False),
            'next_sibling': relation(
                Node,
                primaryjoin=node.c.next_sibling_id==node.c.id,
                remote_side=node.c.id,
                lazy=True,
                uselist=False,
                post_update=True)})

        session = create_session()

        def append_child(parent, child):
            if parent.children:
                parent.children[-1].next_sibling = child
                child.prev_sibling = parent.children[-1]
            parent.children.append(child)

        def remove_child(parent, child):
            child.parent = None
            node = child.next_sibling
            node.prev_sibling = child.prev_sibling
            child.prev_sibling.next_sibling = node
            session.delete(child)
        root = Node('root')

        about = Node('about')
        cats = Node('cats')
        stories = Node('stories')
        bruce = Node('bruce')

        append_child(root, about)
        assert(about.prev_sibling is None)
        append_child(root, cats)
        assert(cats.prev_sibling is about)
        assert(cats.next_sibling is None)
        assert(about.next_sibling is cats)
        assert(about.prev_sibling is None)
        append_child(root, stories)
        append_child(root, bruce)
        session.add(root)
        session.flush()

        remove_child(root, cats)
        # pre-trigger lazy loader on 'cats' to make the test easier
        cats.children
        self.assert_sql_execution(
            testing.db, 
            session.flush,
            CompiledSQL("UPDATE node SET prev_sibling_id=:prev_sibling_id "
             "WHERE node.id = :node_id",
             lambda ctx:{'prev_sibling_id':about.id, 'node_id':stories.id}),

            CompiledSQL("UPDATE node SET next_sibling_id=:next_sibling_id "
             "WHERE node.id = :node_id",
             lambda ctx:{'next_sibling_id':stories.id, 'node_id':about.id}),

            CompiledSQL("UPDATE node SET next_sibling_id=:next_sibling_id "
             "WHERE node.id = :node_id",
             lambda ctx:{'next_sibling_id':None, 'node_id':cats.id}),
             
            CompiledSQL("DELETE FROM node WHERE node.id = :id",
             lambda ctx:[{'id':cats.id}])
        )


class SelfReferentialPostUpdateTest2(_base.MappedTest):

    def define_tables(self, metadata):
        Table("a_table", metadata,
              Column("id", Integer(), primary_key=True),
              Column("fui", String(128)),
              Column("b", Integer(), ForeignKey("a_table.id")))

    def setup_classes(self):
        class A(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def testbasic(self):
        """
        Test that post_update remembers to be involved in update operations as
        well, since it replaces the normal dependency processing completely
        [ticket:413]

        """

        mapper(A, a_table, properties={
            'foo': relation(A,
                            remote_side=[a_table.c.id],
                            post_update=True)})

        session = create_session()

        f1 = A(fui="f1")
        session.add(f1)
        session.flush()

        f2 = A(fui="f2", foo=f1)

        # at this point f1 is already inserted.  but we need post_update
        # to fire off anyway
        session.add(f2)
        session.flush()
        session.clear()

        f1 = session.query(A).get(f1.id)
        f2 = session.query(A).get(f2.id)
        assert f2.foo is f1


if __name__ == "__main__":
    testenv.main()
