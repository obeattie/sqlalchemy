"""bridges the PropertyLoader (i.e. a relation()) and the UOWTransaction 
together to allow processing of scalar- and list-based dependencies at flush time."""

from sync import ONETOMANY,MANYTOONE,MANYTOMANY
from sqlalchemy import sql

class DependencyProcessor(object):
    def __init__(self, key, syncrules, cascade, secondary=None, association=None, is_backref=False, post_update=False):
        # TODO: update instance variable names to be more meaningful
        self.syncrules = syncrules
        self.cascade = cascade
        self.mapper = syncrules.child_mapper
        self.parent = syncrules.parent_mapper
        self.association = association
        self.secondary = secondary
        self.direction = syncrules.direction
        self.is_backref = is_backref
        self.post_update = post_update
        self.key = key

    class MapperStub(object):
        """poses as a Mapper representing the association table in a many-to-many
        join, when performing a commit().  

        The Task objects in the objectstore module treat it just like
        any other Mapper, but in fact it only serves as a "dependency" placeholder
        for the many-to-many update task."""
        def __init__(self, mapper):
            self.mapper = mapper
        def save_obj(self, *args, **kwargs):
            pass
        def delete_obj(self, *args, **kwargs):
            pass
        def _primary_mapper(self):
            return self

    def register_dependencies(self, uowcommit):
        """tells a UOWTransaction what mappers are dependent on which, with regards
        to the two or three mappers handled by this PropertyLoader.

        Also registers itself as a "processor" for one of its mappers, which
        will be executed after that mapper's objects have been saved or before
        they've been deleted.  The process operation manages attributes and dependent
        operations upon the objects of one of the involved mappers."""
        if self.association is not None:
            # association object.  our mapper should be dependent on both
            # the parent mapper and the association object mapper.
            # this is where we put the "stub" as a marker, so we get
            # association/parent->stub->self, then we process the child
            # elments after the 'stub' save, which is before our own
            # mapper's save.
            stub = DependencyProcessor.MapperStub(self.association)
            uowcommit.register_dependency(self.parent, stub)
            uowcommit.register_dependency(self.association, stub)
            uowcommit.register_dependency(stub, self.mapper)
            uowcommit.register_processor(stub, self, self.parent, False)
            uowcommit.register_processor(stub, self, self.parent, True)

        elif self.direction == MANYTOMANY:
            # many-to-many.  create a "Stub" mapper to represent the
            # "middle table" in the relationship.  This stub mapper doesnt save
            # or delete any objects, but just marks a dependency on the two
            # related mappers.  its dependency processor then populates the
            # association table.

            if self.is_backref:
                # if we are the "backref" half of a two-way backref 
                # relationship, let the other mapper handle inserting the rows
                return
            stub = DependencyProcessor.MapperStub(self.mapper)
            uowcommit.register_dependency(self.parent, stub)
            uowcommit.register_dependency(self.mapper, stub)
            uowcommit.register_processor(stub, self, self.parent, False)
            uowcommit.register_processor(stub, self, self.parent, True)
        elif self.direction == ONETOMANY:
            if self.post_update:
                stub = DependencyProcessor.MapperStub(self.mapper)
                uowcommit.register_dependency(self.mapper, stub)
                uowcommit.register_dependency(self.parent, stub)
                uowcommit.register_processor(stub, self, self.parent, False)
                uowcommit.register_processor(stub, self, self.parent, True)
            else:
                uowcommit.register_dependency(self.parent, self.mapper)
                uowcommit.register_processor(self.parent, self, self.parent, False)
                uowcommit.register_processor(self.parent, self, self.parent, True)
        elif self.direction == MANYTOONE:
            if self.post_update:
                stub = DependencyProcessor.MapperStub(self.mapper)
                uowcommit.register_dependency(self.mapper, stub)
                uowcommit.register_dependency(self.parent, stub)
                uowcommit.register_processor(stub, self, self.parent, False)
                uowcommit.register_processor(stub, self, self.parent, True)
            else:
                uowcommit.register_dependency(self.mapper, self.parent)
                uowcommit.register_processor(self.mapper, self, self.parent, False)
                uowcommit.register_processor(self.mapper, self, self.parent, True)
        else:
            raise AssertionError(" no foreign key ?")

    # TODO: this method should be moved to an external object
    def get_object_dependencies(self, obj, uowcommit, passive = True):
        return uowcommit.uow.attributes.get_history(obj, self.key, passive = passive)

    # TODO: this method should be moved to an external object
    def whose_dependent_on_who(self, obj1, obj2):
        """given an object pair assuming obj2 is a child of obj1, returns a tuple
        with the dependent object second, or None if they are equal.  
        used by objectstore's object-level topological sort (i.e. cyclical 
        table dependency)."""
        if obj1 is obj2:
            return None
        elif self.direction == ONETOMANY:
            return (obj1, obj2)
        else:
            return (obj2, obj1)

    # TODO: this method should be moved to an external object
    def process_dependencies(self, task, deplist, uowcommit, delete = False):
        """this method is called during a commit operation to synchronize data between a parent and child object.  
        it also can establish child or parent objects within the unit of work as "to be saved" or "deleted" 
        in some cases."""
        #print self.mapper.table.name + " " + self.key + " " + repr(len(deplist)) + " process_dep isdelete " + repr(delete) + " direction " + repr(self.direction)

        def getlist(obj, passive=True):
            return self.get_object_dependencies(obj, uowcommit, passive)

        connection = uowcommit.transaction.connection(self.mapper)

        # plugin point

        if self.direction == MANYTOMANY:
            secondary_delete = []
            secondary_insert = []
            if delete:
                for obj in deplist:
                    childlist = getlist(obj, False)
                    for child in childlist.deleted_items() + childlist.unchanged_items():
                        associationrow = {}
                        self._synchronize(obj, child, associationrow, False)
                        secondary_delete.append(associationrow)
            else:
                for obj in deplist:
                    childlist = getlist(obj)
                    if childlist is None: continue
                    for child in childlist.added_items():
                        associationrow = {}
                        self._synchronize(obj, child, associationrow, False)
                        secondary_insert.append(associationrow)
                    for child in childlist.deleted_items():
                        associationrow = {}
                        self._synchronize(obj, child, associationrow, False)
                        secondary_delete.append(associationrow)
            if len(secondary_delete):
                # TODO: precompile the delete/insert queries and store them as instance variables
                # on the PropertyLoader
                statement = self.secondary.delete(sql.and_(*[c == sql.bindparam(c.key) for c in self.secondary.c]))
                connection.execute(statement, secondary_delete)
            if len(secondary_insert):
                statement = self.secondary.insert()
                connection.execute(statement, secondary_insert)
        elif self.direction == MANYTOONE and delete:
            if self.cascade.delete_orphan:
                for obj in deplist:
                    childlist = getlist(obj, False)
                    for child in childlist.deleted_items() + childlist.unchanged_items():
                        if child is not None and childlist.hasparent(child) is False:
                            uowcommit.register_object(child, isdelete=True)
            elif self.post_update:
                # post_update means we have to update our row to not reference the child object
                # before we can DELETE the row
                for obj in deplist:
                    self._synchronize(obj, None, None, True)
                    uowcommit.register_object(obj, postupdate=True)
        elif self.direction == ONETOMANY and delete:
            # head object is being deleted, and we manage its list of child objects
            # the child objects have to have their foreign key to the parent set to NULL
            if self.cascade.delete_orphan and not self.post_update:
                for obj in deplist:
                    childlist = getlist(obj, False)
                    for child in childlist.deleted_items():
                        if child is not None and childlist.hasparent(child) is False:
                            uowcommit.register_object(child, isdelete=True)
                    for child in childlist.unchanged_items():
                        if child is not None:
                            uowcommit.register_object(child, isdelete=True)
            else:
                for obj in deplist:
                    childlist = getlist(obj, False)
                    for child in childlist.deleted_items():
                        if child is not None and childlist.hasparent(child) is False:
                            self._synchronize(obj, child, None, True)
                            uowcommit.register_object(child, postupdate=self.post_update)
                    for child in childlist.unchanged_items():
                        if child is not None:
                            self._synchronize(obj, child, None, True)
                            uowcommit.register_object(child, postupdate=self.post_update)
        elif self.association is not None:
            # manage association objects.
            for obj in deplist:
                childlist = getlist(obj, passive=True)
                if childlist is None: continue

                #print "DIRECTION", self.direction
                d = {}
                for child in childlist:
                    self._synchronize(obj, child, None, False)
                    key = self.mapper.instance_key(child)
                    #print "SYNCHRONIZED", child, "INSTANCE KEY", key
                    d[key] = child
                    uowcommit.unregister_object(child)

                for child in childlist.added_items():
                    uowcommit.register_object(child)
                    key = self.mapper.instance_key(child)
                    #print "ADDED, INSTANCE KEY", key
                    d[key] = child

                for child in childlist.unchanged_items():
                    key = self.mapper.instance_key(child)
                    o = d[key]
                    o._instance_key= key

                for child in childlist.deleted_items():
                    key = self.mapper.instance_key(child)
                    #print "DELETED, INSTANCE KEY", key
                    if d.has_key(key):
                        o = d[key]
                        o._instance_key = key
                        uowcommit.unregister_object(child)
                    else:
                        #print "DELETE ASSOC OBJ", repr(child)
                        uowcommit.register_object(child, isdelete=True)
        else:
            for obj in deplist:
                childlist = getlist(obj, passive=True)
                if childlist is not None:
                    for child in childlist.added_items():
                        self._synchronize(obj, child, None, False)
                        if self.direction == ONETOMANY and child is not None:
                            uowcommit.register_object(child, postupdate=self.post_update)
                if self.direction == MANYTOONE:
                    uowcommit.register_object(obj, postupdate=self.post_update)
                else:
                    for child in childlist.deleted_items():
                        if not self.cascade.delete_orphan:
                            self._synchronize(obj, child, None, True)
                            uowcommit.register_object(child, isdelete=False)
                        elif childlist.hasparent(child) is False:
                            uowcommit.register_object(child, isdelete=True)

    # TODO: this method should be moved to an external object
    def _synchronize(self, obj, child, associationrow, clearkeys):
        """called during a commit to execute the full list of syncrules on the 
        given object/child/optional association row"""
        if self.direction == ONETOMANY:
            source = obj
            dest = child
        elif self.direction == MANYTOONE:
            source = child
            dest = obj
        elif self.direction == MANYTOMANY:
            dest = associationrow
            source = None

        if dest is None:
            return

        self.syncrules.execute(source, dest, obj, child, clearkeys)
