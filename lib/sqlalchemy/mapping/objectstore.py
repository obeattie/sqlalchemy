# objectstore.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


"""maintains all currently loaded objects in memory, using the "identity map" pattern.  Also
provides a "unit of work" object which tracks changes to objects so that they may be properly
persisted within a transactional scope."""

import thread
import sqlalchemy
import sqlalchemy.util as util
import sqlalchemy.attributes as attributes
import topological
import weakref
import string
import StringIO

__all__ = ['get_id_key', 'get_row_key', 'commit', 'update', 'clear', 'delete', 
        'begin', 'has_key', 'has_instance', 'UnitOfWork']

LOG = False

def get_id_key(ident, class_, table):
    """returns an identity-map key for use in storing/retrieving an item from the identity
    map, given a tuple of the object's primary key values.

    ident - a tuple of primary key values corresponding to the object to be stored.  these
    values should be in the same order as the primary keys of the table 
    
    class_ - a reference to the object's class

    table - a Table object where the object's primary fields are stored.

    selectable - a Selectable object which represents all the object's column-based fields.
    this Selectable may be synonymous with the table argument or can be a larger construct
    containing that table. return value: a tuple object which is used as an identity key. """
    return (class_, table.hash_key(), tuple(ident))
def get_row_key(row, class_, table, primary_key):
    """returns an identity-map key for use in storing/retrieving an item from the identity
    map, given a result set row.

    row - a sqlalchemy.dbengine.RowProxy instance or other map corresponding result-set
    column names to their values within a row.

    class_ - a reference to the object's class

    table - a Table object where the object's primary fields are stored.

    selectable - a Selectable object which represents all the object's column-based fields.
    this Selectable may be synonymous with the table argument or can be a larger construct
    containing that table. return value: a tuple object which is used as an identity key.
    """
    return (class_, table.hash_key(), tuple([row[column] for column in primary_key]))

def begin():
    """begins a new UnitOfWork transaction.  the next commit will affect only
    objects that are created, modified, or deleted following the begin statement."""
    uow().begin()
    
def commit(*obj):
    """commits the current UnitOfWork transaction.  if a transaction was begun 
    via begin(), commits only those objects that were created, modified, or deleted
    since that begin statement.  otherwise commits all objects that have been
    changed."""
    uow().commit(*obj)
    
def clear():
    """removes all current UnitOfWorks and IdentityMaps for this thread and 
    establishes a new one.  It is probably a good idea to discard all
    current mapped object instances, as they are no longer in the Identity Map."""
    uow.set(UnitOfWork())

def delete(*obj):
    """registers the given objects as to be deleted upon the next commit"""
    uw = uow()
    for o in obj:
        uw.register_deleted(o)
    
def has_key(key):
    """returns True if the current thread-local IdentityMap contains the given instance key"""
    return uow().identity_map.has_key(key)

def has_instance(instance):
    """returns True if the current thread-local IdentityMap contains the given instance"""
    return uow().identity_map.has_key(instance_key(instance))

def instance_key(instance):
    """returns the IdentityMap key for the given instance"""
    return object_mapper(instance).instance_key(instance)

def import_instance(instance):
    """places the given instance in the current thread's unit of work context,
    either in the current IdentityMap or marked as "new".  Returns either the object
    or the current corresponding version in the Identity Map.
    
    this method should be used for any object instance that is coming from a serialized
    storage, from another thread (assuming the regular threaded unit of work model), or any
    case where the instance was loaded/created corresponding to a different base unitofwork
    than the current one."""
    if instance is None:
        return None
    key = getattr(instance, '_instance_key', None)
    mapper = object_mapper(instance)
    key = (key[0], repr(mapper.table), key[2])
    u = uow()
    if key is not None:
        if u.identity_map.has_key(key):
            return u.identity_map[key]
        else:
            instance._instance_key = key
            u.identity_map[key] = instance
    else:
        u.register_new(instance)
    return instance
    
class UOWListElement(attributes.ListElement):
    def __init__(self, obj, key, data=None, deleteremoved=False, **kwargs):
        attributes.ListElement.__init__(self, obj, key, data=data, **kwargs)
        self.deleteremoved = deleteremoved
    def list_value_changed(self, obj, key, item, listval, isdelete):
        uow().modified_lists.append(self)
        if self.deleteremoved and isdelete:
            uow().register_deleted(item)
    def append(self, item, _mapper_nohistory = False):
        if _mapper_nohistory:
            self.append_nohistory(item)
        else:
            attributes.ListElement.append(self, item)
            
class UOWAttributeManager(attributes.AttributeManager):
    def __init__(self):
        attributes.AttributeManager.__init__(self)
        
    def value_changed(self, obj, key, value):
        if hasattr(obj, '_instance_key'):
            uow().register_dirty(obj)
        else:
            uow().register_new(obj)

    def create_list(self, obj, key, list_, **kwargs):
        return UOWListElement(obj, key, list_, **kwargs)
        
class UnitOfWork(object):
    def __init__(self, parent = None, is_begun = False):
        self.is_begun = is_begun
        if parent is not None:
            self.identity_map = parent.identity_map
        else:
            self.identity_map = weakref.WeakValueDictionary()
            
        self.attributes = global_attributes
        self.new = util.HashSet(ordered = True)
        self.dirty = util.HashSet()
        self.modified_lists = util.HashSet()
        self.deleted = util.HashSet()
        self.parent = parent

    def get(self, class_, *id):
        return sqlalchemy.mapper.object_mapper(class_).get(*id)

    def _get(self, key):
        return self.identity_map[key]
        
    def _put(self, key, obj):
        self.identity_map[key] = obj

    def has_key(self, key):
        return self.identity_map.has_key(key)
        
    def _remove_deleted(self, obj):
        if hasattr(obj, "_instance_key"):
            del self.identity_map[obj._instance_key]
        try:            
            del self.deleted[obj]
        except KeyError:
            pass
        try:
            del self.dirty[obj]
        except KeyError:
            pass
        try:
            del self.new[obj]
        except KeyError:
            pass
        self.attributes.commit(obj)
        self.attributes.remove(obj)
        
    def update(self, obj):
        """called to add an object to this UnitOfWork as though it were loaded from the DB,
        but is actually coming from somewhere else, like a web session or similar."""
        self._put(obj._instance_key, obj)
        self.register_dirty(obj)
        
    def register_attribute(self, class_, key, uselist, **kwargs):
        self.attributes.register_attribute(class_, key, uselist, **kwargs)

    def register_callable(self, obj, key, func, uselist, **kwargs):
        self.attributes.set_callable(obj, key, func, uselist, **kwargs)
        
    def register_clean(self, obj):
        try:
            del self.dirty[obj]
        except KeyError:
            pass
        try:
            del self.new[obj]
        except KeyError:
            pass
        self._put(obj._instance_key, obj)
        self.attributes.commit(obj)
        
    def register_new(self, obj):
        self.new.append(obj)
        
    def register_dirty(self, obj):
        self.dirty.append(obj)
            
    def is_dirty(self, obj):
        if not self.dirty.contains(obj):
            return False
        else:
            return True
        
    def register_deleted(self, obj):
        self.deleted.append(obj)  
        mapper = object_mapper(obj)
        # TODO: should the cascading delete dependency thing
        # happen wtihin PropertyLoader.process_dependencies ?
        mapper.register_deleted(obj, self)

    def unregister_deleted(self, obj):
        try:
            self.deleted.remove(obj)
        except KeyError:
            pass
            
    # TODO: tie in register_new/register_dirty with table transaction begins ?
    def begin(self):
        u = UnitOfWork(self, True)
        uow.set(u)
        
    def commit(self, *objects):
        commit_context = UOWTransaction(self)

        if len(objects):
            objset = util.HashSet(iter=objects)
        else:
            objset = None

        for obj in [n for n in self.new] + [d for d in self.dirty]:
            if objset is not None and not objset.contains(obj):
                continue
            if self.deleted.contains(obj):
                continue
            commit_context.register_object(obj)
        for item in self.modified_lists:
            obj = item.obj
            if objset is not None and not objset.contains(obj):
                continue
            if self.deleted.contains(obj):
                continue
            commit_context.register_object(obj, listonly = True)
            for o in item.added_items() + item.deleted_items():
                if self.deleted.contains(o):
                    continue
                commit_context.register_object(o, listonly=True)
        for obj in self.deleted:
            if objset is not None and not objset.contains(obj):
                continue
            commit_context.register_object(obj, isdelete=True)
                
        engines = util.HashSet()
        for mapper in commit_context.mappers:
            for e in mapper.engines:
                engines.append(e)
        
        echo_commit = False        
        for e in engines:
            echo_commit = echo_commit or e.echo_uow
            e.begin()
        try:
            commit_context.execute(echo=echo_commit)
        except:
            for e in engines:
                e.rollback()
            if self.parent:
                self.rollback()
            raise
        for e in engines:
            e.commit()
            
        commit_context.post_exec()
        
        if self.parent:
            uow.set(self.parent)

    def rollback_object(self, obj):
        self.attributes.rollback(obj)

    def rollback(self):
        if not self.is_begun:
            raise "UOW transaction is not begun"
        # TODO: locate only objects that are dirty/new/deleted in this UOW,
        # roll only those back.
        for obj in self.deleted + self.dirty + self.new:
            self.attributes.rollback(obj)
        uow.set(self.parent)
            
class UOWTransaction(object):
    """handles the details of organizing and executing transaction tasks 
    during a UnitOfWork object's commit() operation."""
    def __init__(self, uow):
        self.uow = uow

        #  unique list of all the mappers we come across
        self.mappers = util.HashSet()
        self.dependencies = {}
        self.tasks = {}
        self.saved_objects = util.HashSet()
        self.saved_lists = util.HashSet()
        self.deleted_objects = util.HashSet()
        self.deleted_lists = util.HashSet()

    def register_object(self, obj, isdelete = False, listonly = False, **kwargs):
        """adds an object to this UOWTransaction to be updated in the database.

        'isdelete' indicates whether the object is to be deleted or saved (update/inserted).

        'listonly', indicates that only this object's dependency relationships should be
        refreshed/updated to reflect a recent save/upcoming delete operation, but not a full
        save/delete operation on the object itself, unless an additional save/delete
        registration is entered for the object."""
        #print "REGISTER", repr(obj), repr(getattr(obj, '_instance_key', None)), str(isdelete), str(listonly)
        
        # things can get really confusing if theres duplicate instances floating around,
        # so make sure everything is OK
        if hasattr(obj, '_instance_key') and not self.uow.identity_map.has_key(obj._instance_key):
            raise "Detected a mapped object not present in the current thread's Identity Map: '%s'.  Use objectstore.import_instance() to place deserialized instances or instances from other threads" % repr(obj._instance_key)
            
        mapper = object_mapper(obj)
        self.mappers.append(mapper)
        task = self.get_task_by_mapper(mapper)
        task.append(obj, listonly, isdelete=isdelete, **kwargs)

    def unregister_object(self, obj):
        mapper = object_mapper(obj)
        task = self.get_task_by_mapper(mapper)
        task.delete(obj)
        
    def get_task_by_mapper(self, mapper):
        """every individual mapper involved in the transaction has a single
        corresponding UOWTask object, which stores all the operations involved
        with that mapper as well as operations dependent on those operations.
        this method returns or creates the single per-transaction instance of
        UOWTask that exists for that mapper."""
        try:
            return self.tasks[mapper]
        except KeyError:
            return UOWTask(self, mapper)
            
    def register_dependency(self, mapper, dependency):
        """called by mapper.PropertyLoader to register the objects handled by
        one mapper being dependent on the objects handled by another."""
        self.dependencies[(mapper, dependency)] = True

    def register_processor(self, mapper, processor, mapperfrom, isdeletefrom):
        """called by mapper.PropertyLoader to register itself as a "processor", which
        will be associated with a particular UOWTask, and be given a list of "dependent"
        objects corresponding to another UOWTask to be processed, either after that secondary
        task saves its objects or before it deletes its objects."""
        # when the task from "mapper" executes, take the objects from the task corresponding
        # to "mapperfrom"'s list of save/delete objects, and send them to "processor"
        # for dependency processing
        task = self.get_task_by_mapper(mapper)
        targettask = self.get_task_by_mapper(mapperfrom)
        task.dependencies.append(UOWDependencyProcessor(processor, targettask, isdeletefrom))

    def register_saved_object(self, obj):
        self.saved_objects.append(obj)

    def register_saved_list(self, listobj):
        self.saved_lists.append(listobj)

    def register_deleted_list(self, listobj):
        self.deleted_lists.append(listobj)
        
    def register_deleted_object(self, obj):
        self.deleted_objects.append(obj)
        
    def execute(self, echo=False):
        for task in self.tasks.values():
            task.mapper.register_dependencies(self)

        head = self._sort_dependencies()
        if LOG or echo:
            print "Task dump:\n" + head.dump()
        if head is not None:
            head.execute(self)
            
    def post_exec(self):
        """after an execute/commit is completed, all of the objects and lists that have
        been committed are updated in the parent UnitOfWork object to mark them as clean."""
        for obj in self.saved_objects:
            mapper = object_mapper(obj)
            obj._instance_key = mapper.instance_key(obj)
            self.uow.register_clean(obj)

        for obj in self.saved_lists:
            try:
                obj.commit()
                del self.uow.modified_lists[obj]
            except KeyError:
                pass

        for obj in self.deleted_objects:
            self.uow._remove_deleted(obj)
        
        for obj in self.deleted_lists:
            try:
                obj.commit()
                del self.uow.modified_lists[obj]
            except KeyError:
                pass

    def _sort_dependencies(self):
        """creates a hierarchical tree of dependent tasks.  the root node is returned.
        when the root node is executed, it also executes its child tasks recursively."""
        bymapper = {}
        
        def sort_hier(node):
            if node is None:
                return None
            task = bymapper.get(node.item, None)
            if task is None:
                task = UOWTask(self, node.item)
                bymapper[node.item] = task
            if node.circular:
                task.circular = task._sort_circular_dependencies(self)
                task.iscircular = True
            for child in node.children:
                t = sort_hier(child)
                if t is not None:
                    task.childtasks.append(t)
            return task
            
        mappers = util.HashSet()
        for task in self.tasks.values():
            mappers.append(task.mapper)
            bymapper[task.mapper] = task
    
        head = DependencySorter(self.dependencies, mappers).sort()
        task = sort_hier(head)
        return task


class UOWTaskElement(object):
    """an element within a UOWTask.  corresponds to a single object instance
    to be saved, deleted, or just part of the transaction as a placeholder for 
    further dependencies (i.e. 'listonly').
    in the case of self-referential mappers, may also store a "childtask", which is a
    UOWTask containing objects dependent on this element's object instance."""
    def __init__(self, obj):
        self.obj = obj
        self.listonly = True
        self.childtask = None
        self.isdelete = False
    def __repr__(self):
        return "UOWTaskElement/%d: %s/%d %s" % (id(self), self.obj.__class__.__name__, id(self.obj), (self.listonly and 'listonly' or (self.isdelete and 'delete' or 'save')) )

class UOWDependencyProcessor(object):
    """in between the saving and deleting of objects, process "dependent" data, such as filling in 
    a foreign key on a child item from a new primary key, or deleting association rows before a 
    delete."""
    def __init__(self, processor, targettask, isdeletefrom):
        self.processor = processor
        self.targettask = targettask
        self.isdeletefrom = isdeletefrom
    
    def execute(self, trans, delete):
        if not delete:
            self.processor.process_dependencies(self.targettask, [elem.obj for elem in self.targettask.tosave_elements()], trans, delete = delete)
        else:            
            self.processor.process_dependencies(self.targettask, [elem.obj for elem in self.targettask.todelete_elements()], trans, delete = delete)

    def get_object_dependencies(self, obj, trans, passive):
        return self.processor.get_object_dependencies(obj, trans, passive=passive)

    def whose_dependent_on_who(self, obj, o):        
        return self.processor.whose_dependent_on_who(obj, o)

    def branch(self, task):
        return UOWDependencyProcessor(self.processor, task, self.isdeletefrom)
    
class UOWTask(object):
    def __init__(self, uowtransaction, mapper):
        if uowtransaction is not None:
            uowtransaction.tasks[mapper] = self
        self.uowtransaction = uowtransaction
        self.mapper = mapper
        self.objects = util.OrderedDict()
        self.dependencies = []
        self.iscircular = False
        self.circular = None
        self.childtasks = []
        #print "NEW TASK", repr(self)
        
    def is_empty(self):
        return len(self.objects) == 0 and len(self.dependencies) == 0 and len(self.childtasks) == 0
            
    def append(self, obj, listonly = False, childtask = None, isdelete = False):
        """appends an object to this task, to be either saved or deleted depending on the
        'isdelete' attribute of this UOWTask.  'listonly' indicates that the object should
        only be processed as a dependency and not actually saved/deleted. if the object
        already exists with a 'listonly' flag of False, it is kept as is. 'childtask' is used
        internally when creating a hierarchical list of self-referential tasks, to assign
        dependent operations at the per-object instead of per-task level."""
        try:
            rec = self.objects[obj]
        except KeyError:
            rec = UOWTaskElement(obj)
            self.objects[obj] = rec
        if not listonly:
            rec.listonly = False
        if childtask:
            rec.childtask = childtask
        if isdelete:
            rec.isdelete = True

    def delete(self, obj):
        del self.objects[obj]
        
    def execute(self, trans):
        """executes this UOWTask.  saves objects to be saved, processes all dependencies
        that have been registered, and deletes objects to be deleted. """
        if self.circular is not None:
            self.circular.execute(trans)
            return

        self.mapper.save_obj(self.tosave_objects(), trans)
        for dep in self.save_dependencies():
            dep.execute(trans, delete=False)
        for element in self.tosave_elements():
            if element.childtask is not None:
                element.childtask.execute(trans)
        for dep in self.delete_dependencies():
            dep.execute(trans, delete=True)
        for child in self.childtasks:
            child.execute(trans)
        for element in self.todelete_elements():
            if element.childtask is not None:
                element.childtask.execute(trans)
        self.mapper.delete_obj(self.todelete_objects(), trans)

    def tosave_elements(self):
        return [rec for rec in self.objects.values() if not rec.isdelete]
    def todelete_elements(self):
        return [rec for rec in self.objects.values() if rec.isdelete]
    def tosave_objects(self):
        return [o for o, rec in self.objects.iteritems() if not rec.listonly and not rec.isdelete]
    def todelete_objects(self):
        return [o for o, rec in self.objects.iteritems() if not rec.listonly and rec.isdelete]
    def save_dependencies(self):
        return [dep for dep in self.dependencies if not dep.isdeletefrom]
    def delete_dependencies(self):
        return [dep for dep in self.dependencies if dep.isdeletefrom]
        
    def _sort_circular_dependencies(self, trans):
        """for a single task, creates a hierarchical tree of "subtasks" which associate
        specific dependency actions with individual objects.  This is used for a
        "circular" task, or a task where elements
        of its object list contain dependencies on each other.
        
        this is not the normal case; this logic only kicks in when something like 
        a hierarchical tree is being represented."""
        
        allobjects = self.objects.keys()
        tuples = []
        
        objecttotask = {}

        # dependency processors that arent part of the "circular" thing
        # get put here
        extradep = util.HashSet()

        def get_task(obj):
            try:
                return objecttotask[obj]
            except KeyError:
                t = UOWTask(None, self.mapper)
                objecttotask[obj] = t
                return t

        dependencies = {}
        def get_dependency_task(obj, depprocessor):
            try:
                dp = dependencies[obj]
            except KeyError:
                dp = {}
                dependencies[obj] = dp
            try:
                l = dp[depprocessor]
            except KeyError:
                l = UOWTask(None, None)
                dp[depprocessor] = l
            return l

        for taskelement in self.objects.values():
            # go through all of the dependencies on this task, which are
            # self-referring, and organize them
            # into a hash where we isolate individual objects that depend on each
            # other.  then those individual object relationships will be grabbed
            # back into a hierarchical tree thing down below via make_task_tree.
            obj = taskelement.obj
            parenttask = get_task(obj)
            for dep in self.dependencies:
                (processor, targettask, isdelete) = (dep.processor, dep.targettask, dep.isdeletefrom)
                if dep.targettask is not self:
                    extradep.append(dep)
                    continue
                elif taskelement.isdelete is not dep.isdeletefrom:
                    continue
                #print "GETING LIST OFF PROC", processor.key, "OBJ", repr(obj)
                childlist = dep.get_object_dependencies(obj, trans, passive = True)
                if isdelete:
                    childlist = childlist.unchanged_items() + childlist.deleted_items()
                else:
                    childlist = childlist.added_items()
                for o in childlist:
                    if not self.objects.has_key(o):
                        continue
                    whosdep = dep.whose_dependent_on_who(obj, o)
                    if whosdep is not None:
                        tuples.append(whosdep)
                        if whosdep[0] is obj:
                            get_dependency_task(whosdep[0], dep).append(whosdep[0], isdelete=isdelete)
                        else:
                            get_dependency_task(whosdep[0], dep).append(whosdep[1], isdelete=isdelete)
                    else:
                        get_dependency_task(obj, dep).append(obj, isdelete=isdelete)
                        
        head = DependencySorter(tuples, allobjects).sort()
        if head is None:
            return None

        def make_task_tree(node, parenttask):
            t = objecttotask[node.item]
            parenttask.append(node.item, self.objects[node.item].listonly, t, isdelete=self.objects[node.item].isdelete)
            if dependencies.has_key(node.item):
                for depprocessor, deptask in dependencies[node.item].iteritems():
                    parenttask.dependencies.append(depprocessor.branch(deptask))
            for n in node.children:
                t2 = make_task_tree(n, t)
            return t
        
        # this is the new "circular" UOWTask which will execute in place of "self"    
        t = UOWTask(None, self.mapper)

        # stick the non-circular dependencies and child tasks onto the new
        # circular UOWTask
        t.dependencies += [d for d in extradep]
        t.childtasks = self.childtasks
        make_task_tree(head, t)
        return t

    def dump(self):
        buf = StringIO.StringIO()
        self._dump(buf)
        return buf.getvalue()
        
    def _dump(self, buf, indent=0, circularparent=None):

        def _indent():
            return "  | " * indent
            
        def _dump_processor(proc):
            if proc.isdeletefrom:
                val = [t for t in proc.targettask.objects.values() if t.isdelete]
            else:
                val = [t for t in proc.targettask.objects.values() if not t.isdelete]

            buf.write(_indent() + "  |\n")
            buf.write(_indent() + "  |- UOWDependencyProcessor(%d) %s on %s %s\n" % (id(proc), repr(proc.processor.key), (proc.isdeletefrom and "to delete" or "saved"), _repr_task(proc.targettask)))
            if len(val) == 0:
                buf.write(_indent() + "  |       |-" + "(no objects)\n")
            for v in val:
                buf.write(_indent() + "  |       |-" + _repr_task_element(v) + "\n")
            buf.write(_indent() + "  |\n")
        
        def _repr_task_element(te):
            return "UOWTaskElement(%d): %s(%d) %s" % (id(te), te.obj.__class__.__name__, id(te.obj), (te.listonly and '(listonly)' or (te.isdelete and '(delete)' or '(save)')) )
                
        def _repr_task(task):
            if task.mapper is not None:
                if task.mapper.__class__.__name__ == 'Mapper':
                    name = task.mapper.class_.__name__ + "/" + task.mapper.primarytable.id
                else:
                    name = repr(task.mapper)
            else:
                name = '(none)'
            return ("UOWTask(%d) '%s'" % (id(task), name))


        def _repr(obj):
            return "%s(%d)" % (obj.__class__.__name__, id(obj))

        if self.circular is not None:
            self.circular._dump(buf, indent, self)
            return

        i = _indent()
        if len(i):
            i = i[0:-1] + "-"
        if circularparent is not None:
            buf.write(i + " " + _repr_task(circularparent))
            buf.write("->circular->" + _repr_task(self))
        else:
            buf.write(i + " " + _repr_task(self))
            
        buf.write("\n")
        for rec in self.tosave_elements():
            if rec.listonly:
                continue
            buf.write(_indent() + "  |- Save: " + _repr_task_element(rec) + "\n")
       
        for dep in self.save_dependencies():
            _dump_processor(dep)
        for element in self.tosave_elements():
            if element.childtask is not None:
                element.childtask._dump(buf, indent + 1)
        for dep in self.delete_dependencies():
            _dump_processor(dep)
        for child in self.childtasks:
            child._dump(buf, indent + 1)
        for element in self.todelete_elements():
            if element.childtask is not None:
                element.childtask._dump(buf, indent + 1)

        for rec in self.todelete_elements():
            if rec.listonly:
                continue
            buf.write(_indent() + "  |- Delete: " + _repr_task_element(rec) + "\n")

        buf.write(_indent() + "  |----\n")
        buf.write(_indent() + "\n")           
        
    def __repr__(self):
        if self.mapper is not None:
            if self.mapper.__class__.__name__ == 'Mapper':
                name = self.mapper.class_.__name__ + "/" + self.mapper.primarytable.name
            else:
                name = repr(self.mapper)
        else:
            name = '(none)'
        return ("UOWTask(%d) Mapper: '%s'" % (id(self), name))

class DependencySorter(topological.QueueDependencySorter):
    pass
        
def mapper(*args, **params):
    return sqlalchemy.mapperlib.mapper(*args, **params)

def object_mapper(obj):
    return sqlalchemy.mapperlib.object_mapper(obj)


global_attributes = UOWAttributeManager()
uow = util.ScopedRegistry(lambda: UnitOfWork(), "thread")
