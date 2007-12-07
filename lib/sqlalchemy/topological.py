# topological.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Topological sorting algorithms.

The topological sort is an algorithm that receives this list of
dependencies as a *partial ordering*, that is a list of pairs which
might say, *X is dependent on Y*, *Q is dependent on Z*, but does not
necessarily tell you anything about Q being dependent on X. Therefore,
its not a straight sort where every element can be compared to
another... only some of the elements have any sorting preference, and
then only towards just some of the other elements.  For a particular
partial ordering, there can be many possible sorts that satisfy the
conditions.

"""

from sqlalchemy import util
from sqlalchemy.exceptions import CircularDependencyError

class _Node(object):
    """Represent each item in the sort.

    While the topological sort produces a straight ordered list of
    items, ``_Node`` ultimately stores a tree-structure of those items
    which are organized so that non-dependent nodes are siblings.
    """

    def __init__(self, item):
        self.item = item
        self.dependencies = util.Set()
        self.children = []
        self.cycles = None

    def __str__(self):
        return self.safestr()

    def safestr(self, indent=0):
        return (' ' * indent * 2) + \
            str(self.item) + \
            (self.cycles is not None and (" (cycles: " + repr([x for x in self.cycles]) + ")") or "") + \
            "\n" + \
            ''.join([n.safestr(indent + 1) for n in self.children])

    def __repr__(self):
        return "%s" % (str(self.item))

    def all_deps(self):
        """Return a set of dependencies for this node and all its cycles."""

        deps = util.Set(self.dependencies)
        if self.cycles is not None:
            for c in self.cycles:
                deps.update(c.dependencies)
        return deps

class _EdgeCollection(object):
    """A collection of directed edges."""

    def __init__(self):
        self.parent_to_children = {}
        self.child_to_parents = {}

    def add(self, edge):
        """Add an edge to this collection."""

        (parentnode, childnode) = edge
        if parentnode not in self.parent_to_children:
            self.parent_to_children[parentnode] = util.Set()
        self.parent_to_children[parentnode].add(childnode)
        if childnode not in self.child_to_parents:
            self.child_to_parents[childnode] = util.Set()
        self.child_to_parents[childnode].add(parentnode)
        parentnode.dependencies.add(childnode)

    def remove(self, edge):
        """Remove an edge from this collection.

        Return the childnode if it has no other parents.
        """

        (parentnode, childnode) = edge
        self.parent_to_children[parentnode].remove(childnode)
        self.child_to_parents[childnode].remove(parentnode)
        if len(self.child_to_parents[childnode]) == 0:
            return childnode
        else:
            return None

    def has_parents(self, node):
        return node in self.child_to_parents and len(self.child_to_parents[node]) > 0

    def edges_by_parent(self, node):
        if node in self.parent_to_children:
            return [(node, child) for child in self.parent_to_children[node]]
        else:
            return []

    def get_parents(self):
        return self.parent_to_children.keys()

    def pop_node(self, node):
        """Remove all edges where the given node is a parent.

        Return the collection of all nodes which were children of the
        given node, and have no further parents.
        """

        children = self.parent_to_children.pop(node, None)
        if children is not None:
            for child in children:
                self.child_to_parents[child].remove(node)
                if not self.child_to_parents[child]:
                    yield child

    def __len__(self):
        return sum([len(x) for x in self.parent_to_children.values()])

    def __iter__(self):
        for parent, children in self.parent_to_children.iteritems():
            for child in children:
                yield (parent, child)

    def __str__(self):
        return repr(list(self))

    def __repr__(self):
        return repr(list(self))

class QueueDependencySorter(object):
    """Topological sort adapted from wikipedia's article on the subject.

    It creates a straight-line list of elements, then a second pass
    batches non-dependent elements as siblings in a tree structure.  Future
    versions of this algorithm may separate the "convert to a tree" 
    step.
    """

    def __init__(self, tuples, allitems):
        self.tuples = tuples
        self.allitems = allitems

    def sort(self, allow_cycles=False, ignore_self_cycles=False, create_tree=True):
        (tuples, allitems) = (self.tuples, self.allitems)
        #print "\n---------------------------------\n"
        #print repr([t for t in tuples])
        #print repr([a for a in allitems])
        #print "\n---------------------------------\n"

        nodes = {}
        edges = _EdgeCollection()
        for item in list(allitems) + [t[0] for t in tuples] + [t[1] for t in tuples]:
            if id(item) not in nodes:
                node = _Node(item)
                nodes[id(item)] = node

        for t in tuples:
            if t[0] is t[1]:
                if allow_cycles:
                    n = nodes[id(t[0])]
                    n.cycles = util.Set([n])
                elif not ignore_self_cycles:
                    raise CircularDependencyError("Self-referential dependency detected " + repr(t))
                continue
            childnode = nodes[id(t[1])]
            parentnode = nodes[id(t[0])]
            edges.add((parentnode, childnode))

        queue = []
        for n in nodes.values():
            if not edges.has_parents(n):
                queue.append(n)

        output = []
        while nodes:
            if not queue:
                # edges remain but no edgeless nodes to remove; this indicates
                # a cycle
                if allow_cycles:
                    for cycle in self._find_cycles(edges):
                        lead = cycle[0][0]
                        lead.cycles = util.Set()
                        for edge in cycle:
                            n = edges.remove(edge)
                            lead.cycles.add(edge[0])
                            lead.cycles.add(edge[1])
                            if n is not None:
                                queue.append(n)
                        for n in lead.cycles:
                            if n is not lead:
                                n._cyclical = True
                                for (n,k) in list(edges.edges_by_parent(n)):
                                    edges.add((lead, k))
                                    edges.remove((n,k))
                    continue
                else:
                    # long cycles not allowed
                    raise CircularDependencyError("Circular dependency detected " + repr(edges) + repr(queue))
            node = queue.pop()
            if not hasattr(node, '_cyclical'):
                output.append(node)
            del nodes[id(node.item)]
            for childnode in edges.pop_node(node):
                queue.append(childnode)
        if create_tree:
            return self._create_batched_tree(output)
        elif allow_cycles:
            return output
        else:
            return [n.item for n in output]


    def _create_batched_tree(self, nodes):
        """Given a list of nodes from a topological sort, organize the
        nodes into a tree structure, with as many non-dependent nodes
        set as siblings to each other as possible.
        """

        if not nodes:
            return None
        # a list of all currently independent subtrees as a tuple of
        # (root_node, set_of_all_tree_nodes, set_of_all_cycle_nodes_in_tree)
        # order of the list has no semantics for the algorithmic
        independents = []
        # in reverse topological order
        for node in util.reversed(nodes):
            # nodes subtree and cycles contain the node itself
            subtree = util.Set([node])
            if node.cycles is not None:
                cycles = util.Set(node.cycles)
            else:
                cycles = util.Set()
            # get a set of dependent nodes of node and its cycles
            nodealldeps = node.all_deps()
            if nodealldeps:
                # iterate over independent node indexes in reverse order so we can efficiently remove them
                for index in xrange(len(independents)-1,-1,-1):
                    child, childsubtree, childcycles = independents[index]
                    # if there is a dependency between this node and an independent node
                    if (childsubtree.intersection(nodealldeps) or childcycles.intersection(node.dependencies)):
                        # prepend child to nodes children
                        # (append should be fine, but previous implemetation used prepend)
                        node.children[0:0] = (child,)
                        # merge childs subtree and cycles
                        subtree.update(childsubtree)
                        cycles.update(childcycles)
                        # remove the child from list of independent subtrees
                        independents[index:index+1] = []
            # add node as a new independent subtree
            independents.append((node,subtree,cycles))
        # choose an arbitrary node from list of all independent subtrees
        head = independents.pop()[0]
        # add all other independent subtrees as a child of the chosen root
        # used prepend [0:0] instead of extend to maintain exact behaviour of previous implementation
        head.children[0:0] = [i[0] for i in independents]
        return head

    def _find_cycles(self, edges):
        involved_in_cycles = util.Set()
        cycles = {}
        def traverse(node, goal=None, cycle=None):
            if goal is None:
                goal = node
                cycle = []
            elif node is goal:
                return True

            for (n, key) in edges.edges_by_parent(node):
                if key in cycle:
                    continue
                cycle.append(key)
                if traverse(key, goal, cycle):
                    cycset = util.Set(cycle)
                    for x in cycle:
                        involved_in_cycles.add(x)
                        if x in cycles:
                            existing_set = cycles[x]
                            [existing_set.add(y) for y in cycset]
                            for y in existing_set:
                                cycles[y] = existing_set
                            cycset = existing_set
                        else:
                            cycles[x] = cycset
                cycle.pop()

        for parent in edges.get_parents():
            traverse(parent)

        # sets are not hashable, so uniquify with id
        unique_cycles = dict([(id(s), s) for s in cycles.values()]).values()
        for cycle in unique_cycles:
            edgecollection = [edge for edge in edges
                              if edge[0] in cycle and edge[1] in cycle]
            yield edgecollection
