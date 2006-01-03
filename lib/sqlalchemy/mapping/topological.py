# topological.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""topological sorting algorithms.  the key to the unit of work is to assemble a list
of dependencies amongst all the different mappers that have been defined for classes.
Related tables with foreign key constraints have a definite insert order, deletion order, 
objects need dependent properties from parent objects set up before saved, etc.  
These are all encoded as dependencies, in the form "mapper X is dependent on mapper Y", 
meaning mapper Y's objects must be saved before those of mapper X, and mapper X's objects 
must be deleted before those of mapper Y.

The topological sort is an algorithm that receives this list of dependencies as a "partial
ordering", that is a list of pairs which might say, "X is dependent on Y", "Q is dependent
on Z", but does not necessarily tell you anything about Q being dependent on X. Therefore,
its not a straight sort where every element can be compared to another...only some of the
elements have any sorting preference, and then only towards just some of the other elements.
For a particular partial ordering, there can be many possible sorts that satisfy the
conditions.

An intrinsic "gotcha" to this algorithm is that since there are many possible outcomes
to sorting a partial ordering, the algorithm can return any number of different results for the
same input; just running it on a different machine architecture, or just random differences
in the ordering of dictionaries, can change the result that is returned.  While this result
is guaranteed to be true to the incoming partial ordering, if the partial ordering itself
does not properly represent the dependencies, code that works fine will suddenly break, then
work again, then break, etc.   Most of the bugs I've chased down while developing the "unit of work"
have been of this nature - very tricky to reproduce and track down, particularly before I 
realized this characteristic of the algorithm.
"""
import string

class QueueDependencySorter(object):
    """this is a topological sort from wikipedia.  its very stable.  it creates a straight-line
    list of elements, then a second pass groups non-dependent actions together to build
    more of a tree structure with siblings."""
    class Node:
        """represents a node in a tree.  stores an 'item' which represents the 
        dependent thing we are talking about.  if node 'a' is an ancestor node of 
        node 'b', it means 'a's item is *not* dependent on that of 'b'."""
        def __init__(self, item):
            self.item = item
            self.circular = False
            self.edges = {}
            self.dependencies = {}
            self.children = []
        def __str__(self):
            return self.safestr()
        def safestr(self, indent=0):
            return (' ' * indent) + "%s  (idself=%s)" % (str(self.item), repr(id(self))) + "\n" + string.join([n.safestr(indent + 1) for n in self.children], '')
        def describe(self):
            return "%s  (idself=%s)" % (str(self.item), repr(id(self)))
        def __repr__(self):
            return self.describe()
            
    def __init__(self, tuples, allitems):
        self.tuples = tuples
        self.allitems = allitems
        
    def sort(self):
        (tuples, allitems) = (self.tuples, self.allitems)

        #print "\n---------------------------------\n"        
        #print repr([t for t in tuples])
        #print repr([a for a in allitems])
        #print "\n---------------------------------\n"        

        nodes = {}
        edges = {}
        for item in allitems + [t[0] for t in tuples] + [t[1] for t in tuples]:
            if not nodes.has_key(item):
                node = QueueDependencySorter.Node(item)
                nodes[item] = node
                edges[node] = {}
        
        for t in tuples:
            if t[0] is t[1]:
                nodes[t[0]].circular = True
                continue
            childnode = nodes[t[1]]
            parentnode = nodes[t[0]]
            edges[parentnode][childnode] = True
            parentnode.dependencies[childnode] = True
            childnode.edges[parentnode] = True

        queue = []
        for n in nodes.values():
            if len(n.edges) == 0:
                queue.append(n)
        
        output = []
        while len(edges) > 0:
            if len(queue) == 0:
                raise "Circular dependency detected " + repr(edges) + repr(queue)
            node = queue.pop()
            output.append(node)
            nodeedges = edges.pop(node, None)
            if nodeedges is None:
                continue
            for childnode in nodeedges.keys():
                del childnode.edges[node]
                if len(childnode.edges) == 0:
                    queue.append(childnode)

            
        #print repr(output)
        head = None
        node = None
        for o in output:
            if head is None:
                head = o
                node = o
            else:
                for x in node.children:
                    if x.dependencies.has_key(o):
                        node = x
                node.children.append(o)
        return head


class TreeDependencySorter(object):
    """
    this is my first topological sorting algorithm.  its crazy, but matched my thinking
    at the time.  it also creates the kind of structure I want.  but, I am not 100% sure
    it works in all cases since I always did really poorly in linear algebra.  anyway,
    I got the other one above to produce a tree structure too so we should be OK.
    """
    class Node:
        """represents a node in a tree.  stores an 'item' which represents the 
        dependent thing we are talking about.  if node 'a' is an ancestor node of 
        node 'b', it means 'a's item is *not* dependent on that of 'b'."""
        def __init__(self, item):
            #print "new node on " + str(item)
            self.item = item
            self.children = HashSet()
            self.parent = None
            self.circular = False
        def append(self, node):
            """appends the given node as a child on this node.  removes the node from 
            its preexisting parent."""
            if node.parent is not None:
                del node.parent.children[node]
            self.children.append(node)
            node.parent = self
        def is_descendant_of(self, node):
            """returns true if this node is a descendant of the given node"""
            n = self
            while n is not None:
                if n is node:
                    return True
                else:
                    n = n.parent
            return False
        def get_root(self):
            """returns the highest ancestor node of this node, i.e. which has no parent"""
            n = self
            while n.parent is not None:
                n = n.parent
            return n
        def get_sibling_ancestor(self, node):
            """returns the node which is:
            - an ancestor of this node 
            - is a sibling of the given node 
            - not an ancestor of the given node
            
            - else returns this node's root node."""
            n = self
            while n.parent is not None and n.parent is not node.parent and not node.is_descendant_of(n.parent):
                n = n.parent
            return n
        def __str__(self):
            return self.safestr({})
        def safestr(self, hash, indent = 0):
            if hash.has_key(self):
                return (' ' * indent) + "RECURSIVE:%s(%s, %s)" % (str(self.item), repr(id(self)), self.parent and repr(id(self.parent)) or 'None')
            hash[self] = True
            return (' ' * indent) + "%s  (idself=%s, idparent=%s)" % (str(self.item), repr(id(self)), self.parent and repr(id(self.parent)) or "None") + "\n" + string.join([n.safestr(hash, indent + 1) for n in self.children], '')
        def describe(self):
            return "%s  (idself=%s)" % (str(self.item), repr(id(self)))
            
    def __init__(self, tuples, allitems):
        self.tuples = tuples
        self.allitems = allitems
        
    def sort(self):
        (tuples, allitems) = (self.tuples, self.allitems)
        
        nodes = {}
        # make nodes for all the items and store in the hash
        for item in allitems + [t[0] for t in tuples] + [t[1] for t in tuples]:
            if not nodes.has_key(item):
                nodes[item] = TreeDependencySorter.Node(item)

        # loop through tuples
        for tup in tuples:
            (parent, child) = (tup[0], tup[1])
            # get parent node
            parentnode = nodes[parent]

            # if parent is child, mark "circular" attribute on the node
            if parent is child:
                parentnode.circular = True
                # and just continue
                continue

            # get child node
            childnode = nodes[child]
            
            if parentnode.parent is childnode:
                # check for "a switch"
                t = parentnode.item
                parentnode.item = childnode.item
                childnode.item = t
                nodes[parentnode.item] = parentnode
                nodes[childnode.item] = childnode
            elif parentnode.is_descendant_of(childnode):
                # check for a line thats backwards with nodes in between, this is a 
                # circular dependency (although confirmation on this would be helpful)
                raise "Circular dependency detected"
            elif not childnode.is_descendant_of(parentnode):
                # if relationship doesnt exist, connect nodes together
                root = childnode.get_sibling_ancestor(parentnode)
                parentnode.append(root)

            
        # now we have a collection of subtrees which represent dependencies.
        # go through the collection root nodes wire them together into one tree        
        head = None
        for node in nodes.values():
            if node.parent is None:
                if head is not None:
                    head.append(node)
                else:
                    head = node
        #print str(head)
        return head
                        