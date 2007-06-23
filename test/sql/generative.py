import testbase
from sqlalchemy import *

class TraversalTest(testbase.AssertMixin):
    """test ClauseVisitor's traversal, particularly its ability to copy and modify
    a ClauseElement in place."""
    
    def setUpAll(self):
        global A, B
        
        # establish two ficticious ClauseElements.
        # define deep equality semantics as well as deep identity semantics.
        class A(ClauseElement):
            def __init__(self, expr):
                self.expr = expr

            def accept_visitor(self, visitor):
                visitor.visit_a(self)

            def is_other(self, other):
                return other is self
            
            def __eq__(self, other):
                return other.expr == self.expr
            
            def __ne__(self, other):
                return other.expr != self.expr
                
            def __str__(self):
                return "A(%s)" % repr(self.expr)
                
        class B(ClauseElement):
            def __init__(self, *items):
                self.items = items

            def is_other(self, other):
                if other is not self:
                    return False
                for i1, i2 in zip(self.items, other.items):
                    if i1 is not i2:
                        return False
                return True

            def __eq__(self, other):
                for i1, i2 in zip(self.items, other.items):
                    if i1 != i2:
                        return False
                return True
            
            def __ne__(self, other):
                for i1, i2 in zip(self.items, other.items):
                    if i1 != i2:
                        return True
                return False
                
            def get_children(self, clone=False, **kwargs):
                if clone:
                    self.items = [i._clone() for i in self.items]
                return self.items
            
            def accept_visitor(self, visitor):
                visitor.visit_b(self)
                
            def __str__(self):
                return "B(%s)" % repr([str(i) for i in self.items])
    
    def test_test_classes(self):
        a1 = A("expr1")
        struct = B(a1, A("expr2"), B(A("expr1b"), A("expr2b")), A("expr3"))
        struct2 = B(a1, A("expr2"), B(A("expr1b"), A("expr2b")), A("expr3"))
        struct3 = B(a1, A("expr2"), B(A("expr1b"), A("expr2bmodified")), A("expr3"))

        assert a1 is a1
        assert struct is struct
        assert struct == struct2
        assert struct != struct3
        assert struct is not struct2
        assert struct is not struct3
        
    def test_clone(self):    
        struct = B(A("expr1"), A("expr2"), B(A("expr1b"), A("expr2b")), A("expr3"))
        
        class Vis(ClauseVisitor):
            def visit_a(self, a):
                pass
            def visit_b(self, b):
                pass
                
        vis = Vis()
        s2 = vis.traverse(struct, clone=True)
        assert struct == s2
        assert struct is not s2

    def test_no_clone(self):    
        struct = B(A("expr1"), A("expr2"), B(A("expr1b"), A("expr2b")), A("expr3"))

        class Vis(ClauseVisitor):
            def visit_a(self, a):
                pass
            def visit_b(self, b):
                pass

        vis = Vis()
        s2 = vis.traverse(struct, clone=False)
        assert struct == s2
        assert struct is s2
        
    def test_change_in_place(self):
        struct = B(A("expr1"), A("expr2"), B(A("expr1b"), A("expr2b")), A("expr3"))
        struct2 = B(A("expr1"), A("expr2modified"), B(A("expr1b"), A("expr2b")), A("expr3"))
        struct3 = B(A("expr1"), A("expr2"), B(A("expr1b"), A("expr2bmodified")), A("expr3"))

        class Vis(ClauseVisitor):
            def visit_a(self, a):
                if a.expr == "expr2":
                    a.expr = "expr2modified"
            def visit_b(self, b):
                pass

        vis = Vis()
        s2 = vis.traverse(struct, clone=True)
        assert struct != s2
        assert struct is not s2
        assert struct2 == s2

        class Vis2(ClauseVisitor):
            def visit_a(self, a):
                if a.expr == "expr2b":
                    a.expr = "expr2bmodified"
            def visit_b(self, b):
                pass

        vis2 = Vis2()
        s3 = vis2.traverse(struct, clone=True)
        assert struct != s3
        assert struct3 == s3
        
if __name__ == '__main__':
    testbase.main()        