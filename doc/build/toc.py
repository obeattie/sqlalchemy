
toc_by_file = {}
toc_by_path = {}

class TOCElement(object):
    def __init__(self, filename, name, description, parent=None, **kwargs):
        self.filename = filename
        self.name = name
        self.parent = parent
        self.content = None
        self.path = self._create_path()
        print "NEW TOC:", self.path
        for key, value in kwargs.iteritems():
            setattr(self, key, value)

        toc_by_path[self.path] = self
            
        self.is_top = (self.parent is not None and self.parent.filename != self.filename) or self.parent is None
        if self.is_top:
            toc_by_file[self.filename] = self

        self.content = None
        self.previous = None
        self.next = None
        self.children = []
        if parent:
            if len(parent.children):
                self.previous = parent.children[-1]
                parent.children[-1].next = self
            parent.children.append(self)

    def _create_path(self):
        elem = self
        tokens = []
        while elem.parent is not None:
            tokens.insert(0, elem.name)
            elem = elem.parent
        return '_'.join(tokens)
