<%flags>inherit='base.myt'</%flags>
<%attr>
    title='Modules and Classes'
    filename='docstrings'
</%attr>

<%init>
    import cPickle as pickle
    import os
    filename = os.path.join(os.path.dirname(self.file), 'compiled_docstrings.pickle')
    data = pickle.load(file(filename))
</%init>

% for obj in data:
<& pydoc.myt:obj_doc, obj=obj &>
%

