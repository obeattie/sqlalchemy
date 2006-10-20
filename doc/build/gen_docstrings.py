import myghty.interp
import myghty.exception as exception
from toc import TOCElement
import cPickle as pickle
import docstring, sys

component_root = [
    {'components': './components'},
]

def create_docstring_toc(filename, root):
    data = pickle.load(file(filename))

    def create_obj_toc(obj, toc):
        if obj.isclass:
            s = []
            links = []
            for elem in obj.inherits:
                if isinstance(elem, docstring.ObjectDoc):
                    links.append("<a href=\"#%s\">%s</a>" % (str(elem.id), elem.name))
                    s.append(elem.name)
                else:
                    links.append(str(elem))
                    s.append(str(elem))
            description = "class " + obj.classname + "(%s)" % (','.join(s))
            htmldescription = "class " + obj.classname + "(%s)" % (','.join(links))
        else:
            description = obj.description
            htmldescription = obj.description

        toc = TOCElement(filename, obj.name, description, toc)

        if not obj.isclass and obj.functions:
            TOCElement(filename, name="modfunc", description="Module Functions", parent=toc)

        if obj.classes:
            for class_ in obj.classes:
                create_obj_toc(class_, toc)
                
    for obj in data:
        create_obj_toc(obj, root)
    return data

def gen(data, toc):
    interp = myghty.interp.Interpreter( component_root = component_root)
    comp = interp.make_component("""
<%args>
    data
</%args>
% for obj in data:
<& py_doc.myt:obj_doc, obj=obj &>
%
""")
    try:
        interp.execute(comp, out_buffer = sys.stdout, request_args = {'data':data}, raise_error = True)
    except exception.Error, e:
        sys.stderr.write(e.textformat())
