#!/usr/bin/env python
import sys,re,os
import myghty.interp
import myghty.exception as exception
import cPickle as pickle

sys.path = ['../../lib', './lib/'] + sys.path

import gen_docstrings, read_markdown, toc

files = [
    'tutorial',
    'dbengine',
    'metadata',
    'sqlconstruction',
    'datamapping',
    'unitofwork',
    'adv_datamapping',
    'types',
    'pooling',
    'plugins',
    'docstrings',
    ]

title='SQLAlchemy 0.3 Documentation'
version = '0.3.0'

root = toc.TOCElement('', 'root', '', version=version, doctitle=title)

docstrings = gen_docstrings.make_all_docs()
pickle.dump(docstrings, file(os.path.join(os.getcwd(), 'content', "compiled_docstrings.pickle"), 'w'))
gen_docstrings.create_docstring_toc(docstrings, root)

read_markdown.parse_markdown_files(root, files)
pickle.dump(root, file(os.path.join(os.getcwd(), 'content', "table_of_contents.pickle"), 'w'))

component_root = [
    {'components': './components'},
    {'content' : './content'},
    {'output' :'./output'}
]
output = os.path.dirname(os.getcwd())

interp = myghty.interp.Interpreter(component_root = component_root, output_encoding='utf-8')

def genfile(name, toc):
    infile = name + ".myt"
    outname = os.path.join(os.getcwd(), '../', name + ".html")
    outfile = file(outname, 'w')
    print infile, '->', outname
    interp.execute(infile, out_buffer=outfile, request_args={'toc':toc,'extension':'html'}, raise_error=True)
    
try:
    for filename in files:
        genfile(filename, root)
except exception.Error, e:
    sys.stderr.write(e.textformat())


        


