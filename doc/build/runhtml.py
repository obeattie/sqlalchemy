#!/usr/bin/env python
import sys,re,os

component_root = [
    {'components': './components'},
    {'content' : './content'}
]
doccomp = ['document_base.myt']
output = os.path.dirname(os.getcwd())

sys.path = ['./lib/'] + sys.path

import myghty.http.HTTPServerHandler as HTTPServerHandler

port = 8080
httpd = HTTPServerHandler.HTTPServer(
    port = port,
    output_encoding='utf-8',
    handlers = [
        {'.*(?:\.myt|/$)' : HTTPServerHandler.HSHandler(path_translate=[(r'^/$', r'/index.myt')], data_dir = './cache', component_root = component_root)},
    ],

    docroot = [{'.*' : '../'}],
    
)       

print "Listening on %d" % port        
httpd.serve_forever()
