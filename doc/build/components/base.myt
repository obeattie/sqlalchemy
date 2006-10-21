<%args>
    extension="myt"
    toc = None
</%args>
<%init>
    import cPickle as pickle
    import os, time
    title = m.request_component.attributes['title']
    if toc is None:
        filename = os.path.join(os.path.dirname(m.request_component.file), 'table_of_contents.pickle')
        toc = pickle.load(file(filename))
    version = toc.version
    last_updated = toc.last_updated
    current = toc.get_by_file(m.request_component.attributes['filename'])
</%init>

<link href="style.css" rel="stylesheet" type="text/css"></link>
<link href="syntaxhighlight.css" rel="stylesheet" type="text/css"></link>

<link href="docs.css" rel="stylesheet" type="text/css"></link>
<script src="scripts.js"></script>


<div style="position:absolute;left:0px;top:0px;"><a name="top"></a>&nbsp;</div>

<div class="doccontainer">

<div class="docheader">

<h1><% title %></h1>
<div class="">Version: <% version %>   Last Updated: <% time.strftime('%x %X', time.localtime(last_updated)) %></div>
</div>


<A name="<% current.path %>"></a>
<& nav.myt:topnav, item=current, extension=extension &>
<div class="sectioncontent">
% m.call_next(toc=toc, extension=extension)
</div>


</div>


