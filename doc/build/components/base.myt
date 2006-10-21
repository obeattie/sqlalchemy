<%args>
    extension="myt"
</%args>
<%global>
    import cPickle as pickle
    import os, time
    toc = request.instance().request_args.get('toc')
    if toc is None:
        filename = os.path.join(os.path.dirname(request.instance().request_component.file), 'table_of_contents.pickle')
        toc = pickle.load(file(filename))
    version = toc.version
    last_updated = toc.last_updated
</%global>

<%method title>
    <% toc.root.doctitle %> 
% t = m.request_component.attributes.get('title')
% if t:
    - <% t %>
%
</%method>

<div style="position:absolute;left:0px;top:0px;"><a name="top"></a>&nbsp;</div>

<div class="doccontainer">

<div class="docheader">

<h1><% toc.root.doctitle %></h1>
<div class="">Version: <% version %>   Last Updated: <% time.strftime('%x %X', time.localtime(last_updated)) %></div>
</div>

% m.call_next(toc=toc, extension=extension)

</div>


