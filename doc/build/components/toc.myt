<%doc>toc.myt - prints full table of contents listings given toc.TOCElement strucures</%doc>

<%method toc>
	<%args>
		toc
		extension
	</%args>
	
	
	<div class="maintoc">

	<a name="table_of_contents"></a>
	<h2>Table of Contents</h2>
	&nbsp;&nbsp;
	<a href="#full_index">(view full table)</a>
	<br/><br/>
	
	<div style="margin-left:50px;">
	<& printtoc, root = toc, current = None, full = False, children=False, extension=extension, anchor_toplevel=False &>
	</div>

	</div>


	<div class="maintoc">
	<a name="full_index"></a>
	<h2>Table of Contents: Full</h2>
	&nbsp;&nbsp;
	<a href="#table_of_contents">(view brief table)</a>
	<br/><br/>

	<div style="margin-left:50px;">
	<& printtoc, root = toc, current = None, full = True, children=True, extension=extension, anchor_toplevel=False &>
	</div>

	</div>
</%method>


<%method printtoc>
<%args> 
    root
    current = None
    full = False
    children = True
    extension
    anchor_toplevel=False
</%args>

<ul class="toc_list">
% for i in root.children:
    <& printtocelement, item=i, bold = (i == current), full = full, children=children, extension=extension, anchor_toplevel=anchor_toplevel &>
%
</ul>
</%method>

<%def printtocelement>
<%doc>prints a TOCElement as a table of contents item and prints its immediate child items</%doc>
    <%args>
        item
        bold = False
        full = False
        children = True
        extension
        anchor_toplevel
    </%args>
    
        <li><A style="<% bold and "font-weight:bold;" or "" %>" href="<% item.get_link(extension=extension, anchor=anchor_toplevel) %>"><% item.description %></a></li>
    
% if children:  
    <ul class="small_toc_list">
%   for i in item.children:
        <& printsmtocelem, item=i, children=full, extension=extension &>
%
    </ul>
%
</%def>

<%def printsmtocelem>
    <%args>
        item
        children = False
        extension
    </%args>    
    <li><A href="<% item.get_link(extension=extension) %>"><% item.description %></a></li>

% if children:
    <ul class="small_toc_list">
%   for i in item.children:
        <& printsmtocelem, item = i, extension=extension &>
%
    </ul>
%

</%def>
