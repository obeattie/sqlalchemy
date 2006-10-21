<%method topnav>
	<%args>
		item
		extension
	</%args>
<div class="topnav">

<div class="topnavsectionlink">

<a href="index.<% extension %>">Table of Contents</a>

<div class="prevnext">
% if item.previous is not None:
Previous: <& formatting.myt:itemlink, item=item.previous, anchor=False &>
%

% if item.previous is not None and item.next is not None:
&nbsp; | &nbsp;
%

% if item.next is not None:

Next: <& formatting.myt:itemlink, item=item.next, anchor=False &>
%

</div>
</div>

<div class="topnavmain">
	<div class="topnavheader"><% item.description %></div>
	<div class="topnavitems">
	<& formatting.myt:printtoc, root=item, current=None, full=True, extension=extension, anchor_toplevel=True &>
	</div>
</div>

</div>
</%method>

<%method pagenav>
<%args>
    item
</%args>
<div class="sectionnavblock">
<div class="sectionnav">

%       if item.previous is not None:
        Previous: <& formatting.myt:itemlink, item=item.previous &>
%       # end if

%       if item.next is not None:
%               if item.previous is not None:
                |
%               # end if

        Next: <& formatting.myt:itemlink, item=item.next &>
%       # end if

</div>
</div>
</%method>
