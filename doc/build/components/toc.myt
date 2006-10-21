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
	<& formatting.myt:printtoc, root = toc, current = None, full = False, children=False, extension=extension, anchor_toplevel=False &>
	</div>

	</div>


	<div class="maintoc">
	<a name="full_index"></a>
	<h2>Table of Contents: Full</h2>
	&nbsp;&nbsp;
	<a href="#table_of_contents">(view brief table)</a>
	<br/><br/>

	<div style="margin-left:50px;">
	<& formatting.myt:printtoc, root = toc, current = None, full = True, children=True, extension=extension, anchor_toplevel=False &>
	</div>

	</div>
</%method>
