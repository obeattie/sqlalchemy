<%inherit file="layout.mako"/>

<%!
    local_script_files = ['_static/searchtools.js']
%>
<%def name="show_title()">${_('Search')}</%def>

<h1 id="search-documentation">${_('Search')}</h1>
<p>
From here you can search these documents. Enter your search
words into the box below and click "search". Note that the search
function will automatically search for all of the words. Pages
containing fewer words won't appear in the result list.
</p>
<form action="" method="get">
<input type="text" name="q" value="" />
<input type="submit" value="${_('search')}" />
<span id="search-progress" style="padding-left: 10px"></span>
</form>
% if search_performed:
<h2>${_('Search Results')}</h2>
% if not search_results:
  <p>${_('Your search did not match any results.')}</p>
% endif
% endif
<div id="search-results">
% if search_results:
<ul>
% for href, caption, context in search_results:
  <li><a href="${pathto(item.href)}">${caption}</a>
    <div class="context">${context|h}</div>
  </li>
% endfor
</ul>
% endif
</div>

<%def name="footer()">
    ${parent.footer()}
    <script type="text/javascript" src="searchindex.js"></script>
</%def>
