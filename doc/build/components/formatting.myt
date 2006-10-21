<%doc>formatting.myt - library of HTML formatting functions to operate on a TOCElement tree</%doc>

<%global>
    import string, re
    import highlight
</%global>


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



<%method section>
<%args>
    toc
    path
</%args>
<%init>
    item = toc.get_by_path(path)
</%init>

<A name="<% item.path %>"></a>

<div class="subsection" style="margin-left:<% repr(item.depth * 10) %>px;">

<%python>
    content = m.content()
    re2 = re.compile(r"'''PYESC(.+?)PYESC'''", re.S)
    content = re2.sub(lambda m: m.group(1), content)
</%python>

% if item.depth > 1:
<h3><% item.description %></h3>
%

    <div class="sectiontext">
    <% content %>
</div>

% if item.depth > 1:
%   if (item.next and item.next.depth >= item.depth):
    <a href="#<% item.get_page_root().path %>" class="toclink">back to section top</a>
%
% else:
    <a href="#<% item.get_page_root().path %>" class="toclink">back to section top</a>
    <& nav.myt:pagenav, item=item &>
% 

</div>

</%method>


<%method formatplain>
    <%filter>
        import re
        f = re.sub(r'\n[\s\t]*\n[\s\t]*', '</p>\n<p>', f)
        f = "<p>" + f + "</p>"
        return f
    </%filter>
<% m.content() | h%>
</%method>



<%method paramtable>
    <table cellspacing="0" cellpadding="0" width="100%">
    <% m.content() %>
    </table>
</%method>

<%method member_doc>
       <%args>
               name = ""
               link = ""
               type = None
       </%args>
       <tr>
       <td>
           <div class="darkcell">
           <A name=""></a>
           <b><% name %></b>
           <div class="docstring"><% m.content() %></div>
           </div>
       </td>
       </tr>
</%method>



<%method codeline trim="both">
<span class="codeline"><% m.content() %></span>
</%method>

<%method code autoflush=False>
<%args>
    title = None
    syntaxtype = 'python'
    html_escape = False
    use_sliders = False
</%args>

<%init>
    def fix_indent(f):
        f =string.expandtabs(f, 4)
        g = ''
        lines = string.split(f, "\n")
        whitespace = None
        for line in lines:
            if whitespace is None:
                match = re.match(r"^([ ]*).+", line)
                if match is not None:
                    whitespace = match.group(1)

            if whitespace is not None:
                line = re.sub(r"^%s" % whitespace, "", line)

            if whitespace is not None or re.search(r"\w", line) is not None:
                g += (line + "\n")


        return g.rstrip()

    p = re.compile(r'<pre>(.*?)</pre>', re.S)
    def hlight(match):
        return "<pre>" + highlight.highlight(fix_indent(match.group(1)), html_escape = html_escape, syntaxtype = syntaxtype) + "</pre>"
    content = p.sub(hlight, "<pre>" + m.content() + "</pre>")
</%init>
<div class="<% use_sliders and "sliding_code" or "code" %>">
% if title is not None:
    <div class="codetitle"><% title %></div>
%
<% content %></div>
</%method>



<%method itemlink trim="both">
    <%args>
    item
    anchor=True
    </%args>
    <%args scope="request">
        extension='myt'
    </%args>
    <a href="<% item.get_link(extension=extension, anchor=anchor) %>"><% item.description %></a>
</%method>

<%method toclink trim="both">
    <%args>
        toc 
        path
        description=None
        extension
    </%args>
    <%init>
        item = toc.get_by_path(path)
        if description is None:
            if item:
                description = item.description
            else:
                description = path
    </%init>
% if item:
    <a href="<% item.get_link(extension=extension) %>"><% description %></a>
% else:
    <b><% description %></b>
%
</%method>


<%method link trim="both">
    <%args>
        href
        text
        class_
    </%args>
    <a href="<% href %>" <% class_ and (('class=\"%s\"' % class_) or '')%>><% text %></a>
</%method>

<%method popboxlink trim="both"> 
    <%args>
        name=None
        show='show'
        hide='hide'
    </%args>
    <%init>
        if name is None:
            name = m.attributes.setdefault('popbox_name', 0)
        name += 1
        m.attributes['popbox_name'] = name
        name = "popbox_" + repr(name)
    </%init>
javascript:togglePopbox('<% name %>', '<% show %>', '<% hide %>')
</%method>

<%method popbox trim="both">
<%args>
    name = None
    class_ = None
</%args>
<%init>
    if name is None:
        name = 'popbox_' + repr(m.attributes['popbox_name'])
</%init>
<div id="<% name %>_div" class="<% class_ %>" style="display:none;"><% m.content().strip() %></div>
</%method>

<%method poplink trim="both">
    <%args>
        link='sql'
    </%args>
    <%init>
        href = m.scomp('SELF:popboxlink')
    </%init>
    '''PYESC<& SELF:link, href=href, text=link, class_="codepoplink" &>PYESC'''
</%method>

<%method codepopper trim="both">
	<%init>
		c = m.content()
		c = re.sub(r'\n', '<br/>\n', c.strip())
	</%init>
    </pre><&|SELF:popbox, class_="codepop" &><% c %></&><pre>
</%method>

<%method poppedcode trim="both">
	<%init>
		c = m.content()
		c = re.sub(r'\n', '<br/>\n', c.strip())
	</%init>
    </pre><div class="codepop"><% c %></div><pre>
</%method>
