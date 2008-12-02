<%text>#coding:utf-8
<%inherit file="/base.html"/>
<%!
    in_docs=True
%>
</%text>

<%!
    import re
    def backslash_to_text(t):
        return re.sub(r'\\', '<%text>\</%text>', t)
%>

<div style="text-align:right">
<b>Quick Select:</b> <a href="/docs/05/">0.5</a> | <a href="/docs/04/">0.4</a> | <a href="/docs/03/">0.3</a>
</div>

${capture(next.body) | backslash_to_text}

<%text><%def name="style()"></%text>
        <link rel="stylesheet" href="${pathto('_static/docs.css', 1)}" type="text/css" />
        <link rel="stylesheet" href="${pathto('_static/pygments.css', 1)}" type="text/css" />

        <script type="text/javascript">
          var DOCUMENTATION_OPTIONS = {
              URL_ROOT:    '${pathto("", 1)}',
              VERSION:     '${release|h}',
              COLLAPSE_MODINDEX: false,
              FILE_SUFFIX: '${file_suffix}'
          };
        </script>
        % for scriptfile in script_files + self.attr.local_script_files:
            <script type="text/javascript" src="${pathto(scriptfile, 1)}"></script>
        % endfor
        <script type="text/javascript" src="${pathto('_static/init.js', 1)}"></script>
        % if hasdoc('about'):
            <link rel="author" title="${_('About these documents')}" href="${pathto('about')}" />
        % endif
        <link rel="index" title="${_('Index')}" href="${pathto('genindex')}" />
        <link rel="search" title="${_('Search')}" href="${pathto('search')}" />
        % if hasdoc('copyright'):
            <link rel="copyright" title="${_('Copyright')}" href="${pathto('copyright')}" />
        % endif
        <link rel="top" title="${docstitle|h}" href="${pathto('index')}" />
        % if parents:
            <link rel="up" title="${parents[-1]['title']|util.striptags}" href="${parents[-1]['link']|h}" />
        % endif
        % if nexttopic:
            <link rel="next" title="${nexttopic['title']|util.striptags}" href="${nexttopic['link']|h}" />
        % endif
        % if prevtopic:
            <link rel="prev" title="${prevtopic['title']|util.striptags}" href="${prevtopic['link']|h}" />
        % endif
    <%def name="extrahead()"></%def>
    ${self.extrahead()}
    <%text>${parent.style()}</%text>
    <link href="/css/site_docs.css" rel="stylesheet" type="text/css"></link>
    
<%text></%def></%text>

<%def name="show_title()">${title}</%def>

<%!
    local_script_files = []
%>
