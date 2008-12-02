<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
  "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">

<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
        ${metatags and metatags or ''}
        <title>${capture(self.show_title)|util.striptags} &mdash; ${docstitle|h}</title>
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

    </head>
    <body>
        ${next.body()}
    </body>
</html>

<%def name="show_title()">${title}</%def>

<%!
    local_script_files = []
%>
