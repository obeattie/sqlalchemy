<%inherit file="${context['mako_layout']}"/>

        <h1>${docstitle|h}</h1>
        <div id="pagecontrol">
            <a href="${pathto('modindex')}">Modules</a>
            |
            <a href="${pathto('genindex')}">Index</a>
        </div>
        <div class="versionheader">
            Version: <span class="versionnum">${release}</span> Last Updated: ${last_updated}
        </div>

        <div class="topnav">
            <div id="search">
            Search:
            <form class="search" action="${pathto('search')}" method="get">
              <input type="text" name="q" size="18" /> <input type="submit" value="${_('Go')}" />
              <input type="hidden" name="check_keywords" value="yes" />
              <input type="hidden" name="area" value="default" />
            </form>
            </div>
            
            <div class="navbanner">
                <a class="totoc" href="${pathto(master_doc)}">Table of Contents</a>
                ${prevnext()}
                % if title:
                <h2>${title}</h2>
                % endif
            </div>
            % if display_toc:
                ${toc}
            % endif
            <div class="clearboth"></div>
        </div>
        
        <div class="document">
            ${next.body()}
        </div>

        <%def name="footer()">
            <div class="bottomnav">
                ${prevnext()}
                % if hasdoc('copyright'):
                    &copy; <a href="${pathto('copyright')}">Copyright</a> ${copyright|h}.
                % else:
                    &copy; Copyright ${copyright|h}.
                % endif

                % if show_sphinx:
                    Created using <a href="http://sphinx.pocoo.org/">Sphinx</a> ${sphinx_version|h}.
                % endif
            </div>
        </%def>
        ${self.footer()}

<%def name="prevnext()">
<div class="prevnext">
    % if prevtopic:
        Previous:
        <a href="${prevtopic['link']|h}" title="${_('previous chapter')}">${prevtopic['title']}</a>
    % endif
    % if nexttopic:
        Next:
        <a href="${nexttopic['link']|h}" title="${_('next chapter')}">${nexttopic['title']}</a>
    % endif
</div>
</%def>

