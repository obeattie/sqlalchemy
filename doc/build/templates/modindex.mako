<%inherit file="layout.mako"/>

<%def name="show_title()">${_('Global Module Index')}</%def>

<%def name="extrahead()">
    ${parent.extrahead()}
    % if collapse_modindex:
    <script type="text/javascript">
      DOCUMENTATION_OPTIONS.COLLAPSE_MODINDEX = true;
    </script>
    % endif
</%def>

<h1 id="global-module-index">${_('Global Module Index')}</h1>
% for i, letter in enumerate(letters):
    ${i > 0 and '| ' or ''}<a href="#cap-${letter}"><strong>${letter}</strong></a>
% endfor
<hr/>

<table width="100%" class="indextable" cellspacing="0" cellpadding="2">
% for modname, collapse, cgroup, indent, fname, synops, pform, dep in modindexentries:
    % if not modname:
        <tr class="pcap"><td></td><td>&nbsp;</td><td></td></tr>
        <tr class="cap"><td></td><td><a name="cap-${fname}"><strong>${fname}</strong></a></td><td></td></tr>
    % else:
        <tr${indent and 'class="cg-%s"' % cgroup or ''}>
         <td>
            % if collapse:
           <img src="${pathto('_static/minus.png', 1)}" id="toggle-${cgroup}"
                class="toggler" style="display: none" alt="-" />
             %endif
         </td>
         <td>${indent and '&nbsp;&nbsp;&nbsp;' or '' }
           % if fname:
            <a href="${fname}"><tt class="xref">${modname|h}</tt></a>
           % else:
            <tt class="xref">${modname|h}</tt>
           % endif

         % if pform and pform[0]:
            <em>(${ ', '.join(pform) })</em>
         % endif
        </td><td>
           % if dep:
            <strong>${_('Deprecated')}:</strong>
           % endif
         <em>${synops|h}</em></td></tr>
    % endif
% endfor
</table>

