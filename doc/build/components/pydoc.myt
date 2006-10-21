<%global>
import docstring
</%global>

<%method obj_doc>
    <%args>
        obj
    </%args>

<div>
<&|formatting.myt:formatplain&><% obj.doc %></&>

% if not obj.isclass and obj.functions:
    <div>
<&|formatting.myt:paramtable&>
%   for func in obj.functions:
    <& SELF:function_doc, func=func &>
%
</&>
% else:
% if obj.functions:
<&|formatting.myt:paramtable&>
%   for func in obj.functions:
%   if isinstance(func, docstring.FunctionDoc):
    <& SELF:function_doc, func=func &>
%   elif isinstance(func, docstring.PropertyDoc):
    <& SELF:property_doc, prop=func &>
%
%
</div>
</&>
%
%

% if obj.classes:
<&|formatting.myt:paramtable&>
%   for class_ in obj.classes:
      <& SELF:obj_doc, obj=class_ &>
%   
</&>
%    
</div>

</%method>

<%method function_doc>
    <%args>func</%args>
    <tr>
    <td>
        <div class="darkcell">
        <A name=""></a>
        <b><% func.name %>(<% string.join(map(lambda k: "<i>%s</i>" % k, func.arglist), ", ")%>)</b>
        <div class="docstring">
        <&|formatting.myt:formatplain&><% func.doc %></&>
        </div>
        </div>
    </td>
    </tr>
</%method>


<%method property_doc>
    <%args>
        prop
    </%args>
    <tr>
         <td>
         <div class="darkcell">
         <A name=""></a>
         <b><% name %></b>
         <div class="docstring">
         <&|formatting.myt:formatplain&><% prop.doc %></&>
         </div> 
         </div>
     </td>
     </tr>
</%method>


