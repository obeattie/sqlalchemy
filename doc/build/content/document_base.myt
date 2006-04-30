<%flags>inherit="doclib.myt"</%flags>

<%python scope="global">

    files = [
        'tutorial',
        'dbengine',
        'metadata',
        'sqlconstruction',
        'datamapping',
        'unitofwork',
        'adv_datamapping',
        'types',
        'pooling',
        'docstrings',
        ]

</%python>

<%attr>
    files=files
    wrapper='section_wrapper.myt'
    onepage='documentation'
    index='index'
    title='SQLAlchemy Documentation'
    version = '0.2.0'
</%attr>

<%method title>
% try:
#  avoid inheritance via attr instead of attributes
    <% m.base_component.attr['title'] %> - SQLAlchemy Documentation
% except KeyError:
    SQLAlchemy Documentation
%
</%method>





