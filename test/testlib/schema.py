import testbase
from sqlalchemy import schema

__all__ = 'Table', 'Column',

table_options = {}

def Table(*args, **kw):
    """A schema.Table wrapper/hook for dialect-specific tweaks."""

    test_opts = dict([(k,kw.pop(k)) for k in kw.keys()
                      if k.startswith('test_')])

    kw.update(table_options)

    if testbase.db.name == 'mysql':
        if 'mysql_engine' not in kw and 'mysql_type' not in kw:
            if 'test_needs_fk' in test_opts or 'test_needs_acid' in test_opts:
                kw['mysql_engine'] = 'InnoDB'

    return schema.Table(*args, **kw)

def Column(*args, **kw):
    """A schema.Column wrapper/hook for dialect-specific tweaks."""

    # TODO: a Column that creates a Sequence automatically for PK columns,
    # which would help Oracle tests
    return schema.Column(*args, **kw)
