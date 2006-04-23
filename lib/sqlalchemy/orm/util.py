# mapper/util.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


import sqlalchemy.sql as sql
import sets

class TableFinder(sql.ClauseVisitor):
    """given a Clause, locates all the Tables within it into a list."""
    def __init__(self, table, check_columns=False):
        self.tables = []
        self.check_columns = check_columns
        if table is not None:
            table.accept_visitor(self)
    def visit_table(self, table):
        self.tables.append(table)
    def __len__(self):
        return len(self.tables)
    def __getitem__(self, i):
        return self.tables[i]
    def __iter__(self):
        return iter(self.tables)
    def __contains__(self, obj):
        return obj in self.tables
    def __add__(self, obj):
        return self.tables + list(obj)
    def visit_column(self, column):
        if self.check_columns:
            column.table.accept_visitor(self)


class CascadeOptions(object):
    """keeps track of the options sent to relation().cascade"""
    def __init__(self, arg=""):
        values = sets.Set([c.strip() for c in arg.split(',')])
        self.delete_orphan = "delete-orphan" in values
        self.delete = "delete" in values or self.delete_orphan
        self.save_update = "save-update" in values
        self.merge = "merge" in values
        self.expunge = "expunge" in values
    def __contains__(self, item):
        return getattr(self, item.replace("-", "_"), False)
        