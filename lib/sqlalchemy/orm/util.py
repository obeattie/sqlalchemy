# mapper/util.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import sets

class CascadeOptions(object):
    """keeps track of the options sent to relation().cascade"""
    def __init__(self, arg=""):
        values = sets.Set([c.strip() for c in arg.split(',')])
        self.delete_orphan = "delete-orphan" in values
        self.delete = "delete" in values or self.delete_orphan or "all" in values
        self.save_update = "save-update" in values or "all" in values
        self.merge = "merge" in values or "all" in values
        self.expunge = "expunge" in values or "all" in values
    def __contains__(self, item):
        return getattr(self, item.replace("-", "_"), False)
        