# __init__.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from types import *
from sql import *
from schema import *
from exceptions import *
from engine import *
import sqlalchemy.sql
import sqlalchemy.mapping as mapping
from sqlalchemy.mapping import *
import sqlalchemy.ext.proxy

from sqlalchemy.mapping.objectstore import Session, get_session

create_engine = sqlalchemy.engine.create_engine


def global_connect(*args, **kwargs):
    sqlalchemy.schema.default_metadata.connect(*args, **kwargs)
    