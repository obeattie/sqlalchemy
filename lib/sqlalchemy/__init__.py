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
import sqlalchemy.orm as orm
from sqlalchemy.orm import *
import sqlalchemy.ext.proxy

from sqlalchemy.orm.session import Session, current_session

create_engine = sqlalchemy.engine.create_engine
create_session = sqlalchemy.orm.session.Session

def global_connect(*args, **kwargs):
    sqlalchemy.schema.default_metadata.connect(*args, **kwargs)
    