# dynamic.py
# Copyright (C) the SQLAlchemy authors and contributors
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Dynamic collection API.

Dynamic collections act like Query() objects for read operations and support
basic add/delete mutation.

"""

from sqlalchemy import log, util
import sqlalchemy.exceptions as sa_exc

from sqlalchemy.orm import attributes, object_session, \
     util as mapperutil, strategies
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.util import has_identity


class DynaLoader(strategies.AbstractRelationLoader):
    def init_class_attribute(self):
        self.is_class_level = True
        self._register_attribute(self.parent.class_, impl_class=DynamicAttributeImpl, target_mapper=self.parent_property.mapper, order_by=self.parent_property.order_by)

    def create_row_processor(self, selectcontext, path, mapper, row, adapter):
        return (None, None)

DynaLoader.logger = log.class_logger(DynaLoader)

class DynamicAttributeImpl(attributes.AttributeImpl):
    uses_objects = True
    accepts_scalar_loader = False
    
    def __init__(self, class_, key, typecallable, class_manager, target_mapper, order_by, **kwargs):
        super(DynamicAttributeImpl, self).__init__(class_, key, typecallable, class_manager, **kwargs)
        self.target_mapper = target_mapper
        self.order_by = order_by
        self.query_class = AppenderQuery

    def get(self, state, passive=False):
        if passive:
            return self._get_collection_history(state, passive=True).added_items
        else:
            return self.query_class(self, state)

    def get_collection(self, state, user_data=None, passive=True):
        if passive:
            return self._get_collection_history(state, passive=passive).added_items
        else:
            history = self._get_collection_history(state, passive=passive)
            return history.added_items + history.unchanged_items

    def fire_append_event(self, state, value, initiator):
        state.modified = True

        if self.trackparent and value is not None:
            self.sethasparent(attributes.instance_state(value), True)
        for ext in self.extensions:
            ext.append(state, value, initiator or self)

    def fire_remove_event(self, state, value, initiator):
        state.modified = True

        if self.trackparent and value is not None:
            self.sethasparent(attributes.instance_state(value), False)

        for ext in self.extensions:
            ext.remove(state, value, initiator or self)
        
    def set(self, state, value, initiator):
        if initiator is self:
            return

        old_collection = self.get(state).assign(value)

        # TODO: emit events ???
        state.modified = True

    def delete(self, *args, **kwargs):
        raise NotImplementedError()
        
    def get_history(self, state, passive=False):
        c = self._get_collection_history(state, passive)
        return (c.added_items, c.unchanged_items, c.deleted_items)
        
    def _get_collection_history(self, state, passive=False):
        try:
            c = state.dict[self.key]
        except KeyError:
            state.dict[self.key] = c = CollectionHistory(self, state)

        if not passive:
            return CollectionHistory(self, state, apply_to=c)
        else:
            return c
        
    def append(self, state, value, initiator, passive=False):
        if initiator is not self:
            self._get_collection_history(state, passive=True).added_items.append(value)
            self.fire_append_event(state, value, initiator)
    
    def remove(self, state, value, initiator, passive=False):
        if initiator is not self:
            self._get_collection_history(state, passive=True).deleted_items.append(value)
            self.fire_remove_event(state, value, initiator)

            
class AppenderQuery(Query):
    def __init__(self, attr, state):
        super(AppenderQuery, self).__init__(attr.target_mapper, None)
        self.instance = state.obj()
        self.attr = attr
    
    def __session(self):
        sess = object_session(self.instance)
        if sess is not None and self.autoflush and sess.autoflush and self.instance in sess:
            sess.flush()
        if not has_identity(self.instance):
            return None
        else:
            return sess
    
    def session(self):
        return self.__session()
    session = property(session, lambda s, x:None)
    
    def __iter__(self):
        sess = self.__session()
        if sess is None:
            return iter(self.attr._get_collection_history(
                attributes.instance_state(self.instance),
                passive=True).added_items)
        else:
            return iter(self._clone(sess))

    def __getitem__(self, index):
        sess = self.__session()
        if sess is None:
            return self.attr._get_collection_history(
                attributes.instance_state(self.instance),
                passive=True).added_items.__getitem__(index)
        else:
            return self._clone(sess).__getitem__(index)
    
    def count(self):
        sess = self.__session()
        if sess is None:
            return len(self.attr._get_collection_history(
                attributes.instance_state(self.instance),
                passive=True).added_items)
        else:
            return self._clone(sess).count()
    
    def _clone(self, sess=None):
        # note we're returning an entirely new Query class instance here
        # without any assignment capabilities;
        # the class of this query is determined by the session.
        instance = self.instance
        if sess is None:
            sess = object_session(instance)
            if sess is None:
                raise sa_exc.UnboundExecutionError("Parent instance %s is not bound to a Session, and no contextual session is established; lazy load operation of attribute '%s' cannot proceed" % (mapperutil.instance_str(instance), self.attr.key))

        q = sess.query(self.attr.target_mapper).with_parent(instance, self.attr.key)
        if self.attr.order_by:
            q = q.order_by(self.attr.order_by)
        return q

    def assign(self, collection):
        instance = self.instance
        if has_identity(instance):
            oldlist = list(self)
        else:
            oldlist = []
        self.attr._get_collection_history(attributes.instance_state(self.instance), passive=True).replace(oldlist, collection)
        return oldlist
        
    def append(self, item):
        self.attr.append(attributes.instance_state(self.instance), item, None)

    def remove(self, item):
        self.attr.remove(attributes.instance_state(self.instance), item, None)

            
class CollectionHistory(object): 
    """Overrides AttributeHistory to receive append/remove events directly."""

    def __init__(self, attr, state, apply_to=None):
        if apply_to:
            deleted = util.IdentitySet(apply_to.deleted_items)
            added = apply_to.added_items
            coll = AppenderQuery(attr, state).autoflush(False)
            self.unchanged_items = [o for o in util.IdentitySet(coll) if o not in deleted]
            self.added_items = apply_to.added_items
            self.deleted_items = apply_to.deleted_items
        else:
            self.deleted_items = []
            self.added_items = []
            self.unchanged_items = []
            
    def replace(self, olditems, newitems):
        self.added_items = newitems
        self.deleted_items = olditems
        
