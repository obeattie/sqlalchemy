from sqlalchemy.orm.session import Session
from sqlalchemy.orm.query import Query

__all__ = ['ShardedSession', 'ShardedQuery']

class ShardedSession(Session):
    def __init__(self, shard_chooser, id_chooser, query_chooser, shards=None, **kwargs):
        """construct a ShardedSession.
        
            shard_chooser
                a callable which, passed a Mapper and a mapped instance, returns a
                shard ID.  this id may be based off of the attributes present within the
                object, or on some round-robin scheme.  If the scheme is based on a
                selection, it should set whatever state on the instance to mark it in
                the future as participating in that shard.
            
            id_chooser
                a callable, passed a query and a tuple of identity values,
                which should return a list of shard ids where the ID might
                reside.  The databases will be queried in the order of this
                listing.
                
            query_chooser
                for a given Query, returns the list of shard_ids where the query
                should be issued.  Results from all shards returned will be 
                combined together into a single listing.
        
        """
        super(ShardedSession, self).__init__(**kwargs)
        self.shard_chooser = shard_chooser
        self.id_chooser = id_chooser
        self.query_chooser = query_chooser
        self.__binds = {}
        self._mapper_flush_opts = {'connection_callable':self.connection}
        self._query_cls = ShardedQuery
        if shards is not None:
            for k in shards:
                self.bind_shard(k, shards[k])
        
    def connection(self, mapper=None, instance=None, shard_id=None, **kwargs):
        if shard_id is None:
            shard_id = self.shard_chooser(mapper, instance)

        if self.transaction is not None:
            return self.transaction.connection(mapper, shard_id=shard_id)
        else:
            return self.get_bind(mapper, shard_id=shard_id, instance=instance).contextual_connect(**kwargs)
    
    def get_bind(self, mapper, shard_id=None, instance=None):
        if shard_id is None:
            shard_id = self.shard_chooser(mapper, instance)
        return self.__binds[shard_id]

    def bind_shard(self, shard_id, bind):
        self.__binds[shard_id] = bind

class ShardedQuery(Query):
    def __init__(self, *args, **kwargs):
        super(ShardedQuery, self).__init__(*args, **kwargs)
        self.id_chooser = self.session.id_chooser
        self.query_chooser = self.session.query_chooser
        self._shard_id = None
        
    def _clone(self):
        q = ShardedQuery.__new__(ShardedQuery)
        q.__dict__ = self.__dict__.copy()
        return q
    
    def set_shard(self, shard_id):
        """return a new query, limited to a single shard ID.
        
        all subsequent operations with the returned query will 
        be against the single shard regardless of other state.
        """
        
        q = self._clone()
        q._shard_id = shard_id
        return q
        
    def _execute_and_instances(self, statement):
        if self._shard_id is not None:
            result = self.session.connection(mapper=self.mapper, shard_id=self._shard_id).execute(statement, **self._params)
            try:
                return iter(self.instances(result))
            finally:
                result.close()
        else:
            partial = []
            for shard_id in self.query_chooser(self):
                result = self.session.connection(mapper=self.mapper, shard_id=shard_id).execute(statement, **self._params)
                try:
                    partial = partial + list(self.instances(result))
                finally:
                    result.close()
            # if some kind of in memory 'sorting' were done, this is where it would happen
            return iter(partial)

    def get(self, ident, **kwargs):
        if self._shard_id is not None:
            return super(ShardedQuery, self).get(ident)
        else:
            for shard_id in self.id_chooser(self, ident):
                o = self.set_shard(shard_id).get(ident, **kwargs)
                if o is not None:
                    return o
            else:
                return None
    
    def load(self, ident, **kwargs):
        if self._shard_id is not None:
            return super(ShardedQuery, self).load(ident)
        else:
            for shard_id in self.id_chooser(self, ident):
                o = self.set_shard(shard_id).load(ident, raiseerr=False, **kwargs)
                if o is not None:
                    return o
            else:
                raise exceptions.InvalidRequestError("No instance found for identity %s" % repr(ident))
