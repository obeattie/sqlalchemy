<%flags>inherit='document_base.myt'</%flags>
<%attr>title='Modules and Classes'</%attr>
<&|doclib.myt:item, name="docstrings", description="Modules and Classes" &>
<%init>
    import sqlalchemy.schema as schema
    import sqlalchemy.engine as engine
    import sqlalchemy.engine.strategies as strategies
    import sqlalchemy.sql as sql
    import sqlalchemy.pool as pool
    import sqlalchemy.mapping as mapping
    import sqlalchemy.exceptions as exceptions
    import sqlalchemy.ext.proxy as proxy
    import sqlalchemy.mods.threadlocal as threadlocal
</%init>


<& pydoc.myt:obj_doc, obj=sql, classes=[sql.Engine, sql.AbstractDialect, sql.ClauseParameters, sql.Compiled, sql.ClauseElement, sql.TableClause, sql.ColumnClause] &>
<& pydoc.myt:obj_doc, obj=schema &>
<& pydoc.myt:obj_doc, obj=engine, classes=[engine.ComposedSQLEngine, engine.Connection, engine.Transaction, engine.Dialect, engine.ConnectionProvider, engine.ExecutionContext, engine.ResultProxy, engine.RowProxy] &>
<& pydoc.myt:obj_doc, obj=strategies &>
<& pydoc.myt:obj_doc, obj=mapping, classes=[mapping.Mapper, mapping.MapperExtension] &>
<& pydoc.myt:obj_doc, obj=mapping.query, classes=[mapping.query.Query] &>
<& pydoc.myt:obj_doc, obj=mapping.objectstore, classes=[mapping.objectstore.Session, mapping.objectstore.SessionTransaction] &>
<& pydoc.myt:obj_doc, obj=threadlocal &>
<& pydoc.myt:obj_doc, obj=exceptions &>
<& pydoc.myt:obj_doc, obj=pool, classes=[pool.DBProxy, pool.Pool, pool.QueuePool, pool.SingletonThreadPool] &>
<& pydoc.myt:obj_doc, obj=proxy &>

</&>
