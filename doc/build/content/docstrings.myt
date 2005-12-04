<%flags>inherit='document_base.myt'</%flags>
<%attr>title='Modules and Classes'</%attr>
<&|doclib.myt:item, name="docstrings", description="Modules and Classes" &>
<%init>
    import sqlalchemy.schema as schema
    import sqlalchemy.engine as engine
    import sqlalchemy.sql as sql
    import sqlalchemy.pool as pool
    import sqlalchemy.mapping as mapping
</%init>


<& pydoc.myt:obj_doc, obj=schema &>
<& pydoc.myt:obj_doc, obj=engine, classes=[engine.SQLEngine, engine.ResultProxy, engine.RowProxy] &>
<& pydoc.myt:obj_doc, obj=sql &>
<& pydoc.myt:obj_doc, obj=pool, classes=[pool.DBProxy, pool.Pool, pool.QueuePool, pool.SingletonThreadPool] &>
<& pydoc.myt:obj_doc, obj=mapping &>
<& pydoc.myt:obj_doc, obj=mapping.objectstore, classes=[mapping.objectstore.UnitOfWork] &>
</&>