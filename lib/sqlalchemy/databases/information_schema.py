import sqlalchemy.sql as sql
import sqlalchemy.engine as engine
import sqlalchemy.schema as schema
import sqlalchemy.ansisql as ansisql
import sqlalchemy.types as sqltypes
from sqlalchemy import *
from sqlalchemy.ansisql import *

generic_engine = ansisql.engine()

gen_schemata = schema.Table("schemata", generic_engine,
    Column("catalog_name", String),
    Column("schema_name", String),
    Column("schema_owner", String),
    schema="information_schema")

gen_tables = schema.Table("tables", generic_engine,
    Column("table_catalog", String),
    Column("table_schema", String),
    Column("table_name", String),
    Column("table_type", String),
    schema="information_schema")

gen_columns = schema.Table("columns", generic_engine,
    Column("table_schema", String),
    Column("table_name", String),
    Column("column_name", String),
    Column("is_nullable", Integer),
    Column("data_type", String),
    Column("ordinal_position", Integer),
    Column("character_maximum_length", Integer),
    Column("numeric_precision", Integer),
    Column("numeric_scale", Integer),
    Column("column_default", Integer),
    schema="information_schema")
    
gen_constraints = schema.Table("table_constraints", generic_engine,
    Column("table_schema", String),
    Column("table_name", String),
    Column("constraint_name", String),
    Column("constraint_type", String),
    schema="information_schema")

gen_column_constraints = schema.Table("constraint_column_usage", generic_engine,
    Column("table_schema", String),
    Column("table_name", String),
    Column("column_name", String),
    Column("constraint_name", String),
    schema="information_schema")

gen_key_constraints = schema.Table("key_column_usage", generic_engine,
    Column("table_schema", String),
    Column("table_name", String),
    Column("column_name", String),
    Column("constraint_name", String),
    schema="information_schema")

class ISchema(object):
    def __init__(self, engine):
        self.engine = engine
        self.cache = {}
    def __getattr__(self, name):
        if name not in self.cache:
            # This is a bit of a hack.
            # It would probably be better to have a dict
            # with just the information_schema tables at
            # the module level, so as to avoid returning
            # unrelated objects that happen to be named
            # 'gen_*'
            try:
                gen_tbl = globals()['gen_'+name]
            except KeyError:
                raise AttributeError('information_schema table %s not found' % name)
            self.cache[name] = gen_tbl.toengine(self.engine)
        return self.cache[name]


def reflecttable(engine, table, ischema_names, use_mysql=False):
    columns = gen_columns.toengine(engine)
    constraints = gen_constraints.toengine(engine)
    
    if use_mysql:
        # no idea which INFORMATION_SCHEMA spec is correct, mysql or postgres
        key_constraints = schema.Table("key_column_usage", engine,
            Column("table_schema", String),
            Column("table_name", String),
            Column("column_name", String),
            Column("constraint_name", String),
            Column("referenced_table_schema", String),
            Column("referenced_table_name", String),
            Column("referenced_column_name", String),
            schema="information_schema", useexisting=True)
    else:
        column_constraints = gen_column_constraints.toengine(engine)
        key_constraints = gen_key_constraints.toengine(engine)


    if table.schema is not None:
        current_schema = table.schema
    else:
        current_schema = engine.get_default_schema_name()
    
    s = select([columns], 
        sql.and_(columns.c.table_name==table.name,
        columns.c.table_schema==current_schema),
        order_by=[columns.c.ordinal_position])
        
    c = s.execute()
    while True:
        row = c.fetchone()
        if row is None:
            break
        #print "row! " + repr(row)
 #       continue
        (name, type, nullable, charlen, numericprec, numericscale, default) = (
            row[columns.c.column_name], 
            row[columns.c.data_type], 
            row[columns.c.is_nullable] == 'YES', 
            row[columns.c.character_maximum_length],
            row[columns.c.numeric_precision],
            row[columns.c.numeric_scale],
            row[columns.c.column_default]
            )

        args = []
        for a in (charlen, numericprec, numericscale):
            if a is not None:
                args.append(a)
        coltype = ischema_names[type]
        #print "coltype " + repr(coltype) + " args " +  repr(args)
        coltype = coltype(*args)
        colargs= []
        if default is not None:
            colargs.append(PassiveDefault(sql.text(default, escape=False)))
        table.append_item(schema.Column(name, coltype, nullable=nullable, *colargs))

    s = select([constraints.c.constraint_name, constraints.c.constraint_type, constraints.c.table_name, key_constraints], use_labels=True, from_obj=[constraints.join(column_constraints, column_constraints.c.constraint_name==constraints.c.constraint_name).join(key_constraints, key_constraints.c.constraint_name==column_constraints.c.constraint_name)])
    if not use_mysql:
        s.append_column(column_constraints)
        s.append_whereclause(constraints.c.table_name==table.name)
        s.append_whereclause(constraints.c.table_schema==current_schema)
        colmap = [constraints.c.constraint_type, key_constraints.c.column_name, column_constraints.c.table_schema, column_constraints.c.table_name, column_constraints.c.column_name]
    else:
        # this doesnt seem to pick up any foreign keys with mysql
        s.append_whereclause(key_constraints.c.table_name==constraints.c.table_name)
        s.append_whereclause(key_constraints.c.table_schema==constraints.c.table_schema)
        s.append_whereclause(constraints.c.table_name==table.name)
        s.append_whereclause(constraints.c.table_schema==current_schema)
        colmap = [constraints.c.constraint_type, key_constraints.c.column_name, key_constraints.c.referenced_table_schema, key_constraints.c.referenced_table_name, key_constraints.c.referenced_column_name]
    c = s.execute()

    while True:
        row = c.fetchone()
        if row is None:
            break
#        continue
        (type, constrained_column, referred_schema, referred_table, referred_column) = (
            row[colmap[0]],
            row[colmap[1]],
            row[colmap[2]],
            row[colmap[3]],
            row[colmap[4]]
        )
        #print "type %s on column %s to remote %s.%s.%s" % (type, constrained_column, referred_schema, referred_table, referred_column) 
        if type=='PRIMARY KEY':
            table.c[constrained_column]._set_primary_key()
        elif type=='FOREIGN KEY':
            remotetable = Table(referred_table, engine, autoload = True, schema=referred_schema)
            table.c[constrained_column].append_item(schema.ForeignKey(remotetable.c[referred_column]))
            
