from sqlalchemy import *

class Part(object):pass
class Design(object):pass
class DesignType(object):pass
class InheritedPart(object):pass

engine = create_engine('sqlite://', echo=True)

designType = Table('design_types', engine, 
	Column('design_type_id', Integer, primary_key=True),
	)

design =Table('design', engine, 
	Column('design_id', Integer, primary_key=True),
	Column('design_type_id', Integer, ForeignKey('design_types.design_type_id')))

part = Table('parts', engine, 
	Column('part_id', Integer, primary_key=True),
	Column('design_id', Integer, ForeignKey('design.design_id')),
	Column('design_type_id', Integer, ForeignKey('design_types.design_type_id')))
	
inheritedPart = Table('inherited_part', engine,
	Column('ip_id', Integer, primary_key=True),
	Column('part_id', Integer, ForeignKey('parts.part_id')),
	Column('design_id', Integer, ForeignKey('design.design_id')),
	)

designType.create()
design.create()
part.create()
inheritedPart.create()
	
assign_mapper(Part, part)

assign_mapper(InheritedPart, inheritedPart, properties=dict(
	part=relation(Part, lazy=False)
))

assign_mapper(Design, design, properties=dict(
	parts=relation(Part, private=True, backref="design"),
	inheritedParts=relation(InheritedPart, private=True, backref="design"),
))

assign_mapper(DesignType, designType, properties=dict(
#	designs=relation(Design, private=True, backref="type"),
))

Design.mapper.add_property("type", relation(DesignType, lazy=False, backref="designs"))
Part.mapper.add_property("design", relation(Design, lazy=False, backref="parts"))
#Part.mapper.add_property("designType", relation(DesignType))

d = Design()
objectstore.commit()
objectstore.clear()
print "lets go !\n\n\n"
x = Design.get(1)
x.inheritedParts


