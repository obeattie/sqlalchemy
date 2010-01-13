"""relation_caching.py

Load a set of Person and Address objects, specifying that 
related PostalCode, City, Country objects should be pulled from long 
term cache.

"""
import __init__ # if running as a script
from model import Person, Address, cache_address_bits
from meta import Session
from sqlalchemy.orm import eagerload
import os

for p in Session.query(Person).options(eagerload(Person.addresses), cache_address_bits):
    print p.format_full()


print "\n\nIf this was the first run of relation_caching.py, SQL was likely emitted to "\
        "load postal codes, cities, countries.\n"\
        "If run a second time, only a single SQL statement will run - all "\
        "related data is pulled from cache.\n"\
        "To clear the cache, delete the directory %r.  \n"\
        "This will cause a re-load of cities, postal codes and countries on "\
        "the next run.\n"\
        % os.path.join(__init__.root, 'container_file')
