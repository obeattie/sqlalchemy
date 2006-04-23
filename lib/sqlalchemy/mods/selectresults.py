from sqlalchemy.ext.selectresults import *


def install_plugin():
    orm.global_extensions.append(SelectResultsExt)
install_plugin()
