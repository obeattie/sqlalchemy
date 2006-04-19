from sqlalchemy.ext.selectresults import *


def install_plugin():
    mapping.global_extensions.append(SelectResultsExt)
install_plugin()
