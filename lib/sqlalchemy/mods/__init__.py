import sets

def install_mods(*mods):
    for mod in mods:
        if isinstance(mod, str):
            _mod = getattr(__import__('sqlalchemy.mods.%s' % mod).mods, mod)
            if _mod not in installed_mods:
                _mod.install_plugin()
                installed_mods.add(_mod)
        else:
            if mod not in installed_mods:
                mod.install_plugin()
                installed_mods.add(mod)
            
installed_mods = sets.Set()