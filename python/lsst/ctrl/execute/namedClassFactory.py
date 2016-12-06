#
# LSST Data Management System
# Copyright 2008-2016 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#
 
from builtins import object
 
class NamedClassFactory(object):
    """Create a new "name" class object
 
    Parameters
    ----------
    name : `str`
        the fully qualified name of an object
 
    Returns
    -------
    classobj : `object`
        an object of the specified name
    """
 
    def createClass(name):
        dot = name.rindex('.')
        pack = name[0:dot]
        modname = name[dot+1:]
        # -1 is no longer accepted in python 3
        # module = __import__(name, globals(), locals(), [modname], -1)
        module = __import__(name, globals(), locals(), [modname], 0)
        classobj = getattr(module, modname)
        if classobj is None:
            raise RuntimeError("Attempt to instantiate class \"" + name +
                               "\" failed. Could not find that class.")
        return classobj

    # static method to createClasso

    createClass = staticmethod(createClass)
