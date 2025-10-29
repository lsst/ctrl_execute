# This file is part of ctrl_execute.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


class NamedClassFactory:
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

    @staticmethod
    def createClass(name):
        dot = name.rindex(".")
        pack = name[0:dot]
        modname = name[dot + 1 :]
        modname = modname[0].capitalize() + modname[1:]
        # -1 is no longer accepted in python 3
        # module = __import__(name, globals(), locals(), [modname], -1)
        module = __import__(name, globals(), locals(), [modname], 0)
        classobj = getattr(module, modname)
        if classobj is None:
            raise RuntimeError(f"Attempt to instantiate class {name!r} failed. Could not find that class.")
        return classobj
