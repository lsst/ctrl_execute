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

import os


def resolve(input: str) -> str:
    """Render a string with any `$`-prefixed words substituted with a matching
    environment variable.

    .. deprecated:: w.2025.05
        `lsst.ctrl.execute.envString.resolve` is deprecated and no longer used
        by any internal APIs because `lsst.resource.ResourcePath` handles
        environment variable expansion.

    Parameters
    ----------
    input : `str`
        A string containing environment variables to resolve.

    Raises
    ------
    RuntimeError
        If the environment variable does not exist
    """

    if "$" in (retVal := os.path.expandvars(input)):
        raise RuntimeError(f"couldn't resolve all environment variables in {retVal}")
    return retVal
