#!/usr/bin/env python

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
