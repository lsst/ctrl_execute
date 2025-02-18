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
import sys

import lsst.utils
from lsst.resources import ResourcePath


def find_package_file(filename: str, kind: str = "config", platform: str | None = None) -> ResourcePath:
    """Find a package file from a set of candidate locations.

    Parameters
    ----------
    filename : `str`
        The unqualified name of a file to locate.

    kind : `str`
        The name of a subdirectory in which to look for the file within a
        package location, relative to an ``etc/`` directory.

    platform : `str` | `None`
        The name of a platform plugin in which to look for the file, or `None`
        if no platform plugin should be searched.

    Returns
    -------
    `lsst.resources.ResourcePath`

    Raises
    ------
    FileNotFoundError
        If a requested file object cannot be located in the candidate hierarchy

    Notes
    -----
    The candidate locations are, in descending order of preference:
    - An ``.lsst`` directory in the user's home directory.
    - An ``lsst`` directory in the user's ``$XDG_CONFIG_HOME`` directory
    - An ``etc/{kind}`` directory in the EUPS stack environment for the
      platform.
    - An ``etc/{kind}`` directory in the current Python environment/venv shared
      data directory.
    - An ``etc/{kind}`` directory in an installed ``lsst.ctrl.platform.*``
      package.
    - An ``etc/{kind}`` directory in the ``lsst.ctrl.execute`` package.
    """
    # If the path, after expansion, is absolute, we don't need to go looking
    # for it, it should be exactly where it is.
    if (_filename := ResourcePath(filename, forceAbsolute=False)).isabs():
        return _filename

    home_dir = os.getenv("HOME", "~")
    xdg_config_home = os.getenv("XDG_CONFIG_HOME", "~/.config")
    try:
        platform_pkg_dir = lsst.utils.getPackageDir(f"ctrl_platform_{platform}")
    except (LookupError, ValueError):
        platform_pkg_dir = None

    file_candidates = [
        ResourcePath(home_dir).join(".lsst").join(_filename),
        ResourcePath(xdg_config_home).join("lsst").join(_filename),
        (ResourcePath(platform_pkg_dir).join("etc").join(kind).join(_filename) if platform_pkg_dir else None),
        ResourcePath(sys.exec_prefix).join("etc").join(kind).join(_filename),
        (
            ResourcePath(f"resource://lsst.ctrl.platform.{platform}/etc/{kind}/{_filename}")
            if platform
            else None
        ),
        ResourcePath(f"resource://lsst.ctrl.execute/etc/{kind}/{_filename}"),
    ]
    try:
        found_file: ResourcePath = [c for c in file_candidates if c is not None and c.exists()][0]
    except IndexError:
        raise FileNotFoundError(f"No file {filename} found in package file lookup")
    return found_file
