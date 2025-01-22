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

import lsst.utils
from lsst.ctrl.execute.envString import resolve
from lsst.resources import ResourcePath


def find_package_file(
    filename: str, kind: str = "config", platform: str = "s3df"
) -> ResourcePath:
    """Find a package file from a set of candidate locations.

    The candidate locations are, in descending order of preference:
    - An `.lsst` directory in the user's home directory.
    - An `lsst` directory in the user's `$XDG_CONFIG_HOME` directory
    - An `etc/{kind}` directory in the stack environment for the platform
    - An `etc/{kind}` directory in an installed `lsst.ctrl.platform.*` package
    - An `etc/{kind}` directory in the `lsst.ctrl.execute` package.

    Raises
    ------
    IndexError
        If a requested file object cannot be located in the candidate hierarchy
    """
    _filename = resolve(filename)
    home_dir = os.getenv("HOME", "/")
    xdg_config_home = os.getenv("XDG_CONFIG_HOME", f"{home_dir}/.config")
    try:
        platform_pkg_dir = lsst.utils.getPackageDir(f"ctrl_platform_{platform}")
    except (LookupError, ValueError):
        platform_pkg_dir = None

    file_candidates = [
        ResourcePath(f"file://{home_dir}/.lsst/{_filename}"),
        ResourcePath(f"file://{xdg_config_home}/lsst/{_filename}"),
        (
            ResourcePath(f"file://{platform_pkg_dir}/etc/{kind}/{_filename}")
            if platform_pkg_dir
            else None
        ),
        ResourcePath(
            f"resource://lsst.ctrl.platform.{platform}/etc/{kind}/{_filename}"
        ),
        ResourcePath(f"resource://lsst.ctrl.execute/etc/{kind}/{_filename}"),
    ]
    try:
        found_file: ResourcePath = [
            c for c in file_candidates if c is not None and c.exists()
        ][0]
    except IndexError:
        raise
    return found_file
