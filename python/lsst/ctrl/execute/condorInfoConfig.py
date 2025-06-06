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

import lsst.pex.config as pexConfig
from lsst.ctrl.execute.findPackageFile import find_package_file


class FakeTypeMap(dict):
    def __init__(self, configClass):
        self.configClass = configClass

    def __getitem__(self, k):
        return self.setdefault(k, self.configClass)


class UserInfoConfig(pexConfig.Config):
    """User information"""

    name = pexConfig.Field(doc="user login name", dtype=str, default=None)
    home = pexConfig.Field(doc="user home directory", dtype=str, default=None)
    scratch = pexConfig.Field(doc="user scratch directory", dtype=str, default=None)


class UserConfig(pexConfig.Config):
    """User specific information"""

    user = pexConfig.ConfigField(doc="user", dtype=UserInfoConfig)


class CondorInfoConfig(pexConfig.Config):
    """A pex_config file describing the platform specific information about
    remote user logins.
    """

    platform = pexConfig.ConfigChoiceField("platform info", FakeTypeMap(UserConfig))


if __name__ == "__main__":
    config = CondorInfoConfig()
    filename = find_package_file("condor-info.py")
    config.loadFromStream(filename.read())

    for i in config.platform:
        print(i)
