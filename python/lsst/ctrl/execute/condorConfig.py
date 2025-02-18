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


class PlatformConfig(pexConfig.Config):
    """Platform specific information"""

    defaultRoot = pexConfig.Field(doc="remote root for working directories", dtype=str, default=None)
    localScratch = pexConfig.Field(doc="local Condor scratch directory", dtype=str, default=None)
    idsPerJob = pexConfig.Field(doc="number of ids to work on per job", dtype=int, default=1)
    dataDirectory = pexConfig.Field(
        doc="remote directory where date that jobs will use is kept",
        dtype=str,
        default=None,
    )
    fileSystemDomain = pexConfig.Field(doc="network domain name of remote system", dtype=str, default=None)
    eupsPath = pexConfig.Field(doc="location of remote EUPS stack", dtype=str, default=None)
    nodeSetRequired = pexConfig.Field(doc="is the nodeset required", dtype=bool, default=False)
    scheduler = pexConfig.Field(doc="scheduler type", dtype=str, default=None)
    manager = pexConfig.Field(doc="workflow manager", dtype=str, default=None)
    setup_using = pexConfig.Field(doc="environment setup type", dtype=str, default=None)
    manager_software_home = pexConfig.Field(
        doc="location of workflow manager software", dtype=str, default=None
    )


class CondorConfig(pexConfig.Config):
    """A pex_config file describing the platform specific information required
    to fill out templates for running ctrl_orca jobs
    """

    platform = pexConfig.ConfigField("platform configuration", PlatformConfig)


class FakeTypeMap(dict):
    def __init__(self, configClass):
        self.configClass = configClass

    def __getitem__(self, k):
        return self.setdefault(k, self.configClass)
