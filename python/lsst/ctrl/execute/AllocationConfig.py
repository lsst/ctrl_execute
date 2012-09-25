# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
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

class AllocatedPlatformConfig(pexConfig.Config):
    queue = pexConfig.Field("default root working for directories",str, default=None)
    email = pexConfig.Field("notify by e-mail pbs string", str, default=None)

    scratchDirectory  = pexConfig.Field("scratch directory",str, default=None)
    loginHostName  = pexConfig.Field("host name",str, default=None)
    utilityPath = pexConfig.Field("utility path", str, default=None)

class AllocationConfig(pexConfig.Config):
    platform = pexConfig.ConfigField("platform allocation", AllocatedPlatformConfig)
