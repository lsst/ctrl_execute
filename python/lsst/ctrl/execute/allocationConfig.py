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

import lsst.pex.config as pexConfig


class AllocatedPlatformConfig(pexConfig.Config):
    """Platform specific information"""

    queue = pexConfig.Field(doc="the scheduler queue to submit to", dtype=str, default="debug")
    email = pexConfig.Field(
        doc="line to add to the scheduler file to get email notification (if supported)",
        dtype=str,
        default=None,
    )

    scratchDirectory = pexConfig.Field(
        doc="directory on the remote system where the scheduler file is sent",
        dtype=str,
        default=None,
    )
    loginHostName = pexConfig.Field(doc="the host to login and copy files to", dtype=str, default=None)
    utilityPath = pexConfig.Field(
        doc="the directory containing the scheduler commands", dtype=str, default=None
    )
    totalCoresPerNode = pexConfig.Field(doc="the TOTAL number of cores on each node", dtype=int, default=1)
    glideinShutdown = pexConfig.Field(
        doc="number of seconds of inactivity before glideins are cancelled",
        dtype=int,
        default=3600,
    )


class AllocationConfig(pexConfig.Config):
    """A pex_config file describing the platform specific information required
    to fill out a scheduler file which will be used to submit a scheduler
    request.
    """

    # this is done on two levels instead of one for future expansion of this
    # config class, which may require local attributes to be specified.
    platform = pexConfig.ConfigField("platform allocation information", AllocatedPlatformConfig)
