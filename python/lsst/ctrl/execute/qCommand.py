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
import os.path

from lsst.ctrl.execute.allocationConfig import AllocationConfig
from lsst.ctrl.execute.condorInfoConfig import CondorInfoConfig
from lsst.ctrl.execute.findPackageFile import find_package_file


class QCommand:
    """A class which wraps qsub-style commands for execution"""

    def __init__(self, platform: str):
        # can handle both grid-proxy and ssh logins
        self.remoteLoginCmd = "/usr/bin/gsissh"

        # can handle both grid-proxy and ssh copies
        self.remoteCopyCmd = "/usr/bin/gsiscp"

        configFileName = find_package_file("condor-info.py")

        condorInfoConfig = CondorInfoConfig()
        condorInfoConfig.loadFromStream(configFileName.read())

        configName = find_package_file("pbsConfig.py", platform=platform)

        allocationConfig = AllocationConfig()
        allocationConfig.loadFromStream(configName.read())

        self.userName = condorInfoConfig.platform[platform].user.name

        self.hostName = allocationConfig.platform.loginHostName
        self.utilityPath = allocationConfig.platform.utilityPath

    def runCommand(self, command):
        """Execute the command line"""
        cmd_split = command.split()
        pid = os.fork()
        if not pid:
            os.execvp(cmd_split[0], cmd_split)
        pid, status = os.wait()
        # low order bits of status contain the signal that killed the process
        # high order bits of status contain the exit code
        exitCode = (status & 0xFF00) >> 8
        return exitCode
