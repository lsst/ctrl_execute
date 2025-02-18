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

import logging
import os
import sys
from string import Template

from lsst.ctrl.execute.allocator import Allocator

_LOG = logging.getLogger(__name__)


class PbsPlugin(Allocator):
    def submit(self, platform, platformPkgDir):
        # This have specific paths to prevent abitrary binaries from being
        # executed. The "gsi"* utilities are configured to use either grid
        # proxies or ssh, automatically.
        remoteLoginCmd = "/usr/bin/gsissh"
        remoteCopyCmd = "/usr/bin/gsiscp"

        configName = os.path.join(platformPkgDir, "etc", "config", "pbsConfig.py")

        self.loadPbs(configName)
        verbose = self.isVerbose()

        pbsName = os.path.join(platformPkgDir, "etc", "templates", "generic.pbs.template")
        generatedPbsFile = self.createPbsFile(pbsName)

        condorFile = os.path.join(platformPkgDir, "etc", "templates", "glidein_condor_config.template")
        generatedCondorConfigFile = self.createCondorConfigFile(condorFile)

        scratchDirParam = self.getScratchDirectory()
        template = Template(scratchDirParam)
        scratchDir = template.substitute(USER_HOME=self.getUserHome())

        userName = self.getUserName()
        hostName = self.getHostName()

        utilityPath = self.getUtilityPath()

        #
        # execute copy of PBS file to XSEDE node
        #
        cmd = (
            f"{remoteCopyCmd} {generatedPbsFile} "
            f"{userName}@{hostName}:{scratchDir}/{os.path.basename(generatedPbsFile)}"
        )
        _LOG.debug(cmd)
        exitCode = self.runCommand(cmd, verbose)
        if exitCode != 0:
            _LOG.error("error running %s to %s.", remoteCopyCmd, hostName)
            sys.exit(exitCode)

        #
        # execute copy of Condor config file to XSEDE node
        #
        cmd = (
            f"{remoteCopyCmd} {generatedCondorConfigFile} "
            f"{userName}@{hostName}:{scratchDir}/{os.path.basename(generatedCondorConfigFile)}"
        )
        _LOG.debug(cmd)
        exitCode = self.runCommand(cmd, verbose)
        if exitCode != 0:
            _LOG.error("error running %s to %s.", remoteCopyCmd, hostName)
            sys.exit(exitCode)

        #
        # execute qsub command on XSEDE node to perform Condor glide-in
        #
        cmd = (
            f"{remoteLoginCmd} {userName}@{hostName} "
            f"{utilityPath}/qsub {scratchDir}/{os.path.basename(generatedPbsFile)}"
        )
        _LOG.debug(cmd)
        exitCode = self.runCommand(cmd, verbose)
        if exitCode != 0:
            _LOG.error("error running %s to %s.", remoteLoginCmd, hostName)
            sys.exit(exitCode)

        self.printNodeSetInfo()

    def loadPbs(self, name):
        configuration = self.loadAllocationConfig(name, "pbs")
        template = Template(configuration.platform.scratchDirectory)
        scratchDir = template.substitute(USER_HOME=self.getUserHome())
        self.defaults["SCRATCH_DIR"] = scratchDir
