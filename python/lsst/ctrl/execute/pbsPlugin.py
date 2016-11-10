#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2008-2012 LSST Corporation.
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

from __future__ import print_function
from builtins import str
from builtins import object
import os
import pwd
from datetime import datetime
from string import Template
from lsst.ctrl.execute import envString
from lsst.ctrl.execute.allocationConfig import AllocationConfig
from lsst.ctrl.execute.condorConfig import CondorConfig
from lsst.ctrl.execute.condorInfoConfig import CondorInfoConfig
from lsst.ctrl.execute.templateWriter import TemplateWriter
from lsst.ctrl.execute.seqFile import SeqFile

from lsst.ctrl.execute.plugin import Plugin

class PbsPlugin(Plugin):

    def submit(self, creator, platform, platformPkgDir):
        # This have specific paths to prevent abitrary binaries from being
        # executed. The "gsi"* utilities are configured to use either grid proxies
        # or ssh, automatically.
        remoteLoginCmd = "/usr/bin/gsissh"
        remoteCopyCmd = "/usr/bin/gsiscp"
    
        configName = os.path.join(platformPkgDir, "etc", "config", "pbsConfig.py")
        creator.loadPbs(configName)
    
        verbose = creator.isVerbose()
    
        pbsName = os.path.join(platformPkgDir, "etc", "templates", "generic.pbs.template")
        generatedPbsFile = creator.createPbsFile(pbsName)
    
        condorFile = os.path.join(platformPkgDir, "etc", "templates", "glidein_condor_config.template")
        generatedCondorConfigFile = creator.createCondorConfigFile(condorFile)
    
        scratchDirParam = creator.getScratchDirectory()
        template = Template(scratchDirParam)
        scratchDir = template.substitute(USER_HOME=creator.getUserHome())
        userName = creator.getUserName()
    
        hostName = creator.getHostName()
    
        utilityPath = creator.getUtilityPath()
    
        #
        # execute copy of PBS file to XSEDE node
        #
        cmd = "%s %s %s@%s:%s/%s" % (remoteCopyCmd, generatedPbsFile, userName,
                                     hostName, scratchDir, os.path.basename(generatedPbsFile))
        if verbose:
            print(cmd)
        exitCode = runCommand(cmd, verbose)
        if exitCode != 0:
            print("error running %s to %s." % (remoteCopyCmd, hostName))
            sys.exit(exitCode)
    
        #
        # execute copy of Condor config file to XSEDE node
        #
        cmd = "%s %s %s@%s:%s/%s" % (remoteCopyCmd, generatedCondorConfigFile, userName,
                                     hostName, scratchDir, os.path.basename(generatedCondorConfigFile))
        if verbose:
            print(cmd)
        exitCode = runCommand(cmd, verbose)
        if exitCode != 0:
            print("error running %s to %s." % (remoteCopyCmd, hostName))
            sys.exit(exitCode)
    
        #
        # execute qsub command on XSEDE node to perform Condor glide-in
        #
        cmd = "%s %s@%s %s/qsub %s/%s" % (remoteLoginCmd, userName, hostName,
                                          utilityPath, scratchDir, os.path.basename(generatedPbsFile))
        if verbose:
            print(cmd)
        exitCode = runCommand(cmd, verbose)
        if exitCode != 0:
            print("error running %s to %s." % (remoteLoginCmd, hostName))
            sys.exit(exitCode)
    
        nodes = creator.getNodes()
        slots = creator.getSlots()
        wallClock = creator.getWallClock()
        nodeString = ""
        if int(nodes) > 1:
            nodeString = "s"
        print("%s node%s will be allocated on %s with %s slots per node and maximum time limit of %s" %
              (nodes, nodeString, platform, slots, wallClock))
        print("Node set name:")
        print(creator.getNodeSetName())
