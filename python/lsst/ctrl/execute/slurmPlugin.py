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

from __future__ import print_function
from builtins import str
from builtins import object
import os, sys
import pwd
from datetime import datetime
from string import Template
from lsst.ctrl.execute import envString
from lsst.ctrl.execute.allocationConfig import AllocationConfig
from lsst.ctrl.execute.condorConfig import CondorConfig
from lsst.ctrl.execute.condorInfoConfig import CondorInfoConfig
from lsst.ctrl.execute.templateWriter import TemplateWriter
from lsst.ctrl.execute.seqFile import SeqFile

from lsst.ctrl.execute.allocator import Allocator


class slurmPlugin(Allocator):

    def submit(self, platform, platformPkgDir):
        remoteCopyCmd = "/usr/bin/cp"
    
        configName = os.path.join(platformPkgDir, "etc", "config", "slurmConfig.py")
    
        self.loadSlurm(configName)
        verbose = self.isVerbose()
    
        # create the fully-resolved scratch directory string
        scratchDirParam = self.getScratchDirectory()
        template = Template(scratchDirParam)
        scratchDir = template.substitute(USER_HOME=self.getUserHome())
    
        # create the slurm submit file
        slurmName = os.path.join(platformPkgDir, "etc", "templates", "generic.slurm.template")
        generatedSlurmFile = self.createSubmitFile(slurmName)
    
        # create the condor configuration file
        condorFile = os.path.join(platformPkgDir, "etc", "templates", "glidein_condor_config.template")
        generatedCondorConfigFile = self.createCondorConfigFile(condorFile)
    
        # create the script that the slurm submit file calls
        allocationName = os.path.join(platformPkgDir, "etc", "templates", "allocation.sh.template")
        allocationFile = self.createAllocationFile(allocationName)
    
        # run the sbatch command
        template = Template(self.getLocalScratchDirectory())
        localScratchDir = template.substitute(USER_NAME=self.getUserName())
        if not os.path.exists(localScratchDir):
            os.mkdir(localScratchDir)
        os.chdir(localScratchDir)
        cmd = "sbatch %s" % generatedSlurmFile
        exitCode = self.runCommand(cmd, verbose)
        if exitCode != 0:
            print("error running %s" % cmd)
            sys.exit(exitCode)
    
        # print node set information
        self.printNodeSetInfo()

    def loadSlurm(self, name):
        allocationConfig = self.loadAllocationConfig(name, "slurm")

        template = Template(allocationConfig.platform.scratchDirectory)
        scratchDir = template.substitute(USER_NAME=self.getUserName())
        self.defaults["SCRATCH_DIR"] = scratchDir

        self.allocationFileName = os.path.join(self.configDir, "allocation_%s.sh" % self.uniqueIdentifier)
        self.defaults["GENERATED_ALLOCATE_SCRIPT"] = os.path.basename(self.allocationFileName)

    def createAllocationFile(self, input):
        """Creates an Allocation script file using the file "input" as a Template

        Returns
        -------
        outfile : `str`
            The newly created file name
        """
        outfile = self.createFile(input, self.allocationFileName)
        if self.opts.verbose:
            print("wrote new allocation script file to %s" % outfile)
        os.chmod(outfile, 0755)
        return outfile
