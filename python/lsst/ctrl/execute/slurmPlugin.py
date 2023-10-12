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
import subprocess
import sys
from string import Template

from lsst.ctrl.execute.allocator import Allocator


class SlurmPlugin(Allocator):
    def submit(self, platform, platformPkgDir):
        configName = os.path.join(platformPkgDir, "etc", "config", "slurmConfig.py")

        self.loadSlurm(configName, platformPkgDir)
        verbose = self.isVerbose()

        # create the fully-resolved scratch directory string
        scratchDirParam = self.getScratchDirectory()
        template = Template(scratchDirParam)
        template.substitute(USER_HOME=self.getUserHome())

        # create the slurm submit file
        slurmName = os.path.join(
            platformPkgDir, "etc", "templates", "generic.slurm.template"
        )
        generatedSlurmFile = self.createSubmitFile(slurmName)

        # create the condor configuration file
        condorFile = os.path.join(
            platformPkgDir, "etc", "templates", "glidein_condor_config.template"
        )
        self.createCondorConfigFile(condorFile)

        # create the script that the slurm submit file calls
        allocationName = os.path.join(
            platformPkgDir, "etc", "templates", "allocation.sh.template"
        )
        self.createAllocationFile(allocationName)

        nodes = self.getNodes()
        cpus = self.getCPUs()
        memoryPerCore = self.getMemoryPerCore()
        totalMemory = cpus * memoryPerCore
        print("Targeting %s glidein(s) for the computing pool/set." % nodes)

        # run the sbatch command
        template = Template(self.getLocalScratchDirectory())
        localScratchDir = template.substitute(USER_NAME=self.getUserName())
        if not os.path.exists(localScratchDir):
            os.mkdir(localScratchDir)
        os.chdir(localScratchDir)
        if verbose:
            print(
                "The working local scratch directory localScratchDir is %s "
                % localScratchDir
            )

            print("The generated Slurm submit file is %s " % generatedSlurmFile)

        cmd = "sbatch --mem %s %s" % (totalMemory, generatedSlurmFile)

        auser = self.getUserName()
        jobname = "".join(["glide_", auser])
        if verbose:
            print("The unix user name is %s " % auser)
            print("The Slurm job name for the glidein jobs is %s " % jobname)
            print("The user home directory is %s " % self.getUserHome())

        batcmd = "".join(["squeue --noheader --name=", jobname, " | wc -l"])
        result = subprocess.check_output(batcmd, shell=True)
        strResult = result.decode("UTF-8")

        print("Detected this number of preexisting glidein jobs: %s " % strResult)

        numberToAdd = nodes - int(strResult)
        print("The number of glidein jobs to submit now is %s" % numberToAdd)

        for glide in range(0, numberToAdd):
            print("Submitting glidein %s " % glide)
            exitCode = self.runCommand(cmd, verbose)
            if exitCode != 0:
                print("error running %s" % cmd)
                sys.exit(exitCode)

    def loadSlurm(self, name, platformPkgDir):
        if self.opts.reservation is not None:
            self.defaults["RESERVATION"] = (
                "#SBATCH --reservation=%s" % self.opts.reservation
            )
        else:
            self.defaults["RESERVATION"] = ""

        allocationConfig = self.loadAllocationConfig(name, "slurm")

        template = Template(allocationConfig.platform.scratchDirectory)
        scratchDir = template.substitute(USER_NAME=self.getUserName())
        self.defaults["SCRATCH_DIR"] = scratchDir

        self.allocationFileName = os.path.join(
            self.configDir, "allocation_%s.sh" % self.uniqueIdentifier
        )
        self.defaults["GENERATED_ALLOCATE_SCRIPT"] = os.path.basename(
            self.allocationFileName
        )

        # handle dynamic slot block template:
        # 1) if it isn't specified, just put a comment in it's place
        # 2) if it's specified, but without a filename, use the default
        # 3) if it's specified with a filename, use that.
        dynamicSlotsName = None
        if self.opts.dynamic is None:
            self.defaults["DYNAMIC_SLOTS_BLOCK"] = "#"
            return

        if self.opts.dynamic == "__default__":
            dynamicSlotsName = os.path.join(
                platformPkgDir, "etc", "templates", "dynamic_slots.template"
            )
        else:
            dynamicSlotsName = self.opts.dynamic

        with open(dynamicSlotsName) as f:
            lines = f.readlines()
            block = ""
            for line in lines:
                block += line
            self.defaults["DYNAMIC_SLOTS_BLOCK"] = block

    def createAllocationFile(self, input):
        """Creates Allocation script file using the file "input" as a Template

        Returns
        -------
        outfile : `str`
            The newly created file name
        """
        outfile = self.createFile(input, self.allocationFileName)
        if self.opts.verbose:
            print("Wrote new Slurm job allocation bash script to %s" % outfile)
        os.chmod(outfile, 0o755)
        return outfile
