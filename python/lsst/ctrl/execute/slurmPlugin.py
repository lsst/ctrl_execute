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

import hashlib
import math
import os
import subprocess
import sys
import time
from string import Template

import htcondor
from lsst.ctrl.bps.htcondor import condor_q
from lsst.ctrl.execute.allocator import Allocator


class SlurmPlugin(Allocator):

    @staticmethod
    def countSlurmJobs(jobname, jobstates):
        """Check Slurm queue for Glideins of given states

        Parameters
        ----------
        jobname : `string`
                  Slurm jobname to be searched for via squeue.
        jobstates : `string`
                  Slurm jobstates to be searched for via squeue.

        Returns
        -------
        numberOfJobs : `int`
                       The number of Slurm jobs detected via squeue.
        """
        batcmd = f"squeue --noheader --states={jobstates} --name={jobname} | wc -l"
        print(f"The squeue command is {batcmd}")
        time.sleep(3)
        try:
            resultPD = subprocess.check_output(batcmd, shell=True)
        except subprocess.CalledProcessError as e:
            print(e.output)
        numberOfJobs = int(resultPD.decode("UTF-8"))
        return numberOfJobs

    @staticmethod
    def countIdleSlurmJobs(jobname):
        """Check Slurm queue for Idle Glideins

        Parameters
        ----------
        jobname : `string`
            Slurm jobname to be searched for via squeue.

        Returns
        -------
        numberOfJobs : `int`
                       The number of Slurm jobs detected via squeue.
        """
        print(f"Checking if idle Slurm job {jobname} exists:")
        numberOfJobs = SlurmPlugin.countSlurmJobs(jobname, jobstates="PD")
        return numberOfJobs

    @staticmethod
    def countRunningSlurmJobs(jobname):
        """Check Slurm queue for Running Glideins

        Parameters
        ----------
        jobname : `string`
            Slurm jobname to be searched for via squeue.

        Returns
        -------
        numberOfJobs : `int`
                       The number of Slurm jobs detected via squeue.
        """
        print(f"Checking if running Slurm job {jobname} exists:")
        numberOfJobs = SlurmPlugin.countSlurmJobs(jobname, jobstates="R")
        return numberOfJobs

    def submit(self, platform, platformPkgDir):
        configName = os.path.join(platformPkgDir, "etc", "config", "slurmConfig.py")

        self.loadSlurm(configName, platformPkgDir)
        verbose = self.isVerbose()
        auto = self.isAuto()

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

        cpus = self.getCPUs()
        memoryPerCore = self.getMemoryPerCore()
        totalMemory = cpus * memoryPerCore

        # run the sbatch command
        template = Template(self.getLocalScratchDirectory())
        localScratchDir = template.substitute(USER_SCRATCH=self.getUserScratch())
        slurmSubmitDir = os.path.join(localScratchDir, self.defaults["DATE_STRING"])
        if not os.path.exists(localScratchDir):
            os.mkdir(localScratchDir)
        if not os.path.exists(slurmSubmitDir):
            os.mkdir(slurmSubmitDir)
        os.chdir(slurmSubmitDir)
        if verbose:
            print(
                "The working local scratch directory localScratchDir is %s "
                % localScratchDir
            )
            print("The generated Slurm submit file is %s " % generatedSlurmFile)

        cmd = "sbatch --mem %s %s" % (totalMemory, generatedSlurmFile)

        auser = self.getUserName()
        jobname = f"glide_{auser}"
        if verbose:
            print("The unix user name is %s " % auser)
            print("The Slurm job name for the glidein jobs is %s " % jobname)
            print("The user home directory is %s " % self.getUserHome())

        if auto:
            self.glideinsFromJobPressure(generatedSlurmFile)
        else:
            nodes = self.getNodes()
            # In this case 'nodes' is the Target.

            # Limit number of cores to be <= 8000 which 500 16-core glideins
            # allowed auto glideins is 500
            allowedAutoGlideins = self.getAllowedAutoGlideins()
            # auto glidein size is 16
            autoSize = self.getAutoCPUs()
            targetedCores = nodes * cpus
            coreLimit = allowedAutoGlideins * autoSize
            if targetedCores > coreLimit:
                # Reduce number of nodes because of threshold
                nodes = int(coreLimit / cpus)
                print("Reducing number of glideins because of core limit threshold")
                print(f"coreLimit {coreLimit}")
                print(f"glidein size {cpus}")
                print(f"New number of glideins {nodes}")

            print("Targeting %s glidein(s) for the computing pool/set." % nodes)
            batcmd = "".join(["squeue --noheader --name=", jobname, " | wc -l"])
            print("The squeue command is: %s " % batcmd)
            try:
                result = subprocess.check_output(batcmd, shell=True)
            except subprocess.CalledProcessError as e:
                print(e.output)
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
                "#SBATCH --reservation %s" % self.opts.reservation
            )
        else:
            self.defaults["RESERVATION"] = ""

        if self.opts.qos:
            self.defaults["QOS"] = "#SBATCH --qos %s" % self.opts.qos
        else:
            self.defaults["QOS"] = ""

        allocationConfig = self.loadAllocationConfig(name, "slurm")

        template = Template(allocationConfig.platform.scratchDirectory)
        scratchDir = template.substitute(USER_SCRATCH=self.getUserScratch())
        self.defaults["SCRATCH_DIR"] = scratchDir

        self.allocationFileName = os.path.join(
            self.configDir, "allocation_%s.sh" % self.uniqueIdentifier
        )
        self.defaults["GENERATED_ALLOCATE_SCRIPT"] = os.path.basename(
            self.allocationFileName
        )

        # For partitionable slots the classad 'Cpus' shows how many cpus
        # remain to be allocated. Thus for a slot running jobs the value
        # of Rank of TotalCpus - Cpus will increase with the load.
        # Because higher Rank is preferred, loaded slots are favored.
        if self.opts.packnodes is None:
            self.defaults["PACK_BLOCK"] = "#"
        else:
            self.defaults["PACK_BLOCK"] = "Rank = TotalCpus - Cpus"

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

    def glideinsFromJobPressure(self, generatedSlurmFile):
        """Determine and submit the glideins needed from job pressure"""

        verbose = self.isVerbose()
        autoCPUs = self.getAutoCPUs()
        memoryPerCore = self.getMemoryPerCore()
        memoryLimit = autoCPUs * memoryPerCore
        auser = self.getUserName()

        # projection contains the job classads to be returned.
        # These include the cpu and memory profile of each job,
        # in the form of RequestCpus and RequestMemory
        projection = [
            "ClusterId",
            "ProcId",
            "JobStatus",
            "Owner",
            "RequestCpus",
            "JobUniverse",
            "RequestMemory",
        ]
        owner = f'(Owner=="{auser}")'
        # query for idle jobs
        jstat = f"(JobStatus=={htcondor.JobStatus.IDLE})"
        # query for vanilla universe
        # JobUniverse constants are in htcondor C++
        # UNIVERSE = { 1: "Standard", ..., 5: "Vanilla", ... }
        juniv = "(JobUniverse==5)"

        # The constraint determines that the jobs to be returned belong to
        # the current user (Owner) and are Idle vanilla universe jobs.
        full_constraint = f"{owner} && {jstat} && {juniv}"
        print("Auto: Query for htcondor jobs.")
        if verbose:
            print(f"full_constraint {full_constraint}")
        try:
            condorq_data = condor_q(
                constraint=full_constraint,
                projection=projection,
            )

        except Exception as exc:
            raise type(exc)("Problem querying condor schedd for jobs") from None

        if not condorq_data:
            print("Auto: No HTCondor Jobs detected.")
            return

        condorq_large = []
        condorq_small = []
        schedd_name, condorq_full = condorq_data.popitem()

        print("Auto: Search for Large htcondor jobs.")
        for jid, ajob in condorq_full.items():
            thisCpus = ajob["RequestCpus"]
            if isinstance(ajob["RequestMemory"], int):
                thisEvalMemory = ajob["RequestMemory"]
            else:
                thisEvalMemory = ajob["RequestMemory"].eval()
                if verbose:
                    print(f"Making an evaluation {thisEvalMemory}")
            # Search for jobs that are Large jobs
            # thisCpus > 16 or thisEvalMemory > 16*4096
            ajob["RequestMemoryEval"] = thisEvalMemory
            if thisEvalMemory > memoryLimit or thisCpus > autoCPUs:
                print(f"Appending a Large Job {jid}")
                condorq_large.append(ajob)
            else:
                condorq_small.append(ajob)

        if not condorq_large:
            print("Auto: no Large jobs detected.")
        else:
            print("Auto: detected Large jobs")
            for ajob in condorq_large:
                if verbose:
                    print(f"\n{ajob['ClusterId']}.{ajob['ProcId']}")
                    print(ajob)
                thisMemory = ajob["RequestMemoryEval"]
                useCores = ajob["RequestCpus"]
                clusterid = ajob["ClusterId"]
                procid = ajob["ProcId"]
                job_label = f"{clusterid}_{procid}_{thisMemory}"
                if useCores < autoCPUs:
                    useCores = autoCPUs
                hash = hashlib.sha1(job_label.encode("UTF-8")).hexdigest()
                shash = hash[:6]
                jobname = f"{auser}_{shash}"
                if verbose:
                    print(f"jobname {jobname}")
                # Check if Job exists Idle in the queue
                numberJobname = SlurmPlugin.countIdleSlurmJobs(jobname)
                if numberJobname > 0:
                    print(f"Job {jobname} already exists, do not submit")
                    continue
                cpuopt = f"--cpus-per-task {useCores}"
                memopt = f"--mem {thisMemory}"
                jobopt = f"-J {jobname}"
                cmd = f"sbatch {cpuopt} {memopt} {jobopt} {generatedSlurmFile}"
                if verbose:
                    print(cmd)
                print(
                    f"Submitting Large glidein for {ajob['ClusterId']}.{ajob['ProcId']}"
                )
                time.sleep(3)
                exitCode = self.runCommand(cmd, verbose)
                if exitCode != 0:
                    print("error running %s" % cmd)
                    sys.exit(exitCode)

        if not condorq_small:
            print("Auto: no small Jobs detected.")
        else:
            print("Auto: summarize small jobs.")
            maxNumberOfGlideins = self.getNodes()
            maxAllowedNumberOfGlideins = self.getAllowedAutoGlideins()
            if verbose:
                print(f"maxNumberOfGlideins {maxNumberOfGlideins}")
                print(f"maxAllowedNumberOfGlideins {maxAllowedNumberOfGlideins}")
            # The number of cores for the small glideins is capped at 8000
            # Corresponds to maxAllowedNumberOfGlideins = 500 16-core glideins
            if maxNumberOfGlideins > maxAllowedNumberOfGlideins:
                maxNumberOfGlideins = maxAllowedNumberOfGlideins
                print("Reducing Small Glidein limit due to threshold.")
            #
            # In the following loop we calculate the number of cores
            # required by the set of small jobs. This calculation utilizes
            # the requested cpus for a job, but also checks the requested
            # memory and counts an effective core for each 'memoryPerCore'
            # of memory (by default the 4GB per core of S3DF Slurm scheduler).
            totalCores = 0
            for ajob in condorq_small:
                requestedCpus = ajob["RequestCpus"]
                # if isinstance(ajob["RequestMemory"], int):
                #     requestedMemory = ajob["RequestMemory"]
                # else:
                #   requestedMemory = ajob["RequestMemoryEval"]
                #    print("Using RequestMemoryEval")
                requestedMemory = ajob["RequestMemoryEval"]
                totalCores = totalCores + requestedCpus
                if verbose:
                    print(f"small: jobid {ajob['ClusterId']}.{ajob['ProcId']}")
                    print(f"\tRequestCpus {requestedCpus}")
                    print(f"\tCurrent value of totalCores {totalCores}")
                neededCpus = requestedMemory / memoryPerCore
                if neededCpus > requestedCpus:
                    if verbose:
                        print("\t\tNeed to Add More:")
                        print(f"\t\tRequestMemory is {requestedMemory} ")
                        print(f"\t\tRatio to {memoryPerCore} MB is {neededCpus}")
                    totalCores = totalCores + (neededCpus - requestedCpus)
                    if verbose:
                        print(f"\t\tCurrent value of totalCores {totalCores}")

            print(f"small: The final TotalCores is {totalCores}")

            # The number of Glideins needed to service the detected Idle jobs
            # is "numberOfGlideins"
            numberOfGlideins = math.ceil(totalCores / autoCPUs)
            print(f"small: Number for detected jobs is {numberOfGlideins}")

            jobname = f"glide_{auser}"

            # Check Slurm queue Running glideins
            existingGlideinsRunning = SlurmPlugin.countRunningSlurmJobs(jobname)

            # Check Slurm queue Idle Glideins
            existingGlideinsIdle = SlurmPlugin.countIdleSlurmJobs(jobname)

            if verbose:
                print(f"small: existingGlideinsRunning {existingGlideinsRunning}")
                print(f"small: existingGlideinsIdle {existingGlideinsIdle}")

            # The number of Glideins needed to service the detected
            # Idle jobs is "numberOfGlideins" less the existing Idle glideins
            numberOfGlideinsReduced = numberOfGlideins - existingGlideinsIdle
            if verbose:
                print(f"small: Target Number to submit {numberOfGlideinsReduced}")

            # The maximum number of Glideins that we can submit with
            # the imposed threshold (maxNumberOfGlideins)
            # is maxSubmitGlideins
            existingGlideins = existingGlideinsRunning + existingGlideinsIdle
            maxSubmitGlideins = maxNumberOfGlideins - existingGlideins
            if verbose:
                print(f"small: maxNumberOfGlideins {maxNumberOfGlideins}")
                print(f"small: maxSubmitGlideins {maxSubmitGlideins}")

            # Reduce the number of Glideins to submit if threshold exceeded
            if numberOfGlideinsReduced > maxSubmitGlideins:
                numberOfGlideinsReduced = maxSubmitGlideins
                print("small: Reducing due to threshold.")
            if verbose:
                print(
                    f"small: Number of Glideins to submit is {numberOfGlideinsReduced}"
                )

            cpuopt = f"--cpus-per-task {autoCPUs}"
            memopt = f"--mem {memoryLimit}"
            jobopt = f"-J {jobname}"
            cmd = f"sbatch {cpuopt} {memopt} {jobopt} {generatedSlurmFile}"
            if verbose:
                print(cmd)
            for glide in range(0, numberOfGlideinsReduced):
                print("Submitting glidein %s " % glide)
                exitCode = self.runCommand(cmd, verbose)
                if exitCode != 0:
                    print("error running %s" % cmd)
                    sys.exit(exitCode)

        return
