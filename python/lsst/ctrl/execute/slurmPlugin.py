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

    def createFilesFromTemplates(self, platformPkgDir):
        """Create the Slurm submit, script, and htcondor config files

        Parameters
        ----------
        platformPkgDir : `str`
            path to the ctrl_platform package being used

        Returns
        -------
        generatedSlurmFile : `str`
            name of the Slurm job description file
        """

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

        verbose = self.isVerbose()
        if verbose:
            print("The generated Slurm submit file is %s " % generatedSlurmFile)

        return generatedSlurmFile

    def submit(self, platform, platformPkgDir):
        """Submit the glidein jobs to the Batch system

        Parameters
        ----------
        platform : `str`
            name of the target compute platform
        platformPkgDir : `str`
            path to the ctrl_platform package being used
        """
        configName = os.path.join(platformPkgDir, "etc", "config", "slurmConfig.py")

        self.loadSlurm(configName, platformPkgDir)
        verbose = self.isVerbose()
        auto = self.isAuto()

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
            print(f"\tworking local scratch directory {localScratchDir}")

        auser = self.getUserName()
        jobname = f"glide_{auser}"
        if verbose:
            print(f"\tunix user name: {auser}")
            print(f"\tuser home directory: {self.getUserHome()}")
            print(f"\tglidein name: {jobname}s")

        if auto:
            self.glideinsFromJobPressure(platformPkgDir)
        else:
            generatedSlurmFile = self.createFilesFromTemplates(platformPkgDir)
            cmd = "sbatch --mem %s %s" % (totalMemory, generatedSlurmFile)
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
                if verbose:
                    print(f"\tcore limit: {coreLimit}")
                    print(f"\tglidein size {cpus}")
                    print(f"\tnew number of glideins {nodes}")

            print(f"Targeting {nodes} glidein(s) for the computing pool/set.")
            batcmd = "".join(["squeue --noheader --name=", jobname, " | wc -l"])
            if verbose:
                print(f"\tcommand: {batcmd}")
            try:
                result = subprocess.check_output(batcmd, shell=True)
            except subprocess.CalledProcessError as e:
                print(e.output)
            strResult = result.decode("UTF-8")
            print(f"Detected {strResult} preexisting glidein(s)")

            numberToAdd = nodes - int(strResult)
            print(f"The number of glidein to submit now is {numberToAdd}")

            for glide in range(0, numberToAdd):
                print(f"Submitting glidein {glide}")
                exitCode = self.runCommand(cmd, verbose)
                if exitCode != 0:
                    print(f"Submit command {cmd} for glidein {glide} failed with exit code {exitCode}")
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

    def glideinsFromJobPressure(self, platformPkgDir):
        """Determine and submit the glideins needed from job pressure

        Parameters
        ----------
        platformPkgDir : `str`
            path to the ctrl_platform package being used
        """

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
        print("Query for idle HTCondor jobs.")
        if verbose:
            print(f"\tquery constraint: {full_constraint}")
        try:
            condorq_data = condor_q(
                constraint=full_constraint,
                projection=projection,
            )
        except Exception as exc:
            raise type(exc)("Problem querying condor schedd for jobs") from None

        if not condorq_data:
            print("No idle HTCondor jobs detected.")
            return

        generatedSlurmFile = self.createFilesFromTemplates(platformPkgDir)
        condorq_large = []
        condorq_small = []
        schedd_name, condorq_full = condorq_data.popitem()

        # Divide idle jobs into two groups: large and small jobs.
        #
        # Note: A job is considered to be large if the number of CPUs it needs
        # exceeds 16 and/or requires more than 16 * 4096 MiB of memory.
        print("Group idle jobs into two categories: large and small based on their requirements")
        for jid, ajob in condorq_full.items():
            thisCpus = ajob["RequestCpus"]
            if isinstance(ajob["RequestMemory"], int):
                thisEvalMemory = ajob["RequestMemory"]
            else:
                thisEvalMemory = ajob["RequestMemory"].eval()
            ajob["RequestMemoryEval"] = thisEvalMemory
            if thisEvalMemory > memoryLimit or thisCpus > autoCPUs:
                condorq_large.append(ajob)
                job_type = "large"
            else:
                condorq_small.append(ajob)
                job_type = "small"
            if verbose:
                print(
                    f"\tjob id: {jid}, type: {job_type}, mem req.: {thisEvalMemory} MiB, "
                    f"CPU(s) req.: {thisCpus}"
                )

        if not condorq_large:
            print("No large idle jobs detected.")
        else:
            print(f"Detected {len(condorq_large)} idle large job(s)")
            for ajob in condorq_large:
                thisMemory = ajob["RequestMemoryEval"]
                useCores = ajob["RequestCpus"]
                if useCores < autoCPUs:
                    useCores = autoCPUs

                job_id = f"{ajob['ClusterId']}.{ajob['ProcId']}"
                glidein_label = f"{job_id}_{thisMemory}"
                hash = hashlib.sha1(glidein_label.encode("UTF-8")).hexdigest()
                glidein_name = f"{auser}_{hash[:6]}"
                print(f"Attempting to create a glidein for the job {job_id}")
                if verbose:
                    print(f"\tlabel: {glidein_label}, name: {glidein_name}")

                # Check if glidein for the job was already exists, but still
                # sits idle in the queue.
                numberJobname = SlurmPlugin.countIdleSlurmJobs(glidein_name)
                if numberJobname > 0:
                    print(
                        f"A glidein for job {job_id} already exists: "
                        "a request to create the glidein not submitted"
                    )
                    continue

                cpuopt = f"--cpus-per-task {useCores}"
                memopt = f"--mem {thisMemory}"
                jobopt = f"-J {glidein_name}"
                cmd = f"sbatch {cpuopt} {memopt} {jobopt} {generatedSlurmFile}"
                if verbose:
                    print(f"\tcommand: {cmd}")
                time.sleep(3)
                exitCode = self.runCommand(cmd, verbose)
                if exitCode != 0:
                    print(f"Submit command {cmd} failed with exit code {exitCode}")
                    sys.exit(exitCode)

        if not condorq_small:
            print("No small idle jobs detected.")
        else:
            print(f"Detected {len(condorq_small)} idle small job(s)")

            # In the following loop we calculate the number of cores
            # required by the set of small jobs. This calculation utilizes
            # the requested cpus for a job, but also checks the requested
            # memory and counts an effective core for each 'memoryPerCore'
            # of memory (by default the 4GB per core of S3DF Slurm scheduler).
            print("Calculate number of needed cores")
            totalCores = 0
            for ajob in condorq_small:
                job_id = f"{ajob['ClusterId']}.{ajob['ProcId']}"

                requestedCpus = ajob["RequestCpus"]
                requestedMemory = ajob["RequestMemoryEval"]
                totalCores = totalCores + requestedCpus
                if verbose:
                    print(
                        f"\tjob id: {job_id}, mem req.: {requestedMemory} MiB, "
                        f"CPU(s): {requestedCpus} (current total: {totalCores})"
                    )

                neededCpus = requestedMemory / memoryPerCore
                if neededCpus > requestedCpus:
                    totalCores = totalCores + (neededCpus - requestedCpus)
                    if verbose:
                        print(
                            f"\t\tadjusted number of CPUs due to memory requirements: {neededCpus} "
                            f"(current total: {totalCores})"
                        )
            print(f"The calculated number of needed cores is {totalCores}")

            # The number of cores for the small glideins is capped at 8000
            # Corresponds to maxAllowedNumberOfGlideins = 500 16-core glideins
            print("Adjust glidein limit if necessary")
            maxAllowedNumberOfGlideins = self.getAllowedAutoGlideins()
            maxNumberOfGlideins = self.getNodes()
            if verbose:
                print(f"\tmax. allowed number of glideins {maxAllowedNumberOfGlideins}")
                print(f"\tmax. number of glideins: {maxNumberOfGlideins}")
            if maxNumberOfGlideins > maxAllowedNumberOfGlideins:
                print(
                    f"Current maximal numbers of glideins ({maxNumberOfGlideins}) exceeds the threshold, "
                    f"reducing to {maxAllowedNumberOfGlideins}"
                )
                maxNumberOfGlideins = maxAllowedNumberOfGlideins

            print("Calculate the number of glideins to request")
            numberOfGlideins = math.ceil(totalCores / autoCPUs)

            glidein_name = f"glide_{auser}"
            existingGlideinsRunning = SlurmPlugin.countRunningSlurmJobs(glidein_name)
            existingGlideinsIdle = SlurmPlugin.countIdleSlurmJobs(glidein_name)
            if verbose:
                print(f"\tnumber of existing running glideins: {existingGlideinsRunning}")
                print(f"\tnumber of existing idle glideins {existingGlideinsIdle}")

            # The number of glideins needed to service the detected small, idle
            # jobs is "numberOfGlideins" less the existing idle glideins.
            numberOfGlideinsReduced = numberOfGlideins - existingGlideinsIdle
            if verbose:
                print(f"\tnumber of needed glideins {numberOfGlideinsReduced}")

            # The maximum number of glideins that we can submit with
            # the imposed threshold (maxNumberOfGlideins)
            # is maxSubmitGlideins
            existingGlideins = existingGlideinsRunning + existingGlideinsIdle
            maxSubmitGlideins = maxNumberOfGlideins - existingGlideins
            if verbose:
                print(f"max. number of glideins to submit: {maxSubmitGlideins}")

            # Reduce the number of Glideins to submit if threshold exceeded
            if numberOfGlideinsReduced > maxSubmitGlideins:
                print(
                    f"Current number of needed glideins ({numberOfGlideinsReduced}) exceeds the threshold, "
                    f"reducing to {maxSubmitGlideins}"
                )
                numberOfGlideinsReduced = maxSubmitGlideins
            print(f"The calculated number of glideins to request is {numberOfGlideinsReduced}")

            print("Attempting to create glideins for idle small jobs")
            cpuopt = f"--cpus-per-task {autoCPUs}"
            memopt = f"--mem {memoryLimit}"
            jobopt = f"-J {glidein_name}"
            cmd = f"sbatch {cpuopt} {memopt} {jobopt} {generatedSlurmFile}"
            if verbose:
                print(f"\tcommand: {cmd}")
            for glide in range(0, numberOfGlideinsReduced):
                print(f"Submitting glidein {glide}")
                exitCode = self.runCommand(cmd, verbose)
                if exitCode != 0:
                    print(f"Submit command {cmd} for glidein {glide} failed with exit code {exitCode}")
                    sys.exit(exitCode)

        return
