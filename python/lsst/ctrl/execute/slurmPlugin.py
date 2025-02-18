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
import logging
import math
import os
import subprocess
import sys
import time
from pathlib import Path
from string import Template

import htcondor

from lsst.ctrl.bps.htcondor import condor_q
from lsst.ctrl.execute.allocator import Allocator
from lsst.ctrl.execute.findPackageFile import find_package_file
from lsst.resources import ResourcePath, ResourcePathExpression

_LOG = logging.getLogger(__name__)


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
        _LOG.debug("The squeue command is %s", batcmd)
        time.sleep(3)
        try:
            resultPD = subprocess.check_output(batcmd, shell=True)
        except subprocess.CalledProcessError as e:
            _LOG.error(e.output)
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
        _LOG.info("Checking if idle Slurm job %s exists:", jobname)
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
        _LOG.info("Checking if running Slurm job %s exists:", jobname)
        numberOfJobs = SlurmPlugin.countSlurmJobs(jobname, jobstates="R")
        return numberOfJobs

    def createFilesFromTemplates(self):
        """Create the Slurm submit, script, and htcondor config files

        Returns
        -------
        generatedSlurmFile : `str`
            name of the Slurm job description file
        """

        scratchDirParam = self.getScratchDirectory()
        template = Template(scratchDirParam)
        template.substitute(USER_HOME=self.getUserHome())

        # create the slurm submit file
        slurmName = find_package_file("generic.slurm.template", kind="templates", platform=self.platform)
        generatedSlurmFile = self.createSubmitFile(slurmName)

        # create the condor configuration file
        condorFile = find_package_file(
            "glidein_condor_config.template", kind="templates", platform=self.platform
        )
        self.createCondorConfigFile(condorFile)

        # create the script that the slurm submit file calls
        allocationName = find_package_file("allocation.sh.template", kind="templates", platform=self.platform)
        self.createAllocationFile(allocationName)

        _LOG.debug("The generated Slurm submit file is %s", generatedSlurmFile)

        return generatedSlurmFile

    def submit(self):
        """Submit the glidein jobs to the Batch system."""
        configName = find_package_file("slurmConfig.py", platform=self.platform)

        self.loadSlurm(configName)
        verbose = self.isVerbose()
        auto = self.isAuto()

        cpus = self.getCPUs()
        memoryPerCore = self.getMemoryPerCore()
        totalMemory = cpus * memoryPerCore

        # run the sbatch command
        template = Template(self.getLocalScratchDirectory())
        localScratchDir = Path(template.substitute(USER_SCRATCH=self.getUserScratch()))
        slurmSubmitDir = localScratchDir / self.defaults["DATE_STRING"]
        localScratchDir.mkdir(exist_ok=True)
        slurmSubmitDir.mkdir(exist_ok=True)
        os.chdir(slurmSubmitDir)
        _LOG.debug(
            "The working local scratch directory localScratchDir is %s ",
            localScratchDir,
        )

        auser = self.getUserName()
        jobname = f"glide_{auser}"
        _LOG.debug("The unix user name is %s", auser)
        _LOG.debug("The Slurm job name for the glidein jobs is %s", jobname)
        _LOG.debug("The user home directory is %s", self.getUserHome())

        if auto:
            self.glideinsFromJobPressure()
        else:
            generatedSlurmFile = self.createFilesFromTemplates()
            cmd = f"sbatch --mem {totalMemory} {generatedSlurmFile}"
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
                _LOG.info("Reducing number of glideins because of core limit threshold")
                _LOG.debug("coreLimit %d", coreLimit)
                _LOG.debug("glidein size %d", cpus)
                _LOG.info("New number of glideins %d", nodes)

            _LOG.info("Targeting %d glidein(s) for the computing pool/set.", nodes)
            batcmd = "".join(["squeue --noheader --name=", jobname, " | wc -l"])
            _LOG.debug("The squeue command is: %s", batcmd)
            try:
                result = subprocess.check_output(batcmd, shell=True)
            except subprocess.CalledProcessError as e:
                _LOG.error(e.output)
            strResult = result.decode("UTF-8")

            _LOG.info("Detected this number of preexisting glidein jobs: %d", int(strResult))

            numberToAdd = nodes - int(strResult)
            _LOG.info("The number of glidein jobs to submit now is %d", numberToAdd)

            for glide in range(0, numberToAdd):
                _LOG.info("Submitting glidein %d", glide)
                exitCode = self.runCommand(cmd, verbose)
                if exitCode != 0:
                    _LOG.error("error running %s", cmd)
                    sys.exit(exitCode)

    def loadSlurm(self, name):
        if self.opts.reservation is not None:
            self.defaults["RESERVATION"] = f"#SBATCH --reservation {self.opts.reservation}"
        else:
            self.defaults["RESERVATION"] = ""

        if self.opts.qos:
            self.defaults["QOS"] = f"#SBATCH --qos {self.opts.qos}"
        else:
            self.defaults["QOS"] = ""

        allocationConfig = self.loadAllocationConfig(name, "slurm")

        template = Template(allocationConfig.platform.scratchDirectory)
        scratchDir = template.substitute(USER_SCRATCH=self.getUserScratch())
        self.defaults["SCRATCH_DIR"] = scratchDir

        self.allocationFileName = Path(self.configDir) / f"allocation_{self.uniqueIdentifier}.sh"
        self.defaults["GENERATED_ALLOCATE_SCRIPT"] = self.allocationFileName.name

        if self.opts.openfiles is None:
            self.defaults["OPEN_FILES"] = 20480
        else:
            self.defaults["OPEN_FILES"] = self.opts.openfiles

        # For partitionable slots the classad 'Cpus' shows how many cpus
        # remain to be allocated. Thus for a slot running jobs the value
        # of Rank of TotalCpus - Cpus will increase with the load.
        # Because higher Rank is preferred, loaded slots are favored.
        if self.opts.packnodes is None:
            self.defaults["PACK_BLOCK"] = "#"
        else:
            self.defaults["PACK_BLOCK"] = "Rank = TotalCpus - Cpus"

        # handle dynamic slot block template:
        # 1) if it isn't specified, just put a comment in its place
        # 2) if it's specified, but without a filename, use the default
        # 3) if it's specified with a filename, use that.
        if self.opts.dynamic is None:
            self.defaults["DYNAMIC_SLOTS_BLOCK"] = "#"
            return

        if self.opts.dynamic == "__default__":
            dynamicSlotsName = find_package_file(
                "dynamic_slots.template", kind="templates", platform=self.platform
            )
        else:
            dynamicSlotsName = ResourcePath(self.opts.dynamic)

        with dynamicSlotsName.open() as f:
            lines = f.readlines()
            block = ""
            for line in lines:
                block += line
            self.defaults["DYNAMIC_SLOTS_BLOCK"] = block

    def createAllocationFile(self, input: ResourcePathExpression):
        """Creates Allocation script file using the file "input" as a Template

        Returns
        -------
        outfile : `str`
            The newly created file name
        """
        outfile = self.createFile(input, self.allocationFileName)
        _LOG.debug("Wrote new Slurm job allocation bash script to %s", outfile)
        os.chmod(outfile, 0o755)
        return outfile

    def glideinsFromJobPressure(self):
        """Determine and submit the glideins needed from job pressure."""

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
        _LOG.info("Auto: Query for htcondor jobs.")
        _LOG.debug("full_constraint %s", full_constraint)
        try:
            condorq_data = condor_q(
                constraint=full_constraint,
                projection=projection,
            )

        except Exception as exc:
            raise type(exc)("Problem querying condor schedd for jobs") from None

        if not condorq_data:
            _LOG.info("Auto: No HTCondor Jobs detected.")
            return

        generatedSlurmFile = self.createFilesFromTemplates()
        condorq_large = []
        condorq_small = []
        schedd_name, condorq_full = condorq_data.popitem()

        _LOG.info("Auto: Search for Large htcondor jobs.")
        for jid, ajob in condorq_full.items():
            thisCpus = ajob["RequestCpus"]
            if isinstance(ajob["RequestMemory"], int):
                thisEvalMemory = ajob["RequestMemory"]
            else:
                thisEvalMemory = ajob["RequestMemory"].eval()
                _LOG.debug("Making an evaluation %s", thisEvalMemory)
            # Search for jobs that are Large jobs
            # thisCpus > 16 or thisEvalMemory > 16*4096
            ajob["RequestMemoryEval"] = thisEvalMemory
            if thisEvalMemory > memoryLimit or thisCpus > autoCPUs:
                _LOG.info("Appending a Large Job %s", jid)
                condorq_large.append(ajob)
            else:
                condorq_small.append(ajob)

        if not condorq_large:
            _LOG.info("Auto: no Large jobs detected.")
        else:
            _LOG.info("Auto: detected Large jobs")
            for ajob in condorq_large:
                _LOG.debug("\n%d.%d", ajob["ClusterId"], ajob["ProcId"])
                _LOG.debug("%s", ajob)
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
                _LOG.debug("jobname %s", jobname)
                # Check if Job exists Idle in the queue
                numberJobname = SlurmPlugin.countIdleSlurmJobs(jobname)
                if numberJobname > 0:
                    _LOG.info("Job %s already exists, do not submit", jobname)
                    continue
                cpuopt = f"--cpus-per-task {useCores}"
                memopt = f"--mem {thisMemory}"
                jobopt = f"-J {jobname}"
                cmd = f"sbatch {cpuopt} {memopt} {jobopt} {generatedSlurmFile}"
                _LOG.debug(cmd)
                _LOG.info(
                    "Submitting Large glidein for %d.%d",
                    ajob["ClusterId"],
                    ajob["ProcId"],
                )
                time.sleep(3)
                exitCode = self.runCommand(cmd, verbose)
                if exitCode != 0:
                    _LOG.error("error running %s", cmd)
                    sys.exit(exitCode)

        if not condorq_small:
            _LOG.info("Auto: no small Jobs detected.")
        else:
            _LOG.info("Auto: summarize small jobs.")
            maxNumberOfGlideins = self.getNodes()
            maxAllowedNumberOfGlideins = self.getAllowedAutoGlideins()
            _LOG.debug("maxNumberOfGlideins %d", maxNumberOfGlideins)
            _LOG.debug("maxAllowedNumberOfGlideins %d", maxAllowedNumberOfGlideins)
            # The number of cores for the small glideins is capped at 8000
            # Corresponds to maxAllowedNumberOfGlideins = 500 16-core glideins
            if maxNumberOfGlideins > maxAllowedNumberOfGlideins:
                maxNumberOfGlideins = maxAllowedNumberOfGlideins
                _LOG.info("Reducing Small Glidein limit due to threshold.")
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
                #     requestedMemory = ajob["RequestMemoryEval"]
                #     logging.debug("Using RequestMemoryEval")
                requestedMemory = ajob["RequestMemoryEval"]
                totalCores = totalCores + requestedCpus
                _LOG.debug("small: jobid %d.%d", ajob["ClusterId"], ajob["ProcId"])
                _LOG.debug("\tRequestCpus %d", requestedCpus)
                _LOG.debug("\tCurrent value of totalCores %d", totalCores)
                neededCpus = requestedMemory / memoryPerCore
                if neededCpus > requestedCpus:
                    _LOG.debug("\t\tNeed to Add More:")
                    _LOG.debug("\t\tRequestMemory is %d", requestedMemory)
                    _LOG.debug("\t\tRatio to %d MB is %d", memoryPerCore, neededCpus)
                    totalCores = totalCores + (neededCpus - requestedCpus)
                    _LOG.debug("\t\tCurrent value of totalCores %d", totalCores)

            _LOG.info("small: The final TotalCores is %d", totalCores)

            # The number of Glideins needed to service the detected Idle jobs
            # is "numberOfGlideins"
            numberOfGlideins = math.ceil(totalCores / autoCPUs)
            _LOG.info("small: Number for detected jobs is %d", numberOfGlideins)

            jobname = f"glide_{auser}"

            # Check Slurm queue Running glideins
            existingGlideinsRunning = SlurmPlugin.countRunningSlurmJobs(jobname)

            # Check Slurm queue Idle Glideins
            existingGlideinsIdle = SlurmPlugin.countIdleSlurmJobs(jobname)

            _LOG.debug("small: existingGlideinsRunning %d", existingGlideinsRunning)
            _LOG.debug("small: existingGlideinsIdle %d", existingGlideinsIdle)

            # The number of Glideins needed to service the detected
            # Idle jobs is "numberOfGlideins" less the existing Idle glideins
            numberOfGlideinsReduced = numberOfGlideins - existingGlideinsIdle
            _LOG.debug("small: Target Number to submit %d", numberOfGlideinsReduced)

            # The maximum number of Glideins that we can submit with
            # the imposed threshold (maxNumberOfGlideins)
            # is maxSubmitGlideins
            existingGlideins = existingGlideinsRunning + existingGlideinsIdle
            maxSubmitGlideins = maxNumberOfGlideins - existingGlideins
            _LOG.debug("small: maxNumberOfGlideins %d", maxNumberOfGlideins)
            _LOG.debug("small: maxSubmitGlideins %d", maxSubmitGlideins)

            # Reduce the number of Glideins to submit if threshold exceeded
            if numberOfGlideinsReduced > maxSubmitGlideins:
                numberOfGlideinsReduced = maxSubmitGlideins
                _LOG.info("small: Reducing due to threshold.")
            _LOG.debug("small: Number of Glideins to submit is %d", numberOfGlideinsReduced)

            cpuopt = f"--cpus-per-task {autoCPUs}"
            memopt = f"--mem {memoryLimit}"
            jobopt = f"-J {jobname}"
            cmd = f"sbatch {cpuopt} {memopt} {jobopt} {generatedSlurmFile}"
            _LOG.debug(cmd)
            for glide in range(0, numberOfGlideinsReduced):
                _LOG.info("Submitting glidein %s", glide)
                exitCode = self.runCommand(cmd, verbose)
                if exitCode != 0:
                    _LOG.error("error running %s", cmd)
                    sys.exit(exitCode)

        return
