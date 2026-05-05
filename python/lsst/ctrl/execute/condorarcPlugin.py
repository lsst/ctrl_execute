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

from lsst.ctrl.execute.allocator import Allocator
from lsst.ctrl.execute.findPackageFile import find_package_file
from lsst.resources import ResourcePathExpression

_LOG = logging.getLogger(__name__)


class CondorarcPlugin(Allocator):
    @staticmethod
    def countArcJobs(collector, jobname, jobstates):
        """Check condor queue for Arc Glideins of given states

        Parameters
        ----------
        jobname : `string`
                  Condor jobname to be searched for via condor_q.
        jobstates : `string`
                  Condor jobstates to be searched for via condor_q.

        Returns
        -------
        numberOfJobs : `int`
                       The number of Arc jobs detected via condor_q.
        """
        constr = f"--constraint 'JobNodeset ==  \"{jobname}\"'"
        autof = "-af ClusterId ProcId"

        print("COLLECTOR")
        print(collector)
        print("COLLECTOR")

        # k8spool="-pool 134.79.23.197   -name 'schedd@134.79.23.197'"
        k8spool = f"-pool {collector}   -name 'schedd@{collector}'"
        if jobstates == "R":
            stateflag = "-run"
        elif jobstates == "I":
            stateflag = "-idle"
        batcmd = f"condor_q {k8spool} {constr} {autof} {stateflag} | wc -l"
        try:
            resultPD = subprocess.check_output(batcmd, shell=True)
        except subprocess.CalledProcessError as e:
            _LOG.error(e.output)
        numberOfJobs = int(resultPD.decode("UTF-8"))
        return numberOfJobs

    @staticmethod
    def countIdleArcJobs(collector, jobname):
        """Check Condor queue for Idle Glideins

        Parameters
        ----------
        jobname : `string`
            Condor Arc jobname to be searched for via condor_q

        Returns
        -------
        numberOfJobs : `int`
                       The number of Condor Arc jobs detected via condor_q
        """
        numberOfJobs = CondorarcPlugin.countArcJobs(collector, jobname, jobstates="I")
        return numberOfJobs

    @staticmethod
    def countRunningArcJobs(collector, jobname):
        """Check Condor queue for Running Glideins

        Parameters
        ----------
        jobname : `string`
            Condor Arc jobname to be searched for via condor_q.

        Returns
        -------
        numberOfJobs : `int`
                       The number of Condor Arc jobs detected via condor_q.
        """
        numberOfJobs = CondorarcPlugin.countArcJobs(collector, jobname, jobstates="R")
        return numberOfJobs

    def createFilesFromTemplates(self):
        """Create the HTCondor submit file

        Returns
        -------
        generatedHTCondorFile : `str`
            name of the HTCondor job description file
        """

        scratchDirParam = self.getScratchDirectory()
        template = Template(scratchDirParam)
        template.substitute(USER_HOME=self.getUserHome())

        # create the slurm submit file
        condorName = find_package_file("generic.htcondor.template", kind="templates", platform=self.platform)
        generatedHTCondorFile = self.createSubmitFile(condorName)

        _LOG.debug("The generated HTCondor submit file is %s", generatedHTCondorFile)

        # create the script that the slurm submit file calls
        scriptName = find_package_file("lsstgrid.sh.template", kind="templates", platform=self.platform)
        self.createAllocationFile(scriptName)

        return generatedHTCondorFile

    def submit(self):
        """Submit the glidein jobs to the Batch system."""
        configName = find_package_file("condorarcConfig.py", platform=self.platform)

        _LOG.info("Sending this configName to loadCondor")
        _LOG.info(configName)
        self.loadCondor(configName)
        verbose = self.isVerbose()
        auto = self.isAuto()

        cpus = self.getCPUs()
        # memoryPerCore = self.getMemoryPerCore()
        # totalMemory = cpus * memoryPerCore

        # make some glidein scratch directories if necessary
        template = Template(self.getLocalScratchDirectory())
        localScratchDir = Path(template.substitute(USER_SCRATCH=self.getUserScratch()))
        glideinDateDir = localScratchDir / self.defaults["DATE_STRING"]
        localScratchDir.mkdir(exist_ok=True)
        glideinDateDir.mkdir(exist_ok=True)
        _LOG.debug(
            "The working local scratch directory localScratchDir is %s ",
            localScratchDir,
        )
        _LOG.debug("glideinDateDir %s", glideinDateDir)

        auser = self.getUserName()
        anodeset = self.getNodeset()
        if anodeset is None:
            jobname = f"glide_{auser}"
        else:
            jobname = f"{anodeset}glide_{auser}"

        _LOG.debug("The unix user name is %s", auser)
        _LOG.debug("The JobNodeset for the glidein jobs is %s", jobname)
        _LOG.debug("The local user home directory is %s", self.getUserHome())


        if auto:
            self.glideinsFromJobPressure()
        else:
            collector = self.getCollector()
            generatedHTCondorFile = self.createFilesFromTemplates()
            changedir = os.path.dirname(generatedHTCondorFile)
            os.chdir(changedir)
            k8spool = f"-pool {collector} -name schedd@{collector}"
            # apenv = f"-append environment=\"MEMPERCORE={memoryPerCore}\""
            # apenv = apenv + f" -append environment=\"ENODESET={anodeset}\""
            # apenv = f" -append environment=\"ENODESET={anodeset}\""
            apq = "-append queue"
            cmd = f"condor_submit {k8spool} {apq} {generatedHTCondorFile}"

            # cmd = f"condor_submit {generatedHTCondorFile}"
            # cmd = f"sbatch --mem {totalMemory} {generatedHTCondorFile}"
            nodes = self.getNodes()
            # In this case 'nodes' is the Target.
            _LOG.debug("The generatedHTCondorFile is  %s", generatedHTCondorFile)

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

            # Check Condor queue Running glideins
            existingGlideinsRunning = CondorarcPlugin.countRunningArcJobs(collector, jobname)

            # Check Condor queue Idle Glideins
            existingGlideinsIdle = CondorarcPlugin.countIdleArcJobs(collector, jobname)

            _LOG.debug("direct: existingGlideinsRunning %d", existingGlideinsRunning)
            _LOG.debug("direct: existingGlideinsIdle %d", existingGlideinsIdle)

            numberToAdd = nodes - (existingGlideinsRunning + existingGlideinsIdle)
            _LOG.info("The number of glidein jobs targeted is  %d", nodes)
            _LOG.info("The number of glidein jobs to submit now is %d", numberToAdd)

            for glide in range(0, numberToAdd):
                _LOG.info("Submitting glidein %d", glide)
                exitCode = self.runCommand(cmd, verbose)
                if exitCode != 0:
                    _LOG.error("error running %s", cmd)
                    sys.exit(exitCode)

    def loadCondor(self, name):
        if self.opts.reservation is not None:
            self.defaults["RESERVATION"] = f"#SBATCH --reservation {self.opts.reservation}"
        else:
            self.defaults["RESERVATION"] = ""

        if self.opts.exclude is not None:
            self.defaults["EXCLUDE"] = f"#SBATCH --exclude {self.opts.exclude}"
        else:
            self.defaults["EXCLUDE"] = ""

        if self.opts.nodelist is not None:
            self.defaults["NODELIST"] = f"#SBATCH --nodelist {self.opts.nodelist}"
        else:
            self.defaults["NODELIST"] = ""

        if self.opts.exclusive is not None:
            self.defaults["EXCLUSIVE"] = "#SBATCH --exclusive"
        else:
            self.defaults["EXCLUSIVE"] = ""

        if self.opts.exclusiveUser is not None:
            self.defaults["EXCLUSER"] = "#SBATCH --exclusive=user"
        else:
            self.defaults["EXCLUSER"] = ""

        if self.opts.qos:
            self.defaults["QOS"] = f"#SBATCH --qos {self.opts.qos}"
        else:
            self.defaults["QOS"] = ""

        allocationConfig = self.loadAllocationConfig(name, "condor")

        template = Template(allocationConfig.platform.scratchDirectory)
        scratchDir = template.substitute(USER_SCRATCH=self.getUserScratch())
        self.defaults["SCRATCH_DIR"] = scratchDir

        self.defaults["ARCCEHOST"] = allocationConfig.platform.arcceHost
        self.defaults["ARCCEPORT"] = allocationConfig.platform.arccePort

        self.allocationFileName = Path(self.configDir) / f"allocation_{self.uniqueIdentifier}.sh"
        self.defaults["GENERATED_ALLOCATE_SCRIPT"] = self.allocationFileName.name

        self.allocationFileName = Path(self.configDir) / f"lsstgrid_{self.uniqueIdentifier}.sh"
        self.defaults["GENERATED_ALLOCATE_SCRIPT"] = self.allocationFileName.name

        if self.opts.openfiles is None:
            self.defaults["OPEN_FILES"] = 20480
        else:
            self.defaults["OPEN_FILES"] = self.opts.openfiles

        if self.opts.nodeset is None:
            self.defaults["NODESET_BLOCK"] = "#"
            self.defaults["NODESET"] = ""
        else:
            self.defaults["NODESET_BLOCK"] = f'Nodeset = "{self.opts.nodeset}"'
            self.defaults["NODESET"] = f"{self.opts.nodeset}"

        # For partitionable slots the classad 'Cpus' shows how many cpus
        # remain to be allocated. Thus for a slot running jobs the value
        # of Rank of TotalCpus - Cpus will increase with the load.
        # Because higher Rank is preferred, loaded slots are favored.
        if self.opts.packnodes is None:
            self.defaults["PACK_BLOCK"] = "#"
        else:
            self.defaults["PACK_BLOCK"] = "Rank = TotalCpus - Cpus"

    def createAllocationFile(self, input: ResourcePathExpression):
        """Creates Allocation script file using the file "input" as a Template

        Returns
        -------
        outfile : `str`
            The newly created file name
        """
        outfile = self.createFile(input, self.allocationFileName)
        _LOG.debug("Wrote arc job bash script to %s", outfile)
        os.chmod(outfile, 0o755)
        return outfile

    def glideinsFromJobPressure(self):
        """Determine and submit the glideins needed from job pressure."""

        verbose = self.isVerbose()
        wallClock = self.getWallClock()
        cpus = self.getCPUs()
        autoCPUs = self.getAutoCPUs()
        minAutoCPUs = self.getMinAutoCPUs()
        if cpus >= minAutoCPUs:
            autoCPUs = cpus
        memoryPerCore = self.getMemoryPerCore()
        memoryLimit = autoCPUs * memoryPerCore

        print("autoCPUs")
        print(autoCPUs)
        print("memoryPerCore")
        print(memoryPerCore)
        print("memoryLimit")
        print(memoryLimit)

        # exit(0)

        auser = self.getUserName()
        anodeset = self.getNodeset()
        collector = self.getCollector()

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
        if anodeset is None:
            full_constraint += " && (JobNodeset is None)"
        else:
            jnodeset = f'(JobNodeset=="{anodeset}")'
            full_constraint += f" && {jnodeset}"
        _LOG.info("Auto: Query for htcondor jobs.")
        _LOG.debug("full_constraint %s", full_constraint)
        try:
            collector1 = htcondor.Collector("134.79.23.197")
            schedd_ad = collector1.locate(htcondor.DaemonTypes.Schedd)
            schedd1 = htcondor.Schedd(schedd_ad)

            myconstr = f"{full_constraint}"
            # myproj=["ClusterId","ProcId","JobStatus","Owner",
            # "RequestCpus","JobUniverse","RequestMemory","ImageSize"]
            # answer = schedd1.query( constraint=myconstr, projection=myproj)
            answer = schedd1.query(constraint=myconstr, projection=projection)

            condorq_data = {}
            condorq_data["k8sschedd"] = answer

        except Exception as exc:
            raise type(exc)("Problem querying condor schedd for jobs") from None

        if not condorq_data:
            _LOG.info("Auto: No HTCondor Jobs detected.")
            return

        _LOG.info("Auto: HTCondor Jobs detected.")

        generatedHTCondorFile = self.createFilesFromTemplates()
        _LOG.debug("generatedHTCondorFile %s", generatedHTCondorFile)
        condorq_large = []
        condorq_small = []
        schedd_name, condorq_full = condorq_data.popitem()

        _LOG.info("schedd_name")
        _LOG.info(schedd_name)
        _LOG.info("Auto: Search for Large htcondor jobs.")
        # for jid, ajob in condorq_full.items():
        for ajob in condorq_full:
            thisCpus = ajob["RequestCpus"]
            if isinstance(ajob["RequestMemory"], int):
                thisEvalMemory = ajob["RequestMemory"]
            else:
                # print(ajob)
                # print(type(ajob["RequestMemory"]))
                thisEvalMemory = ajob["RequestMemory"].eval()
                _LOG.debug("Making an evaluation %s", thisEvalMemory)
            # Search for jobs that are Large jobs
            # thisCpus > 16 or thisEvalMemory > 16*4096
            ajob["RequestMemoryEval"] = thisEvalMemory
            if thisEvalMemory > memoryLimit or thisCpus > autoCPUs:
                _LOG.info("Appending a Large Job %d %d", ajob["ClusterId"], ajob["ProcId"])
                condorq_large.append(ajob)
            else:
                condorq_small.append(ajob)

        if not condorq_large:
            _LOG.info("Auto: no Large jobs detected.")
        else:
            _LOG.info("Auto: detected Large jobs")
            for ajob in condorq_large:
                _LOG.debug("%d.%d", ajob["ClusterId"], ajob["ProcId"])
                thisMemory = ajob["RequestMemoryEval"]
                thisCores = ajob["RequestCpus"]
                clusterid = ajob["ClusterId"]
                procid = ajob["ProcId"]
                job_label = f"{clusterid}_{procid}_{thisMemory}"

                memCores = thisMemory//8192 + 1
                useCores = max(thisCores, memCores)
                if useCores < autoCPUs:
                    useCores = autoCPUs

                _LOG.info("Large job")
                _LOG.info("Large: mem requested  %s", thisMemory)
                _LOG.info("Large: cores for memory  %s", memCores)
                _LOG.info("Large: cores in description  %s", thisCores)
                _LOG.info("Large: cores requesting  %s", useCores)

                hash = hashlib.sha1(job_label.encode("UTF-8")).hexdigest()
                shash = hash[:6]
                if anodeset is None:
                    jobname = f"{auser}_{shash}"
                else:
                    jobname = f"{anodeset}{auser}_{shash}"

                _LOG.info("Large: Job name (JobNodeset) %s", jobname)

                # Check if Job exists Idle in the queue
                numberJobname = CondorarcPlugin.countIdleArcJobs(collector, jobname)
                if numberJobname > 0:
                    _LOG.info("Job %s already exists, do not submit", jobname)
                    continue

                changedir = os.path.dirname(generatedHTCondorFile)
                os.chdir(changedir)
                # k8spool="-pool 134.79.23.197 -name schedd@134.79.23.197"
                k8spool = f"-pool {collector} -name schedd@{collector}"
                apname = f"-append +JobNodeset={jobname}"
                apcpu = f"-append arguments={useCores}"
                # apenv = f"-append environment=\"ENODESET={anodeset}\""

                arc1 = (f"ArcResources=<SlotRequirement><NumberOfSlots>{useCores}</NumberOfSlots>"
                        f"<SlotsPerHost>{useCores}</SlotsPerHost></SlotRequirement>"
                        "<QueueName>lsst</QueueName>"
                        "<IndividualPhysicalMemory>8892000000</IndividualPhysicalMemory>"
                        "<RuntimeEnvironment><Name>ENV/PROXY</Name></RuntimeEnvironment>"
                        f"<WallTime>{wallClock}</WallTime>")

                aparc = f"-append {arc1}"
                apq = "-append queue"

                append = f"{apname} {apcpu} {aparc} {apq}"
                cmd = f"condor_submit {k8spool} {append} {generatedHTCondorFile}"
                _LOG.debug(cmd)
                _LOG.info(
                    "Submitting Large glidein for %d.%d",
                    ajob["ClusterId"],
                    ajob["ProcId"],
                )
                time.sleep(8)
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


            if anodeset is None:
                jobname = f"glide_{auser}"
            else:
                jobname = f"{anodeset}glide_{auser}"

            _LOG.info("small: Job name (JobNodeset) %s", jobname)

            # Check Condor queue Running glideins
            existingGlideinsRunning = CondorarcPlugin.countRunningArcJobs(collector, jobname)

            # Check Condor queue Idle Glideins
            existingGlideinsIdle = CondorarcPlugin.countIdleArcJobs(collector, jobname)

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

            _LOG.info("generatedHTCondorFile %s", generatedHTCondorFile)
            # $LOCAL_SCRATCH/$DATE_STRING/$CONFIGURATION_ID/configs
            _LOG.info(self.defaults["CONFIGURATION_ID"])
            _LOG.info(self.defaults["DATE_STRING"])
            _LOG.info(self.getScratchDirectory())

            arcDateDir = Path(self.getScratchDirectory()) / self.defaults["DATE_STRING"]
            arcBaseDir = Path(arcDateDir) / self.defaults["CONFIGURATION_ID"]
            arcSubmitDir = Path(arcBaseDir) / "configs"
            _LOG.info(arcDateDir)
            _LOG.info(arcBaseDir)
            _LOG.info(arcSubmitDir)
            os.chdir(arcSubmitDir)

            # cpuopt = f"--cpus-per-task {autoCPUs}"
            # memopt = f"--mem {memoryLimit}"
            # jobopt = f"-J {jobname}"

            changedir = os.path.dirname(generatedHTCondorFile)
            os.chdir(changedir)
            # k8spool="-pool 134.79.23.197 -name schedd@134.79.23.197"
            k8spool = f"-pool {collector} -name schedd@{collector}"
            # apenv = f"-append environment=\"ENODESET={anodeset}\""
            apq = "-append queue"
            cmd = f"condor_submit {k8spool} {apq} {generatedHTCondorFile}"
            _LOG.debug(cmd)
            for glide in range(0, numberOfGlideinsReduced):
                _LOG.info("Submitting glidein %s", glide)
                _LOG.info("command line: %s", cmd)
                time.sleep(2)
                exitCode = self.runCommand(cmd, verbose)
                if exitCode != 0:
                    _LOG.error("error running %s", cmd)
                    sys.exit(exitCode)

        return
