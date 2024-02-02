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
import socket
import subprocess
import sys
import time
from string import Template

import htcondor
from lsst.ctrl.bps.htcondor import condor_q
from lsst.ctrl.execute.allocator import Allocator


class SlurmPlugin(Allocator):
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
        jobname = f"glide_{auser}"
        if verbose:
            print("The unix user name is %s " % auser)
            print("The Slurm job name for the glidein jobs is %s " % jobname)
            print("The user home directory is %s " % self.getUserHome())

        if auto:
            self.largeGlideinsFromJobPressure(generatedSlurmFile)
            self.smallGlideinsFromJobPressure(generatedSlurmFile)
        else:
            nodes = self.getNodes()
            # In this case 'nodes' is the Target.

            # Limit the number of cores to be <= 8000 which 500 16-core glideins
            # allowed auto glideins is 500
            allowedAutoGlideins = self.getAllowedAutoGlideins()
            # auto glidein size is 16
            autoSize = self.getAutoCPUs()
            targetedCores = nodes * cpus
            coreLimit = allowedAutoGlideins * autoSize
            if targetedCores > coreLimit:
                # Reduce number of nodes because of threshold
                nodes = int( coreLimit / cpus )
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

    def largeGlideinsFromJobPressure(self, generatedSlurmFile):
        """Determine and submit the large glideins needed from job pressure
        """

        verbose = self.isVerbose()
        autoCPUs = self.getAutoCPUs()
        memoryPerCore = self.getMemoryPerCore()
        memoryLimit = autoCPUs * memoryPerCore
        auser = self.getUserName()

        try:
            schedd_name = socket.getfqdn()
            coll = htcondor.Collector()
            schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd)
            scheddref = htcondor.Schedd(schedd_ad)

            # Query for jobs that have (bps_run isnt Undefined)
            #                          (bps_job_label isnt Undefined)
            # and amongst those identify Large jobs
            #                          thisCores > 16
            #                          thisMemory > 16*4096
            #
            # projection contains the job classads to be returned.
            # These include the cpu and memory profile of each job,
            # in the form of RequestCpus and RequestMemory
            projection = [
                "bps_run",
                "bps_job_label",
                "JobStatus",
                "Owner",
                "RequestCpus",
                "JobUniverse",
                "RequestMemory",
            ]
            owner = f'(Owner=="{auser}") '
            # query for idle jobs
            jstat = f"&& (JobStatus=={htcondor.JobStatus.IDLE}) "
            bps1 = "&& (bps_run isnt Undefined) "
            bps2 = "&& (bps_job_label isnt Undefined) "
            # query for vanilla universe
            # JobUniverse constants are in htcondor C++
            # UNIVERSE = { 1: "Standard", ..., 5: "Vanilla", ... }
            juniv = "&& (JobUniverse==5) "
            large = f"&& (RequestMemory>{memoryLimit} || RequestCpus>{autoCPUs})"
            # The constraint determines that the jobs to be returned belong to
            # the current user (Owner) and are Idle vanilla universe jobs.
            full_constraint = f"{owner}{jstat}{bps1}{bps2}{juniv}{large}"
            if verbose:
                print("Find Large BPS Jobs:")
                print(f"full_constraint {full_constraint}")
            condorq_data = condor_q(
                constraint=full_constraint,
                schedds={schedd_name: scheddref},
                projection=projection,
            )

            if len(condorq_data) == 0:
                print("Auto: No Large BPS Jobs.")
                return

            if len(condorq_data) > 0:
                # Collect a list of the labels
                condorq_bps_large = condorq_data[schedd_name]
                job_labels = []
                if verbose:
                    print("Loop over list of Large Jobs")
                for jid in list(condorq_bps_large.keys()):
                    ajob = condorq_bps_large[jid]
                    if verbose:
                        print(jid)
                        print(ajob["bps_job_label"])
                    job_labels.append(ajob["bps_job_label"])

                #
                # Get a list of the unique labels
                #
                unique_labels = set(job_labels)

                #
                # Make a jobs dictionary with the unique labels as keys
                #
                if verbose:
                    print("Loop over unique job label list")
                label_dict = {}
                for job_label in unique_labels:
                    hash = hashlib.sha1(job_label.encode("UTF-8")).hexdigest()
                    shash = hash[:6]
                    empty_list = []
                    label_dict[job_label] = empty_list

                # Loop over the Large jobs and categorize
                for jid in list(condorq_bps_large.keys()):
                    ajob = condorq_bps_large[jid]
                    this_label = ajob["bps_job_label"]
                    this_list = label_dict[this_label]
                    this_list.append(ajob)

                for job_label in unique_labels:
                    if verbose:
                        print(f"\n{job_label}")
                    existingGlideinsIdle = 0
                    numberOfGlideinsReduced = 0
                    numberOfGlideins = 0
                    alist = label_dict[job_label]
                    thisMemory = alist[0]["RequestMemory"]
                    useCores = alist[0]["RequestCpus"]
                    if useCores < autoCPUs:
                        useCores = autoCPUs
                    hash = hashlib.sha1(job_label.encode("UTF-8")).hexdigest()
                    shash = hash[:6]
                    numberOfGlideins = len(alist)
                    jobname = f"{auser}_{shash}"
                    print(f"{job_label} {jobname} target {numberOfGlideins}")

                    # Do not submit squeue commands rapidly
                    time.sleep(2)
                    # Check Slurm queue Idle Glideins
                    batcmd = f"squeue --noheader --states=PD --name={jobname} | wc -l"
                    if verbose:
                        print("The squeue command is: %s " % batcmd)
                    try:
                        resultPD = subprocess.check_output(batcmd, shell=True)
                    except subprocess.CalledProcessError as e:
                        print(e.output)
                    existingGlideinsIdle = int(resultPD.decode("UTF-8"))
                    if verbose:
                        print(f"existingGlideinsIdle {jobname}")
                        print(existingGlideinsIdle)

                    numberOfGlideinsReduced = numberOfGlideins - existingGlideinsIdle
                    if verbose:
                        print(f"{job_label} reduced {numberOfGlideinsReduced}")

                    cpuopt = f"--cpus-per-task {useCores}"
                    memopt = f"--mem {thisMemory}"
                    jobopt = f"-J {jobname}"
                    cmd = f"sbatch {cpuopt} {memopt} {jobopt} {generatedSlurmFile}"
                    if verbose:
                        print(cmd)
                    for glide in range(0, numberOfGlideinsReduced):
                        print("Submitting Large glidein %s " % glide)
                        exitCode = self.runCommand(cmd, verbose)
                        if exitCode != 0:
                            print("error running %s" % cmd)
                            sys.exit(exitCode)

        except Exception as exc:
            raise type(exc)("Problem querying condor schedd for jobs") from None

        return

    def smallGlideinsFromJobPressure(self, generatedSlurmFile):
        """Determine and submit the small glideins needed from job pressure
        """

        verbose = self.isVerbose()
        maxNumberOfGlideins = self.getNodes()
        maxAllowedNumberOfGlideins = self.getAllowedAutoGlideins()
        if verbose:
            print(f"maxNumberOfGlideins {maxNumberOfGlideins}")
            print(f"maxAllowedNumberOfGlideins {maxAllowedNumberOfGlideins}")
        # The number of cores for the small glideins is capped at 8000
        # This corresponds to maxAllowedNumberOfGlideins = 500 16-core glideins
        if maxNumberOfGlideins > maxAllowedNumberOfGlideins:
            maxNumberOfGlideins = maxAllowedNumberOfGlideins
            print("Reducing Small Glidein limit due to threshold.")
        autoCPUs = self.getAutoCPUs()
        memoryPerCore = self.getMemoryPerCore()
        memoryLimit = autoCPUs * memoryPerCore
        auser = self.getUserName()

        # initialize counters
        totalCores = 0

        try:
            schedd_name = socket.getfqdn()
            coll = htcondor.Collector()
            schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd)
            scheddref = htcondor.Schedd(schedd_ad)
            # projection contains the job classads to be returned.
            # These include the cpu and memory profile of each job,
            # in the form of RequestCpus and RequestMemory
            projection = [
                "JobStatus",
                "Owner",
                "RequestCpus",
                "JobUniverse",
                "RequestMemory",
            ]
            owner = f'(Owner=="{auser}") '
            # query for idle jobs
            jstat = f"&& (JobStatus=={htcondor.JobStatus.IDLE}) "
            # query for vanilla universe
            # JobUniverse constants are in htcondor C++
            # UNIVERSE = { 1: "Standard", ..., 5: "Vanilla", ... }
            juniv = "&& (JobUniverse==5)"
            small = f"&& (RequestMemory<={memoryLimit} && RequestCpus<={autoCPUs})"
            # The constraint determines that the jobs to be returned belong to
            # the current user (Owner) and are Idle vanilla universe jobs.
            full_constraint = f"{owner}{jstat}{juniv}{small}"
            if verbose:
                print("\nQuerying condor queue for standard jobs")
                print(f"full_constraint {full_constraint}")
            condorq_data = condor_q(
                constraint=full_constraint,
                schedds={schedd_name: scheddref},
                projection=projection,
            )
            if len(condorq_data) > 0:
                print("smallGlideins: Fetched")
                condorq_bps = condorq_data[schedd_name]
                if verbose:
                    print(len(condorq_bps))
                    # This can be extremely large
                    # print(condorq_bps)
                # disassemble the dictionary of dictionaries
                for jid in list(condorq_bps.keys()):
                    job = condorq_bps[jid]
                    thisCores = job["RequestCpus"]
                    thisMemory = job["RequestMemory"]
                    totalCores = totalCores + thisCores
                    if verbose:
                        print(
                            f"smallGlideins: The key in the dictionary is  {jid}"
                        )
                        print(f"\tRequestCpus {thisCores}")
                        print(f"\tCurrent value of totalCores {totalCores}")
                    thisRatio = thisMemory / memoryPerCore
                    if thisRatio > thisCores:
                        if verbose:
                            print("\t\tNeed to Add More:")
                            print(f"\t\tRequestMemory is {thisMemory} ")
                            print(f"\t\tRatio to {memoryPerCore} MB is {thisRatio} ")
                        totalCores = totalCores + (thisRatio - thisCores)
                        if verbose:
                            print(f"\t\tCurrent value of totalCores {totalCores}")

            else:
                print("Auto: No small htcondor jobs detected.")
                return
        except Exception as exc:
            raise type(exc)("Problem querying condor schedd for jobs") from None

        print(f"smallGlideins: The final TotalCores is {totalCores}")

        # The number of Glideins needed to service the detected Idle jobs
        # is "numberOfGlideins"
        numberOfGlideins = math.ceil(totalCores / autoCPUs)
        print(
            f"smallGlideins: Number for detected jobs is {numberOfGlideins}"
        )

        # Check Slurm queue Running glideins
        jobname = f"glide_{auser}"
        existingGlideinsRunning = 0
        batcmd = f"squeue --noheader --states=R  --name={jobname} | wc -l"
        print("The squeue command is: %s " % batcmd)
        try:
            resultR = subprocess.check_output(batcmd, shell=True)
        except subprocess.CalledProcessError as e:
            print(e.output)
        existingGlideinsRunning = int(resultR.decode("UTF-8"))

        # Check Slurm queue Idle Glideins
        existingGlideinsIdle = 0
        batcmd = f"squeue --noheader --states=PD --name={jobname} | wc -l"
        print("The squeue command is: %s " % batcmd)
        try:
            resultPD = subprocess.check_output(batcmd, shell=True)
        except subprocess.CalledProcessError as e:
            print(e.output)
        existingGlideinsIdle = int(resultPD.decode("UTF-8"))

        print(
            f"smallGlideins: existingGlideinsRunning {existingGlideinsRunning}"
        )
        print(f"smallGlideins: existingGlideinsIdle {existingGlideinsIdle}")

        # The number of Glideins that we need to submit to service the detected 
        # Idle jobs is "numberOfGlideins" less the existing Idle glideins
        numberOfGlideinsReduced = numberOfGlideins - existingGlideinsIdle
        print(
            f"smallGlideins: Target Number to submit {numberOfGlideinsReduced}"
        )

        # The maximum number of Glideins that we can submit with 
        # the imposed threshold (maxNumberOfGlideins) 
        # is maxSubmitGlideins
        existingGlideins = existingGlideinsRunning + existingGlideinsIdle
        maxSubmitGlideins = maxNumberOfGlideins - existingGlideins
        print(f"smallGlideins: maxNumberOfGlideins {maxNumberOfGlideins}")
        print(f"smallGlideins: maxSubmitGlideins {maxSubmitGlideins}")

        # Reduce the number of Glideins to submit if threshold exceeded
        if numberOfGlideinsReduced > maxSubmitGlideins:
            numberOfGlideinsReduced = maxSubmitGlideins
            print("smallGlideins: Reducing due to threshold.")
        print(
            f"smallGlideins: Number of Glideins to submit is {numberOfGlideinsReduced}"
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
