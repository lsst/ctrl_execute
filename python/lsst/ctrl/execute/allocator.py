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
import pwd
import sys
from datetime import datetime
from string import Template

from lsst.ctrl.execute.allocationConfig import AllocationConfig
from lsst.ctrl.execute.condorInfoConfig import CondorInfoConfig
from lsst.ctrl.execute.templateWriter import TemplateWriter
from lsst.resources import ResourcePath, ResourcePathExpression

_LOG = logging.getLogger(__name__)


class Allocator:
    """A class which consolidates allocation pex_config information with
    override information (obtained from the command line) and produces a
    PBS file using these values.

    Parameters
    ----------
    platform : `str`
        the name of the platform to execute on
    opts : `Config`
        Config object containing options
    condorInfoFileName : `lsst.resources.ResourcePathExpression`
        Name of the file containing Config information

    Raises
    ------
    TypeError
        If the condorInfoFileName is the wrong type.
    """

    def __init__(
        self,
        platform: str,
        opts,
        configuration,
        condorInfoFileName: ResourcePathExpression,
    ):
        """Constructor
        @param platform: target platform for PBS submission
        @param opts: options to override
        """
        self.opts = opts
        self.defaults = {}
        self.configuration = configuration

        condorInfoConfig = CondorInfoConfig()
        condorInfoConfig.loadFromStream(ResourcePath(condorInfoFileName).read())

        self.platform = platform

        # Look up the user's name and home and scratch directory in the
        # $HOME/.lsst/condor-info.py file
        user_name = None
        user_home = None
        user_scratch = None
        for name in condorInfoConfig.platform:
            if name == self.platform:
                user_name = condorInfoConfig.platform[name].user.name
                user_home = condorInfoConfig.platform[name].user.home
                user_scratch = condorInfoConfig.platform[name].user.scratch
                if user_scratch is None and "SCRATCH" in os.environ:
                    user_scratch = os.environ["SCRATCH"]
        if user_name is None:
            raise RuntimeError(
                f"error: {condorInfoFileName} does not specify user name for platform == {self.platform}"
            )
        if user_home is None:
            raise RuntimeError(
                f"error: {condorInfoFileName} does not specify user home for platform == {self.platform}"
            )
        if user_scratch is None:
            raise RuntimeError(
                f"error: {condorInfoFileName} does not specify user scratch for platform == {self.platform}"
            )
        self.defaults["USER_NAME"] = user_name
        self.defaults["USER_HOME"] = user_home
        self.defaults["USER_SCRATCH"] = user_scratch
        self.commandLineDefaults = {}
        self.commandLineDefaults["NODE_COUNT"] = self.opts.nodeCount
        self.commandLineDefaults["COLLECTOR"] = self.opts.collector
        self.commandLineDefaults["CPORT"] = self.opts.collectorport
        self.commandLineDefaults["CPUS"] = self.opts.cpus
        self.commandLineDefaults["WALL_CLOCK"] = self.opts.maximumWallClock
        self.commandLineDefaults["ACCOUNT"] = self.opts.account
        self.commandLineDefaults["MEMPERCORE"] = 4096
        self.commandLineDefaults["ALLOWEDAUTO"] = 500
        self.commandLineDefaults["AUTOCPUS"] = 16
        self.commandLineDefaults["QUEUE"] = self.opts.queue
        self.load()

    def createUniqueIdentifier(self):
        """Creates a unique file identifier, based on the user's name
        and the time at which this method is invoked.

        Returns
        -------
        ident : `str`
            the new identifier
        """
        # This naming scheme follows the conventions used for creating
        # RUNID names.  We've found this allows these files to be more
        # easily located and shared with other users when debugging
        # The tempfile.mkstemp method restricts the file to only the user,
        # and does not guarantee a file name can that easily be identified.
        now = datetime.now()
        self.defaults["DATE_STRING"] = f"{now.year:02d}_{now.month:02d}{now.day:02d}"
        username = pwd.getpwuid(os.geteuid()).pw_name
        ident = (
            f"{username}_{now.year:02d}_{now.month:02d}{now.day:02d}_"
            f"{now.hour:02d}{now.minute:02d}{now.second:02d}"
        )
        return ident

    def load(self):
        """Loads all values from configuration and command line overrides into
        data structures suitable for use by the TemplateWriter object.
        """
        tempLocalScratch = Template(self.configuration.platform.localScratch)
        self.defaults["LOCAL_SCRATCH"] = tempLocalScratch.substitute(
            USER_SCRATCH=self.defaults["USER_SCRATCH"]
        )
        self.defaults["SCHEDULER"] = self.configuration.platform.scheduler

    def loadAllocationConfig(self, name: ResourcePathExpression, suffix):
        """Loads all values from allocationConfig and command line overrides
        into data structures suitable for use by the TemplateWriter object.
        """
        if not (name_ := ResourcePath(name)).exists():
            raise RuntimeError(f"{name_} was not found.")
        allocationConfig = AllocationConfig()
        allocationConfig.loadFromStream(name_.read())

        self.defaults["QUEUE"] = allocationConfig.platform.queue
        self.defaults["EMAIL_NOTIFICATION"] = allocationConfig.platform.email
        self.defaults["HOST_NAME"] = allocationConfig.platform.loginHostName

        self.defaults["UTILITY_PATH"] = allocationConfig.platform.utilityPath

        if self.opts.glideinShutdown is None:
            self.defaults["GLIDEIN_SHUTDOWN"] = str(allocationConfig.platform.glideinShutdown)
        else:
            self.defaults["GLIDEIN_SHUTDOWN"] = str(self.opts.glideinShutdown)

        if self.opts.outputLog is not None:
            self.defaults["OUTPUT_LOG"] = self.opts.outputLog
        else:
            self.defaults["OUTPUT_LOG"] = "glide.out"

        if self.opts.errorLog is not None:
            self.defaults["ERROR_LOG"] = self.opts.errorLog
        else:
            self.defaults["ERROR_LOG"] = "glide.err"

        # This is the TOTAL number of cores in the job, not just the total
        # of the cores you intend to use.   In other words, the total available
        # on a machine, times the number of machines.
        totalCoresPerNode = allocationConfig.platform.totalCoresPerNode
        self.commandLineDefaults["TOTAL_CORE_COUNT"] = self.opts.nodeCount * totalCoresPerNode

        self.uniqueIdentifier = self.createUniqueIdentifier()

        # write these pbs and config files to {LOCAL_DIR}/configs
        self.configDir = os.path.join(
            self.defaults["LOCAL_SCRATCH"],
            self.defaults["DATE_STRING"],
            self.uniqueIdentifier,
            "configs",
        )

        self.submitFileName = os.path.join(self.configDir, f"alloc_{self.uniqueIdentifier}.{suffix}")

        self.condorConfigFileName = os.path.join(self.configDir, f"condor_{self.uniqueIdentifier}.config")

        self.defaults["GENERATED_CONFIG"] = os.path.basename(self.condorConfigFileName)
        self.defaults["CONFIGURATION_ID"] = self.uniqueIdentifier
        return allocationConfig

    def createSubmitFile(self, inputFile):
        """Creates a batch submit file using the file "input" as a Template

        Returns
        -------
        outfile : `str`
            The newly created file name
        """
        if not os.path.exists(self.configDir):
            os.makedirs(self.configDir)
        outfile = self.createFile(inputFile, self.submitFileName)
        _LOG.debug("Wrote new Slurm submit file to %s", outfile)
        return outfile

    def createCondorConfigFile(self, input):
        """Creates a Condor config file using the file "input" as a Template

        Returns
        -------
        outfile : `str`
            The newly created file name
        """
        outfile = self.createFile(input, self.condorConfigFileName)
        _LOG.debug("Wrote new condor configuration file to %s", outfile)
        return outfile

    def createFile(self, input: ResourcePathExpression, output: ResourcePathExpression):
        """Creates a new file, using "input" as a Template, and writes the
        new file to output.

        Returns
        -------
        outfile : `str`
            The newly created file name
        """
        _LOG.debug("Creating file from template using %s", input)
        template = TemplateWriter()
        # Uses the associative arrays of "defaults" and "commandLineDefaults"
        # to write out the new file from the template.
        # The commandLineDefaults override values in "defaults"
        substitutes = self.defaults.copy()
        for key in self.commandLineDefaults:
            val = self.commandLineDefaults[key]
            if val is not None:
                substitutes[key] = self.commandLineDefaults[key]
        template.rewrite(input, output, substitutes)
        return output

    def isVerbose(self):
        """Status of the verbose flag
        @return True if the flag was set, False otherwise
        """
        return self.opts.verbose

    def isAuto(self):
        """Status of the auto flag
        @return True if the flag was set, False otherwise
        """
        return self.opts.auto

    def getUserName(self):
        """Accessor for USER_NAME
        @return the value of USER_NAME
        """
        return self.getParameter("USER_NAME")

    def getUserHome(self):
        """Accessor for USER_HOME
        @return the value of USER_HOME
        """
        return self.getParameter("USER_HOME")

    def getUserScratch(self):
        """Accessor for USER_SCRATCH
        @return the value of USER_SCRATCH
        """
        return self.getParameter("USER_SCRATCH")

    def getHostName(self):
        """Accessor for HOST_NAME
        @return the value of HOST_NAME
        """
        return self.getParameter("HOST_NAME")

    def getUtilityPath(self):
        """Accessor for UTILITY_PATH
        @return the value of UTILITY_PATH
        """
        return self.getParameter("UTILITY_PATH")

    def getScratchDirectory(self):
        """Accessor for SCRATCH_DIR
        @return the value of SCRATCH_DIR
        """
        return self.getParameter("SCRATCH_DIR")

    def getLocalScratchDirectory(self):
        """Accessor for LOCAL_SCRATCH
        @return the value of LOCAL_SCRATCH
        """
        return self.getParameter("LOCAL_SCRATCH")

    def getNodeSetName(self):
        """Accessor for NODE_SET
        @return the value of NODE_SET
        """
        return self.getParameter("NODE_SET")

    def getNodes(self):
        """Accessor for NODE_COUNT
        @return the value of NODE_COUNT
        """
        return self.getParameter("NODE_COUNT")

    def getMemoryPerCore(self):
        """Accessor for MemoryPerCore
        @return the value of MemoryPerCore
        """
        return self.getParameter("MEMPERCORE")

    def getAllowedAutoGlideins(self):
        """Accessor for AllowedAutoGlideins
        @return the value of AllowedAuto
        """
        return self.getParameter("ALLOWEDAUTO")

    def getQOS(self):
        """Accessor for QOS
        @return the value of QOS
        """
        return self.getParameter("QOS")

    def getCPUs(self):
        """Accessor for CPUS
        @return the value of CPUS
        """
        return self.getParameter("CPUS")

    def getAutoCPUs(self):
        """Size of standard glideins for allocateNodes auto
        @return the value of autoCPUs
        """
        return self.getParameter("AUTOCPUS")

    def getWallClock(self):
        """Accessor for WALL_CLOCK
        @return the value of WALL_CLOCK
        """
        return self.getParameter("WALL_CLOCK")

    def getScheduler(self):
        """Accessor for SCHEDULER
        @return the value of SCHEDULER
        """
        return self.getParameter("SCHEDULER")

    def getReservation(self):
        """Accessor for RESERVATION
        @return the value of RESERVATION
        """
        return self.getParameter("RESERVATION")

    def getParameter(self, value):
        """Accessor for generic value
        @return None if value is not set.  Otherwise, use the command line
        override (if set), or the default Config value
        """
        if value in self.commandLineDefaults:
            return self.commandLineDefaults[value]
        if value in self.defaults:
            return self.defaults[value]
        return None

    def printNodeSetInfo(self):
        nodes = self.getNodes()
        cpus = self.getCPUs()
        wallClock = self.getWallClock()
        nodeString = ""

        if int(nodes) > 1:
            nodeString = "s"
        if self.opts.dynamic is None:
            print(
                f"{nodes} glidein{nodeString} will be allocated on "
                f"{self.platform} using default dynamic slots configuration."
            )
            print(f"There will be {cpus} cores per glidein and a maximum time limit of {wallClock}")
        elif self.opts.dynamic == "__default__":
            print(
                f"{nodes} glidein{nodeString} will be allocated on {self.platform} "
                "using default dynamic slots configuration."
            )
            print(f"There will be {cpus} cores per glidein and a maximum time limit of {wallClock}")
        else:
            print(
                f"{nodes} node{nodeString} will be allocated on {self.platform} "
                f"using dynamic slot block specified in '{self.opts.dynamic}'"
            )
            print(f"There will be  {cpus} cores per node and maximum time limit of {wallClock}")
        print("Node set name:")
        print(self.getNodeSetName())

    def runCommand(self, cmd, verbose):
        cmd_split = cmd.split()
        pid = os.fork()
        if not pid:
            # Methods of file transfer and login may
            # produce different output, depending on how
            # the "gsi" utilities are used.  The user can
            # either use grid proxies or ssh, and gsiscp/gsissh
            # does the right thing.  Since the output will be
            # different in either case anything potentially parsing this
            # output (like drpRun), would have to go through extra
            # steps to deal with this output, and which ultimately
            # end up not being useful.  So we optinally close the i/o output
            # of the executing command down.
            #
            # stdin/stdio/stderr is treated specially
            # by python, so we have to close down
            # both the python objects and the
            # underlying c implementations
            if not verbose:
                # close python i/o
                sys.stdin.close()
                sys.stdout.close()
                sys.stderr.close()
                # close C's i/o
                os.close(0)
                os.close(1)
                os.close(2)
            os.execvp(cmd_split[0], cmd_split)
        pid, status = os.wait()
        # high order bits are status, low order bits are signal.
        exitCode = (status & 0xFF00) >> 8
        return exitCode

    def submit(self):
        """Submit the glidein jobs to the Batch system."""
        raise NotImplementedError
