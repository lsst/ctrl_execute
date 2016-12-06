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


class Allocator(object):
    """A class which consolidates allocation pex_config information with override
    information (obtained from the command line) and produces a PBS file using
    these values.

    Parameters
    ----------
    platform : `str`
        the name of the platform to execute on
    opts : `Config`
        Config object containing options
    condorInfoFileName : `str`
        Name of the file containing Config information
    """

    def __init__(self, platform, opts, configuration, condorInfoFileName):
        """Constructor
        @param platform: target platform for PBS submission
        @param opts: options to override
        """
        self.opts = opts
        self.defaults = {}
        self.configuration = configuration

        fileName = envString.resolve(condorInfoFileName)
        condorInfoConfig = CondorInfoConfig()
        condorInfoConfig.load(fileName)

        self.platform = platform

        # Look up the user's name and home directory in the
        # $HOME/.lsst/condor-info.py file
        # If the platform is lsst, and the user_name or user_home
        # is not in there, then default to user running this
        # command and the value of $HOME, respectively.
        user_name = None
        user_home = None
        for name in condorInfoConfig.platform:
            if name == self.platform:
                user_name = condorInfoConfig.platform[name].user.name
                user_home = condorInfoConfig.platform[name].user.home

        if self.platform == "lsst":
            if user_name is None:
                user_name = pwd.getpwuid(os.geteuid()).pw_name
            if user_home is None:
                user_home = os.getenv('HOME')

        if user_name is None:
            raise RuntimeError("error: %s does not specify user name for platform == %s" %
                               (condorInfoFileName, self.platform))
        if user_home is None:
            raise RuntimeError("error: %s does not specify user home for platform == %s" %
                               (condorInfoFileName, self.platform))

        self.defaults["USER_NAME"] = user_name
        self.defaults["USER_HOME"] = user_home

        self.commandLineDefaults = {}

        self.commandLineDefaults["NODE_COUNT"] = self.opts.nodeCount
        self.commandLineDefaults["SLOTS"] = self.opts.slots
        self.commandLineDefaults["WALL_CLOCK"] = self.opts.maximumWallClock

        self.commandLineDefaults["QUEUE"] = self.opts.queue
        if self.opts.email == "no":
            self.commandLineDefaults["EMAIL_NOTIFICATION"] = "#"

        self.load()

    def createNodeSetName(self):
        """Creates the next "node_set" name, using the remote user name and
        a stored sequence number.

        Returns
        -------
        nodeSetName : `str`
            the new node_set name
        """
        s = SeqFile("$HOME/.lsst/node-set.seq")
        n = s.nextSeq()
        nodeSetName = "%s_%d" % (self.defaults["USER_NAME"], n)
        return nodeSetName

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
        username = pwd.getpwuid(os.geteuid()).pw_name
        ident = "%s_%02d_%02d%02d_%02d%02d%02d" % (
            username, now.year, now.month, now.day, now.hour, now.minute, now.second)
        return ident

    def load(self):
        """Loads all values from configuration and command line overrides into
        data structures suitable for use by the TemplateWriter object.
        """
        tempLocalScratch = Template(self.configuration.platform.localScratch)
        self.defaults["LOCAL_SCRATCH"] = tempLocalScratch.substitute(USER_NAME=self.defaults["USER_NAME"])
        # print("localScratch-> %s" % self.defaults["LOCAL_SCRATCH"])
        self.defaults["SCHEDULER"] = self.configuration.platform.scheduler

    def loadAllocationConfig(self, name, suffix):
        """Loads all values from allocationConfig and command line overrides into
        data structures suitable for use by the TemplateWriter object.
        """
        resolvedName = envString.resolve(name)
        allocationConfig = AllocationConfig()
        if not os.path.exists(resolvedName):
            raise RuntimeError("%s was not found." % resolvedName)
        allocationConfig.load(resolvedName)

        self.defaults["QUEUE"] = allocationConfig.platform.queue
        self.defaults["EMAIL_NOTIFICATION"] = allocationConfig.platform.email
        self.defaults["HOST_NAME"] = allocationConfig.platform.loginHostName

        self.defaults["UTILITY_PATH"] = allocationConfig.platform.utilityPath

        if self.opts.glideinShutdown is None:
            self.defaults["GLIDEIN_SHUTDOWN"] = str(allocationConfig.platform.glideinShutdown)
        else:
            self.defaults["GLIDEIN_SHUTDOWN"] = str(self.opts.glideinShutdown)

        if self.opts.nodeSet is None:
            self.defaults["NODE_SET"] = self.createNodeSetName()
        else:
            self.defaults["NODE_SET"] = self.opts.nodeSet

        nodeSetName = self.defaults["NODE_SET"]

        if self.opts.outputLog is not None:
            self.defaults["OUTPUT_LOG"] = self.opts.outputLog
        else:
            self.defaults["OUTPUT_LOG"] = "%s.out" % nodeSetName

        if self.opts.errorLog is not None:
            self.defaults["ERROR_LOG"] = self.opts.errorLog
        else:
            self.defaults["ERROR_LOG"] = "%s.err" % nodeSetName

        # This is the TOTAL number of cores in the job, not just the total
        # of the cores you intend to use.   In other words, the total available
        # on a machine, times the number of machines.
        totalCoresPerNode = allocationConfig.platform.totalCoresPerNode
        self.commandLineDefaults["TOTAL_CORE_COUNT"] = self.opts.nodeCount * totalCoresPerNode

        self.uniqueIdentifier = self.createUniqueIdentifier()

        # write these pbs and config files to {LOCAL_DIR}/configs
        self.configDir = os.path.join(self.defaults["LOCAL_SCRATCH"], "configs")
        if not os.path.exists(self.configDir):
            os.makedirs(self.configDir)

        self.submitFileName = os.path.join(self.configDir, "alloc_%s.%s" % (self.uniqueIdentifier, suffix))

        self.condorConfigFileName = os.path.join(self.configDir, "condor_%s.config" % self.uniqueIdentifier)

        self.defaults["GENERATED_CONFIG"] = os.path.basename(self.condorConfigFileName)
        self.defaults["CONFIGURATION_ID"] = self.uniqueIdentifier
        return allocationConfig
        
    def createSubmitFile(self, inputFile):
        """Creates a PBS file using the file "input" as a Template

        Returns
        -------
        outfile : `str`
            The newly created file name
        """
        outfile = self.createFile(inputFile, self.submitFileName)
        if self.opts.verbose:
            print("wrote new PBS file to %s" % outfile)
        return outfile

    def createCondorConfigFile(self, input):
        """Creates a Condor config file using the file "input" as a Template

        Returns
        -------
        outfile : `str`
            The newly created file name
        """
        outfile = self.createFile(input, self.condorConfigFileName)
        if self.opts.verbose:
            print("wrote new condor_config file to %s" % outfile)
        return outfile

    def createFile(self, input, output):
        """Creates a new file, using "input" as a Template, and writes the
        new file to output.

        Returns
        -------
        outfile : `str`
            The newly created file name
        """
        resolvedInputName = envString.resolve(input)
        if self.opts.verbose:
            print("creating file using %s" % resolvedInputName)
        template = TemplateWriter()
        # Uses the associative arrays of "defaults" and "commandLineDefaults"
        # to write out the new file from the template.
        # The commandLineDefaults override values in "defaults"
        substitutes = self.defaults.copy()
        for key in self.commandLineDefaults:
            val = self.commandLineDefaults[key]
            if val is not None:
                substitutes[key] = self.commandLineDefaults[key]
        template.rewrite(resolvedInputName, output, substitutes)
        return output

    def isVerbose(self):
        """Status of the verbose flag
        @return True if the flag was set, False otherwise
        """
        return self.opts.verbose

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

    def getSlots(self):
        """Accessor for SLOTS
        @return the value of SLOTS
        """
        return self.getParameter("SLOTS")

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
        slots = self.getSlots()
        wallClock = self.getWallClock()
        nodeString = ""

        if int(nodes) > 1:
            nodeString = "s"
        print("%s node%s will be allocated on %s with %s slots per node and maximum time limit of %s" %
              (nodes, nodeString, self.platform, slots, wallClock))
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
        exitCode = (status & 0xff00) >> 8
        return exitCode
