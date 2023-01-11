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
import os.path
import pwd
import sys
from datetime import datetime
from string import Template

import eups
import lsst.utils
from lsst.ctrl.execute import envString

from .condorConfig import CondorConfig
from .condorInfoConfig import CondorInfoConfig
from .templateWriter import TemplateWriter


class Configurator:
    """A class which consolidates Condor pex_config information with override
    information (obtained from the command line) and produces Condor files
    using these values.
    """

    def __init__(self, opts, configFileName):
        """Constructor
        @param opts: options to override
        """
        self.opts = opts
        self.setup_using = None
        self.manager = None

        self.defaults = {}

        fileName = envString.resolve(configFileName)

        condorInfoConfig = CondorInfoConfig()
        condorInfoConfig.load(fileName)

        self.platform = self.opts.platform

        # Look up the user's name and home directory in the
        # $HOME//.lsst/condor-info.py file. If the platform
        # is lsst, and the user_name or user_home is not in
        # there, then default to user running this comman
        # and the value of $HOME, respectively.
        user_name = None
        user_home = None
        for name in list(condorInfoConfig.platform.keys()):
            if name == self.platform:
                user_name = condorInfoConfig.platform[name].user.name
                user_home = condorInfoConfig.platform[name].user.home

        # If we're on the lsst platform and the condorInfoConfig didn't
        # have an entry for lsst user name and home, set to reasonable values
        # These really do need to be set for all the other platforms, since
        # while the user name may be the same, it's unlikely the home
        # directory will be.
        if self.platform == "lsst":
            if user_name is None:
                user_name = pwd.getpwuid(os.geteuid()).pw_name
            if user_home is None:
                user_home = os.getenv("HOME")

        if user_name is None:
            raise RuntimeError(
                "error: %s does not specify user name for platform %s"
                % (configFileName, self.platform)
            )
        if user_home is None:
            raise RuntimeError(
                "error: %s does not specify user home for platform %s"
                % (configFileName, self.platform)
            )

        self.commandLineDefaults = {}
        self.commandLineDefaults["USER_NAME"] = user_name
        self.commandLineDefaults["USER_HOME"] = user_home

        self.commandLineDefaults["DEFAULT_ROOT"] = self.opts.defaultRoot
        self.commandLineDefaults["LOCAL_SCRATCH"] = self.opts.localScratch
        self.commandLineDefaults["DATA_DIRECTORY"] = self.opts.dataDirectory
        self.commandLineDefaults["IDS_PER_JOB"] = self.opts.idsPerJob
        if self.opts.nodeSet is None:
            self.commandLineDefaults["NODE_SET"] = ""
        else:
            self.commandLineDefaults["NODE_SET"] = self.opts.nodeSet
        if self.opts.inputDataFile is None:
            self.commandLineDefaults["INPUT_DATA_FILE"] = None
        else:
            self.commandLineDefaults["INPUT_DATA_FILE"] = os.path.abspath(
                self.opts.inputDataFile
            )
        self.commandLineDefaults["FILE_SYSTEM_DOMAIN"] = self.opts.fileSystemDomain
        self.commandLineDefaults["EUPS_PATH"] = self.opts.eupsPath

        # override user name, if given
        if self.opts.user_name is not None:
            self.commandLineDefaults["USER_NAME"] = self.opts.user_name

        # override user home, if given
        if self.opts.user_home is not None:
            self.commandLineDefaults["USER_HOME"] = self.opts.user_home

        if self.opts.runid is not None:
            self.runid = self.opts.runid
        else:
            self.runid = self.createRunId()

        if self.opts.dagscript is not None:
            self.commandLineDefaults["DAGSCRIPT"] = self.opts.dagscript

        if self.opts.inputscript is not None:
            self.commandLineDefaults["INPUTSCRIPT"] = self.opts.inputscript

        if self.opts.platformConfig is not None:
            self.commandLineDefaults["PLATFORM_CONFIG"] = self.opts.platformConfig

        self.commandLineDefaults["COMMAND"] = self.opts.command
        if self.commandLineDefaults["INPUT_DATA_FILE"] is not None:
            self.commandLineDefaults["COMMAND"] = (
                self.commandLineDefaults["COMMAND"] + " ${id_option}"
            )

    def getGenericConfigFileName(self):
        """Retrieve a ctrl_execute orca config template, depending
        on which target environment jobs will be running on.
        @return the name of the orca config template
        """
        executePkgDir = lsst.utils.getPackageDir("ctrl_execute")

        name = "config_with_%s.py.template" % self.setup_using
        genericConfigName = os.path.join(
            executePkgDir, "etc", "templates", self.manager, name
        )
        if os.path.exists(genericConfigName):
            return genericConfigName
        raise RuntimeError(
            "File %s not found; check etc/templates." % genericConfigName
        )

    def createRunId(self):
        """create a unique runid
        @return runid
        """
        # runid is in the form of <login>_YYYY_MMDD_HHMMSS
        now = datetime.now()
        username = pwd.getpwuid(os.geteuid()).pw_name
        runid = "%s_%02d_%02d%02d_%02d%02d%02d" % (
            username,
            now.year,
            now.month,
            now.day,
            now.hour,
            now.minute,
            now.second,
        )
        self.runid = runid
        return runid

    def getSetupPackages(self):
        """Create a string of all the currently setup LSST software packages,
        excluding any locally setup packages (LOCAL:).  Also include any
        packages specified on the comand line. This string will be
        used to substitute within a preJob Template to create an LSST stack
        environment that jobs will use.
        @return string containing all setup commands, one per line.
        """
        e = eups.Eups()
        setupProducts = e.getSetupProducts()
        a = ""

        # create a new list will all products and versions
        allProducts = {}
        for i in setupProducts:
            allProducts[i.name] = i.version

        # replace any existing products that we saw on the command line, adding
        # them if they're not already there.
        if self.opts.setup is not None:
            for i, pkg in enumerate(self.opts.setup):
                name = pkg[0]
                version = pkg[1]
                print("name = %s, version = %s" % (name, version))
                allProducts[name] = version

        # write out all products, except those that are setup locally.
        for name in allProducts:
            version = allProducts[name]
            if self.platform == "lsst":
                a = a + "setup -j %s %s\\n\\\n" % (name, version)
            else:
                if not version.startswith("LOCAL:"):
                    a = a + "setup -j %s %s\\n\\\n" % (name, version)
        return a

    def load(self, name):
        """Loads all values from configuration and command line overrides into
        data structures suitable for use by the TemplateWriter object.
        """
        resolvedName = envString.resolve(name)
        configuration = CondorConfig()
        configuration.load(resolvedName)
        self.defaults = {}

        if configuration.platform.nodeSetRequired and self.opts.nodeSet is None:
            print("error: nodeset parameter required by this platform")
            sys.exit(10)

        tempDefaultRoot = Template(configuration.platform.defaultRoot)
        self.defaults["DEFAULT_ROOT"] = tempDefaultRoot.substitute(
            USER_NAME=self.commandLineDefaults["USER_NAME"]
        )

        tempLocalScratch = Template(configuration.platform.localScratch)
        self.defaults["LOCAL_SCRATCH"] = tempLocalScratch.substitute(
            USER_NAME=self.commandLineDefaults["USER_NAME"]
        )
        self.defaults["IDS_PER_JOB"] = configuration.platform.idsPerJob
        self.defaults["DATA_DIRECTORY"] = envString.resolve(
            configuration.platform.dataDirectory
        )
        self.defaults["FILE_SYSTEM_DOMAIN"] = configuration.platform.fileSystemDomain
        self.defaults["EUPS_PATH"] = configuration.platform.eupsPath
        self.defaults[
            "MANAGER_SOFTWARE_HOME"
        ] = configuration.platform.manager_software_home

        platform_dir = lsst.utils.getPackageDir("ctrl_platform_" + self.opts.platform)
        self.defaults["PLATFORM_DIR"] = platform_dir
        self.manager = configuration.platform.manager
        self.setup_using = configuration.platform.setup_using

    def createConfiguration(self, input):
        """creates a new Orca configuration file
        @param input: template to use for value substitution
        @return the newly created Orca configuration file
        """
        resolvedInputName = envString.resolve(input)
        if self.opts.verbose:
            print("creating configuration using ", resolvedInputName)
        template = TemplateWriter()
        substitutes = self.defaults.copy()
        for key in self.commandLineDefaults:
            val = self.commandLineDefaults[key]
            if val is not None:
                substitutes[key] = self.commandLineDefaults[key]

        substitutes["CTRL_EXECUTE_SETUP_PACKAGES"] = self.getSetupPackages()

        configDir = os.path.join(substitutes["LOCAL_SCRATCH"], "configs")
        if not os.path.exists(configDir):
            os.mkdir(configDir)
        self.outputFileName = os.path.join(configDir, "%s.config" % (self.runid))
        if self.opts.verbose:
            print("writing new configuration to ", self.outputFileName)
        template.rewrite(resolvedInputName, self.outputFileName, substitutes)
        return self.outputFileName

    def isVerbose(self):
        """Checks to see if verbose flag was set.
        @return value of verbose flag if it was set on the command line
        """
        return self.opts.verbose

    def getParameter(self, value):
        """Accessor for generic value
        @return None if value is not set.  Otherwise, use the comand line
        override (if set), or the default Config value
        """
        if value in self.commandLineDefaults:
            return self.commandLineDefaults[value]
        if value in self.defaults:
            return self.defaults[value]
        return None

    def getRunId(self):
        """Accessor for run id
        @return the value of the run id
        """
        return self.runid
