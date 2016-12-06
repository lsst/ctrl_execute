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

from builtins import object
import argparse


class RunOrcaParser(object):
    """An argument parser for the orchestration config file generation
    and execution
    """

    def __init__(self, basename):
        """Construct a RunOrcaParser
        @param argv: list containing the command line arguments
        @return: the parser options and remaining arguments
        """
        self.defaults = {}

        self.args = []

        self.args = self.parseArgs(basename)

    def parseArgs(self, basename):
        """Parse command line, and test for required arguments
        @param argv: list containing the command line arguments
        @return: the parser options and remaining arguments
        """

        parser = argparse.ArgumentParser(prog=basename)
        parser.add_argument("-p", "--platform", action="store", dest="platform",
                            default=None, help="platform", required=True)
        parser.add_argument("-c", "--command", action="store", dest="command",
                            default=None, help="command", required=True)
        parser.add_argument("-i", "--id-file", action="store", dest="inputDataFile",
                            default=None, help="list of ids", required=True)
        parser.add_argument("-e", "--eups-path", action="store", dest="eupsPath",
                            default=None, help="eups path", required=True)
        parser.add_argument("-N", "--node-set", action="store",
                            default=None, dest="nodeSet",
                            help="name of collection of nodes to use (required by some platforms)",
                            required=False)
        parser.add_argument("-n", "--ids-per-job", action="store",
                            default=None, dest="idsPerJob", help="ids per job")
        parser.add_argument("-r", "--default-root", action="store", dest="defaultRoot",
                            default=None, help="remote working directory for Condor")
        parser.add_argument("-l", "--local-scratch", action="store", dest="localScratch",
                            default=None, help="local staging directory for Condor")
        parser.add_argument("-d", "--data-directory", action="store", dest="dataDirectory",
                            default=None, help="where the data is located")

        parser.add_argument("-F", "--file-system-domain", action="store", dest="fileSystemDomain",
                            default=None, help="file system domain")
        parser.add_argument("-u", "--user-name", action="store", dest="user_name", default=None, help="user")
        parser.add_argument("-H", "--user-home", action="store", dest="user_home", default=None, help="home")
        parser.add_argument("-R", "--run-id", action="store", dest="runid", default=None, help="run id")
        parser.add_argument("-v", "--verbose", action="store_true", dest="verbose",
                            default=False, help="verbose")
        parser.add_argument("-s", "--setup", action="append", nargs=2, help="setup")

        args = parser.parse_args()

        return args

    def getArgs(self):
        """Accessor method to get options set on initialization
        @return opts: command line options
        """
        return self.args
