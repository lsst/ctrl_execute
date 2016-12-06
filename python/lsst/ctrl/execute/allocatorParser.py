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


class AllocatorParser(object):
    """An argument parser for node allocation requests.

    Parameters
    ----------
    basename : `str`
        The name used to identify the running program
    """

    def __init__(self, basename):
        """Construct an AllocatorParser
        @param argv: list containing the command line arguments
        @return: the parser options and remaining arguments
        """

        self.defaults = {}

        self.args = []

        self.args = self.parseArgs(basename)

    def parseArgs(self, basename):
        """Parse command line, and test for required arguments

        Parameters
        ----------
        argv: `list`
            list of strings containing the command line arguments

        Returns
        -------
        The parser options and remaining arguments
        """

        parser = argparse.ArgumentParser(prog=basename)
        parser.add_argument("platform", help="node allocation platform")
        parser.add_argument("-n", "--node-count", action="store", default=None,
                            dest="nodeCount", help="number of nodes to use", type=int, required=True)
        parser.add_argument("-s", "--slots", action="store", default=None, dest="slots",
                            help="slots per node", type=int, required=True)

        parser.add_argument("-m", "--maximum-wall-clock", action="store", dest="maximumWallClock",
                            default=None, help="maximum wall clock time", type=str, required=True)
        parser.add_argument("-N", "--node-set", action="store",
                            dest="nodeSet", default=None, help="node set name")
        parser.add_argument("-q", "--queue", action="store", dest="queue",
                            default=None, help="pbs queue name")
        parser.add_argument("-e", "--email", action="store_true", dest="email",
                            default=None, help="email notification flag")
        parser.add_argument("-O", "--output-log", action="store", dest="outputLog",
                            default=None, help="Output log filename")
        parser.add_argument("-E", "--error-log", action="store", dest="errorLog",
                            default=None, help="Error log filename")
        parser.add_argument("-g", "--glidein-shutdown", action="store", dest="glideinShutdown",
                            type=int, default=None, help="glide-in inactivity shutdown time in seconds")
        parser.add_argument("-v", "--verbose", action="store_true", dest="verbose", help="verbose")

        self.args = parser.parse_args()

        return self.args

    def getArgs(self):
        """Accessor method to get arguments left after standard parsed options
        are initialized.

        Returns
        -------
        args: `list`
            remaining command line arguments
        """
        return self.args

    def getPlatform(self):
        """Accessor method to retrieve the "platform" that was specified on
        the command line.

        Returns
        -------
        platform: `str`
            the name of the "platform"
        """
        return self.args.platform
