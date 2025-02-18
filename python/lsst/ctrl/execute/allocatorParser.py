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

import argparse


class AllocatorParser:
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
        self.args = self.parseArgs(basename)

    def parseArgs(self, basename) -> argparse.Namespace:
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
        parser.add_argument("platform", type=str, default="s3df", help="node allocation platform")
        parser.add_argument(
            "--auto",
            action="store_true",
            dest="auto",
            help="use automatic detection of jobs to determine glide-ins",
        )
        parser.add_argument(
            "-n",
            "--node-count",
            action="store",
            default=None,
            dest="nodeCount",
            help="number of glideins to submit; these are chunks of a node, size the number of cores/cpus",
            type=int,
            required=True,
        )
        parser.add_argument(
            "-c",
            "--cpus",
            action="store",
            default=16,
            dest="cpus",
            help="cores / cpus per glidein",
            type=int,
            required=False,
        )
        parser.add_argument(
            "-a",
            "--account",
            action="store",
            default="rubin:developers",
            dest="account",
            help="Slurm account for glidein job",
            type=str,
        )
        parser.add_argument(
            "--collector",
            action="store",
            default=None,
            dest="collector",
            help="machine name of nondefault htcondor collector",
            type=str,
            required=False,
        )
        parser.add_argument(
            "--collector-port",
            action="store",
            default=9618,
            dest="collectorport",
            help="port used for nondefault htcondor collector",
            type=int,
            required=False,
        )
        parser.add_argument(
            "-s",
            "--qos",
            action="store",
            default=None,
            dest="qos",
            help="Slurm qos for glidein job",
            type=str,
        )
        parser.add_argument(
            "-m",
            "--maximum-wall-clock",
            action="store",
            dest="maximumWallClock",
            default=None,
            help="maximum wall clock time; e.g., 3600, 10:00:00, 6-00:00:00, etc",
            type=str,
            required=True,
        )
        parser.add_argument(
            "-q",
            "--queue",
            action="store",
            dest="queue",
            default="roma,milano",
            help="queue / partition  name",
        )
        parser.add_argument(
            "-O",
            "--output-log",
            action="store",
            dest="outputLog",
            default=None,
            help="Output log filename; this option for PBS, unused with Slurm",
        )
        parser.add_argument(
            "-E",
            "--error-log",
            action="store",
            dest="errorLog",
            default=None,
            help="Error log filename; this option for PBS, unused with Slurm",
        )
        parser.add_argument(
            "-g",
            "--glidein-shutdown",
            action="store",
            dest="glideinShutdown",
            type=int,
            default=None,
            help="glide-in inactivity shutdown time in seconds",
        )
        parser.add_argument(
            "--openfiles",
            action="store",
            dest="openfiles",
            type=int,
            default=20480,
            help="set the limit on number of open files (fd) per process",
        )
        parser.add_argument(
            "-p",
            "--pack",
            action="store_true",
            dest="packnodes",
            help="encourage nodes to pack jobs rather than spread",
        )
        parser.add_argument("-v", "--verbose", action="store_true", dest="verbose", help="verbose")
        parser.add_argument(
            "-r",
            "--reservation",
            action="store",
            dest="reservation",
            default=None,
            help="target a particular Slurm reservation",
        )
        parser.add_argument(
            "-d",
            "--dynamic",
            const="__default__",
            nargs="?",
            action="store",
            dest="dynamic",
            type=str,
            default="__default__",
            help="configure to use dynamic/partitionable slot; legacy option: this is always enabled now",
        )

        self.args = parser.parse_args()

        return self.args

    def getArgs(self):
        """Accessor method to get arguments left after standard parsed options
        are initialized.

        Returns
        -------
        args: `argparse.Namespace`
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
