#
# This file is part of daf_execute.
#
# Developed for the LSST Data Management System.
# LSST Data Management System
# Copyright 2008-2012 LSST Corporation.
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
# # This program is free software: you can redistribute it and/or modify
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

import os.path
import sys
import unittest

import lsst.utils.tests
from lsst.ctrl.execute.allocator import Allocator
from lsst.ctrl.execute.allocatorParser import AllocatorParser
from lsst.ctrl.execute.condorConfig import CondorConfig
from lsst.ctrl.execute.namedClassFactory import NamedClassFactory


def setup_module(module):
    lsst.utils.tests.init()


class SlurmPluginTest(lsst.utils.tests.TestCase):
    def test1(self):
        os.environ["SCRATCH"] = "/scratch/test1"
        sys.argv = [
            "test1",
            "test_platform",
            "-n",
            "64",
            "-c",
            "12",
            "-m",
            "00:30:00",
            "-q",
            "normal",
            "-O",
            "outlog",
            "-E",
            "errlog",
            "-v",
        ]

        al = AllocatorParser(sys.argv[0])
        args = al.getArgs()

        # create the plugin class
        schedulerName = "slurm"

        schedulerClass = NamedClassFactory.createClass("lsst.ctrl.execute." + schedulerName + "Plugin")

        p0 = os.path.join("tests/testfiles", "config_condorInfo.py")
        condor_info_file = p0

        self.config = CondorConfig()
        path = os.path.join("tests", "testfiles", "config_condor.py")
        self.config.load(path)
        self.assertEqual(self.config.platform.defaultRoot, "/usr")

        self.assertTrue(schedulerClass)
        self.assertTrue(args)
        self.assertTrue(self.config)
        self.assertTrue(condor_info_file)

        platform = "test1"
        configuration = CondorConfig()
        p1 = os.path.join("tests/testfiles", "config_execconfig.py")
        execConfigName = p1
        configuration.load(execConfigName)
        scheduler: Allocator = schedulerClass(platform, args, configuration, condor_info_file)
        self.assertTrue(scheduler)

        autocpus = scheduler.getAutoCPUs()
        minautocpus = scheduler.getMinAutoCPUs()
        cpus = scheduler.getCPUs()
        nodes = scheduler.getNodes()
        wallclock = scheduler.getWallClock()
        self.assertEqual(autocpus, 16)
        self.assertEqual(minautocpus, 15)
        self.assertEqual(cpus, 12)
        self.assertEqual(nodes, 64)
        self.assertEqual(wallclock, "00:30:00")


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
