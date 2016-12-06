#!/usr/bin/env python
#
# LSST Data Management System
# Copyright 2008-2012 LSST Corporation.
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
import unittest
import os.path
from lsst.ctrl.execute.condorConfig import CondorConfig
import lsst.utils.tests


def setup_module(module):
    lsst.utils.tests.init()


class TestCondorConfig(lsst.utils.tests.TestCase):
    def setUp(self):
        self.config = CondorConfig()

    def test1(self):
        self.assertIsNone(self.config.platform.defaultRoot)
        self.assertIsNone(self.config.platform.localScratch)
        self.assertIsNone(self.config.platform.dataDirectory)
        self.assertIsNone(self.config.platform.fileSystemDomain)
        self.assertIsNone(self.config.platform.eupsPath)
        self.assertIsNone(self.config.platform.defaultRoot)
        self.assertIsNone(self.config.platform.setup_using)
        self.assertIsNone(self.config.platform.scheduler)

    def test2(self):
        path = os.path.join("tests", "testfiles", "config_condor.py")
        self.config.load(path)

        self.assertEqual(self.config.platform.defaultRoot, "/usr")
        self.assertEqual(self.config.platform.localScratch, "./tests/condor_scratch")
        self.assertEqual(self.config.platform.dataDirectory, "/tmp/data")
        self.assertEqual(self.config.platform.fileSystemDomain, "lsstcorp.org")
        self.assertEqual(self.config.platform.eupsPath, "/var/tmp")
        self.assertEqual(self.config.platform.scheduler, "pbs")
        self.assertIsNone(self.config.platform.setup_using)

    def test3(self):
        path = os.path.join("tests", "testfiles", "config_condor_getenv.py")
        self.config.load(path)

        self.assertEqual(self.config.platform.defaultRoot, "/usr")
        self.assertEqual(self.config.platform.localScratch, "./tests/condor_scratch")
        self.assertEqual(self.config.platform.dataDirectory, "/tmp/data")
        self.assertEqual(self.config.platform.fileSystemDomain, "lsstcorp.org")
        self.assertEqual(self.config.platform.eupsPath, "/var/tmp")
        self.assertEqual(self.config.platform.scheduler, "pbs")
        self.assertEqual(self.config.platform.setup_using, "getenv")

    def test4(self):
        path = os.path.join("tests", "testfiles", "config_condor_setups.py")
        self.config.load(path)

        self.assertEqual(self.config.platform.defaultRoot, "/usr")
        self.assertEqual(self.config.platform.localScratch, "./tests/condor_scratch")
        self.assertEqual(self.config.platform.dataDirectory, "/tmp/data")
        self.assertEqual(self.config.platform.fileSystemDomain, "lsstcorp.org")
        self.assertEqual(self.config.platform.eupsPath, "/var/tmp")
        self.assertEqual(self.config.platform.scheduler, "pbs")
        self.assertEqual(self.config.platform.setup_using, "setups")

    def test3(self):
        path = os.path.join("tests", "testfiles", "config_condor_slurm.py")
        self.config.load(path)

        self.assertEqual(self.config.platform.defaultRoot, "/usr")
        self.assertEqual(self.config.platform.localScratch, "./tests/condor_scratch")
        self.assertEqual(self.config.platform.dataDirectory, "/tmp/data")
        self.assertEqual(self.config.platform.fileSystemDomain, "lsstcorp.org")
        self.assertEqual(self.config.platform.eupsPath, "/var/tmp")
        self.assertEqual(self.config.platform.scheduler, "slurm")
        self.assertEqual(self.config.platform.setup_using, "getenv")

class CondorConfigMemoryTest(lsst.utils.tests.MemoryTestCase):
    pass

if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
