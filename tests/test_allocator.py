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
import filecmp
import os.path
import sys
import time
import unittest

import lsst.utils.tests
from lsst.ctrl.execute.allocatorParser import AllocatorParser
from lsst.ctrl.execute.condorConfig import CondorConfig
from lsst.ctrl.execute.namedClassFactory import NamedClassFactory


def setup_module(module):
    lsst.utils.tests.init()


class TestAllocator(lsst.utils.tests.TestCase):
    def verboseArgs(self):
        argv = [
            "allocator_test",
            "test_platform",
            "-n",
            "64",
            "-c",
            "12",
            "-m",
            "00:30:00",
            "-N",
            "test_set",
            "-q",
            "normal",
            "-e",
            "-O",
            "outlog",
            "-E",
            "errlog",
            "-v",
        ]
        return argv

    def regularArgs(self):
        argv = [
            "allocator_test",
            "test_platform",
            "-n",
            "64",
            "-c",
            "12",
            "-m",
            "00:30:00",
            "-N",
            "test_set",
            "-q",
            "normal",
            "-e",
            "-O",
            "outlog",
            "-E",
            "errlog",
        ]
        return argv

    def subSetup(self, configFileName):
        alp = AllocatorParser(sys.argv[0])
        args = alp.getArgs()

        condorConfigFile = os.path.join("tests", "testfiles", configFileName)
        configuration = CondorConfig()
        configuration.load(condorConfigFile)

        fileName = os.path.join("tests", "testfiles", "allocator-info1.py")

        schedulerName = configuration.platform.scheduler
        schedulerClass = NamedClassFactory.createClass(
            "lsst.ctrl.execute." + schedulerName + "Plugin"
        )

        scheduler = schedulerClass("lsst", args, configuration, fileName)
        return scheduler

    def test1(self):
        sys.argv = self.verboseArgs()
        al = self.subSetup("config_condor.py")

        identifier1 = al.createUniqueIdentifier()
        time.sleep(1)
        identifier2 = al.createUniqueIdentifier()
        self.assertTrue(identifier1 != identifier2)

    def test2(self):
        sys.argv = self.verboseArgs()
        al = self.subSetup("config_asserts.py")

        fileName = os.path.join("tests", "testfiles", "config_allocation.py")
        al.loadPbs(fileName)
        self.assertEqual(al.getHostName(), "bighost.lsstcorp.org")
        self.assertEqual(al.getUtilityPath(), "/bin")
        self.assertEqual(al.getScratchDirectory(), "/tmp")
        self.assertEqual(al.getNodeSetName(), "test_set")
        self.assertEqual(al.getNodes(), 64)
        self.assertEqual(al.getCPUs(), 12)
        self.assertEqual(al.getWallClock(), "00:30:00")
        self.assertEqual(al.getParameter("NODE_COUNT"), 64)
        self.assertEqual(al.getParameter("KAZOO"), None)
        self.assertTrue(al.isVerbose())
        localScratch = "./tests/condor_scratch_asserts"
        configPath = "./tests/condor_scratch_asserts/configs"
        os.rmdir(configPath)
        os.rmdir(localScratch)

    def test3(self):
        sys.argv = self.regularArgs()
        al = self.subSetup("config_condor.py")

        fileName = os.path.join("tests", "testfiles", "config_allocation.py")
        al.loadPbs(fileName)
        pbsName = os.path.join("tests", "testfiles", "generic.pbs.template")
        compare = os.path.join("tests", "testfiles", "generic.pbs.txt")
        generatedPbsFile = al.createSubmitFile(pbsName)

        self.assertTrue(filecmp.cmp(compare, generatedPbsFile))
        condorFile = os.path.join(
            "tests", "testfiles", "glidein_condor_config.template"
        )
        compare = os.path.join("tests", "testfiles", "glidein_condor_config.txt")
        generatedCondorConfigFile = al.createCondorConfigFile(condorFile)
        self.assertTrue(filecmp.cmp(compare, generatedCondorConfigFile))
        os.remove(generatedCondorConfigFile)
        os.remove(generatedPbsFile)
        localScratch = "./tests/condor_scratch_condor"
        configPath = "./tests/condor_scratch_condor/configs"
        os.rmdir(configPath)
        os.rmdir(localScratch)

    def test4(self):
        sys.argv = self.regularArgs()
        al = self.subSetup("config_condor_slurm.py")

        fileName = os.path.join("tests", "testfiles", "config_allocation_slurm.py")
        al.loadSlurm(fileName, None)
        slurmName = os.path.join("tests", "testfiles", "generic.slurm.template")
        compare = os.path.join("tests", "testfiles", "generic.slurm.txt")
        generatedSlurmFile = al.createSubmitFile(slurmName)

        self.assertTrue(filecmp.cmp(compare, generatedSlurmFile))
        condorFile = os.path.join(
            "tests", "testfiles", "glidein_condor_config.template"
        )
        compare = os.path.join("tests", "testfiles", "glidein_condor_config.txt")
        generatedCondorConfigFile = al.createCondorConfigFile(condorFile)
        self.assertTrue(filecmp.cmp(compare, generatedCondorConfigFile))
        os.remove(generatedCondorConfigFile)
        os.remove(generatedSlurmFile)
        localScratch = "./tests/condor_scratch_slurm"
        configPath = "./tests/condor_scratch_slurm/configs"
        os.rmdir(configPath)
        os.rmdir(localScratch)


class AllocatorMemoryTest(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
