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
import os.path
import sys
import time
import unittest

import lsst.utils.tests
from lsst.ctrl.execute.configurator import Configurator
from lsst.ctrl.execute.runOrcaParser import RunOrcaParser


def setup_module(module):
    lsst.utils.tests.init()


class TestConfigurator(lsst.utils.tests.TestCase):
    def getRemoteArgs(self):
        sys.argv = [
            "configurator_test",
            "-p",
            "bigboxes",
            "-c",
            '"echo hello"',
            "-i",
            "$CTRL_EXECUTE_DIR/tests/testfiles/inputfile",
            "-e",
            "/tmp",
            "-N",
            "test_set",
            "-n",
            "16",
            "-v",
        ]
        return sys.argv

    def getLocalArgs(self):
        sys.argv = [
            "configurator_test",
            "-p",
            "lsst",
            "-c",
            '"echo hello"',
            "-i",
            "$CTRL_EXECUTE_DIR/tests/testfiles/inputfile",
            "-e",
            "/tmp2",
            "-N",
            "test_set2",
            "-n",
            "12",
        ]
        return sys.argv

    def getLocalWithSetupArgs(self):
        sys.argv = [
            "configurator_test",
            "-p",
            "lsst",
            "-c",
            '"echo hello"',
            "-i",
            "$CTRL_EXECUTE_DIR/tests/testfiles/inputfile",
            "-e",
            "/tmp2",
            "-N",
            "test_set2",
            "-n",
            "12",
            "--setup",
            "fake_package",
            "1.0",
        ]
        return sys.argv

    def setup(self, args):
        fileName = os.path.join("tests", "testfiles", "allocator-info1.py")
        rop = RunOrcaParser(args[0])
        args = rop.getArgs()
        configurator = Configurator(args, fileName)
        return configurator

    def test1(self):
        configurator = self.setup(self.getRemoteArgs())
        self.assertTrue(configurator.isVerbose())
        self.assertEqual(configurator.getParameter("EUPS_PATH"), "/tmp")
        self.assertEqual(configurator.getParameter("USER_NAME"), "thx1138")
        self.assertEqual(configurator.getParameter("USER_HOME"), "/home/thx1138")

    def test2(self):
        configurator = self.setup(self.getLocalArgs())
        self.assertFalse(configurator.isVerbose())
        self.assertEqual(configurator.getParameter("EUPS_PATH"), "/tmp2")
        self.assertEqual(configurator.getParameter("USER_NAME"), "c3po")
        self.assertEqual(configurator.getParameter("USER_HOME"), "/lsst/home/c3po")
        self.assertEqual(configurator.getParameter("NODE_SET"), "test_set2")
        self.assertIsNone(configurator.getParameter("KAZOO"))

    def test3(self):
        configurator = self.setup(self.getRemoteArgs())
        runId1 = configurator.createRunId()
        time.sleep(1)
        runId2 = configurator.createRunId()
        self.assertTrue(runId1 != runId2)
        self.assertTrue(runId2 == configurator.getRunId())

    def test4(self):
        configurator = self.setup(self.getRemoteArgs())
        self.assertIsNotNone(configurator.getSetupPackages())


class ConfiguratorMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
