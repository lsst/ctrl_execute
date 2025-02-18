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

import os
import sys
import unittest
from subprocess import PIPE, Popen

import lsst.utils.tests


def setup_module(module):
    lsst.utils.tests.init()


class TestDagIdInfo(lsst.utils.tests.TestCase):
    def executeCommand(self, cmd):
        p = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
        stdout, stderr = p.communicate()
        stdout = stdout.decode()
        return stdout

    def test1(self):
        exe = sys.executable
        execPath = "lsst.ctrl.execute.libexec.dagIdInfo"
        filename = os.path.join("tests", "testfiles", "test.diamond.dag")

        stdout = self.executeCommand(f"{exe} -m {execPath} A1 {filename}")
        self.assertEqual(stdout, "run=1033 filter=r camcol=2 field=229\n")

        stdout = self.executeCommand(f"{exe} -m {execPath} A3 {filename}")
        self.assertEqual(stdout, "run=1033 filter=i camcol=2 field=47\n")

        stdout = self.executeCommand(f"{exe} -m {execPath} A17 {filename}")
        val = "run=1033 filter=r camcol=2 field=229 run=1033 filter=i camcol=2 field=47\n"
        self.assertEqual(stdout, val)

        stdout = self.executeCommand(f"{exe} -m {execPath} B1 {filename}")
        self.assertEqual(stdout, "")


class TestDagInfoMemoryTest(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
