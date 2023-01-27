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

import sys
import unittest

import lsst.utils.tests
from lsst.ctrl.execute.allocatorParser import AllocatorParser


def setup_module(module):
    lsst.utils.tests.init()


class TestAllocatorParser(lsst.utils.tests.TestCase):
    def test1(self):
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

        self.assertEqual(args.nodeCount, 64)
        self.assertEqual(args.cpus, 12)
        self.assertEqual(args.maximumWallClock, "00:30:00")
        self.assertEqual(args.queue, "normal")
        self.assertEqual(args.outputLog, "outlog")
        self.assertEqual(args.errorLog, "errlog")
        self.assertTrue(args.verbose)


class AllocatorParserMemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
