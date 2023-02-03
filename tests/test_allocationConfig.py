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
import unittest

import lsst.utils.tests
from lsst.ctrl.execute.allocationConfig import AllocationConfig


def setup_module(module):
    lsst.utils.tests.init()


class AllocationConfigTest(lsst.utils.tests.TestCase):
    def setUp(self):
        self.config = AllocationConfig()

    def test_config1(self):
        self.assertEqual(self.config.platform.queue, "debug")
        self.assertIsNone(self.config.platform.email)
        self.assertIsNone(self.config.platform.scratchDirectory)
        self.assertIsNone(self.config.platform.loginHostName)
        self.assertIsNone(self.config.platform.utilityPath)

    def test_config2(self):
        path = os.path.join("tests", "testfiles", "config_allocation.py")
        self.config.load(path)

        self.assertEqual(self.config.platform.queue, "normal")
        self.assertEqual(self.config.platform.email, "#PBS mail -be")
        self.assertEqual(self.config.platform.scratchDirectory, "/tmp")
        self.assertEqual(self.config.platform.loginHostName, "bighost.lsstcorp.org")
        self.assertEqual(self.config.platform.utilityPath, "/bin")


class AllocationConfigMemoryTest(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
