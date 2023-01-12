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
from lsst.ctrl.execute.condorInfoConfig import CondorInfoConfig


def setup_module(module):
    lsst.utils.tests.init()


class TestCondorInfoConfig(lsst.utils.tests.TestCase):
    def test1(self):
        path = os.path.join("tests", "testfiles", "config_condorInfo.py")
        config = CondorInfoConfig()

        config.load(path)

        self.assertTrue(config.platform["test1"].user.name == "test1")
        self.assertTrue(config.platform["test1"].user.home == "/home/test1")
        self.assertTrue(config.platform["test2"].user.name == "test2")
        self.assertTrue(config.platform["test2"].user.home == "/home/test2")


class CondorInfoConfigMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
