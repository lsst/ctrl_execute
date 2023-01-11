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
import os
import os.path
import unittest

import lsst.utils.tests
from lsst.ctrl.execute.templateWriter import TemplateWriter


def setup_module(module):
    lsst.utils.tests.init()


class TestTemplateWriter(lsst.utils.tests.TestCase):
    def test1(self):
        pairs = {}
        pairs["TEST1"] = "Hello"
        pairs["TEST2"] = "Goodbye"
        infile = os.path.join("tests", "testfiles", "templateWriter.template")
        compare = os.path.join("tests", "testfiles", "templateWriter.txt")
        with lsst.utils.tests.getTempFilePath("_template.txt") as outfile:
            temp = TemplateWriter()
            temp.rewrite(infile, outfile, pairs)
            self.assertTrue(filecmp.cmp(compare, outfile))


class TestTemplateWriterTestCase(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
