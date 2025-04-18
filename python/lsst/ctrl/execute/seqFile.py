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

from lsst.resources import ResourcePath, ResourcePathExpression


class SeqFile:
    """Class which can read and increment files used to store sequence
    numbers"""

    filename: ResourcePath

    def __init__(self, seqFileName: ResourcePathExpression):
        """Constructor
        @param seqFileName file name to operate on
        """
        self.fileName = ResourcePath(seqFileName)

    def nextSeq(self):
        """Produce the next sequence number.
        @return a sequence number
        """
        seq = 0
        if not self.fileName.exists():
            self.writeSeq(seq)
        else:
            seq = self.readSeq()
            seq += 1
            self.writeSeq(seq)
        return seq

    def readSeq(self):
        """Read a sequence number
        @return a sequence number
        """
        with self.fileName.open(mode="r") as seqFile:
            line = seqFile.readline()
            seq = int(line)
        return seq

    def writeSeq(self, seq):
        """Write a sequence number"""
        with self.fileName.open(mode="w") as seqFile:
            print(seq, file=seqFile)
