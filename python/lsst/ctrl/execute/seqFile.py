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

import os.path

from lsst.ctrl.execute import envString


class SeqFile:
    """Class which can read and increment files used to store sequence
    numbers"""

    def __init__(self, seqFileName):
        """Constructor
        @param seqFileName file name to operate on
        """
        self.fileName = envString.resolve(seqFileName)

    def nextSeq(self):
        """Produce the next sequence number.
        @return a sequence number
        """
        seq = 0
        if not os.path.exists(self.fileName):
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
        with open(self.fileName) as seqFile:
            line = seqFile.read()
            seq = int(line)
        seqFile.close()
        return seq

    def writeSeq(self, seq):
        """Write a sequence number"""
        with open(self.fileName, "w") as seqFile:
            print(seq, file=seqFile)
        seqFile.close()
