# This file is part of ctrl_execute.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

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
