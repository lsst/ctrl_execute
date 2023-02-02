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

import errno
import os
import re
import sys

# extracts a line from a DAG file to show which ids were processed for a
# particular dag node
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage:  %s dagNodeName filename" % os.path.basename(sys.argv[0]))
        sys.exit(errno.EINVAL)

    dagNode = sys.argv[1]
    filename = sys.argv[2]

    if not os.path.exists(filename):
        print("file %s not found" % filename)
        sys.exit(errno.ENOENT)

    ex = r"VARS %s var1=\"(?P<idlist>.+?)\"" % dagNode
    file = open(filename)
    for line in file:
        line = line.rstrip(" \n")

        # look for the line with the dagnode name in it
        # and extract everything after "var1", but not the quotes
        values = re.search(ex, line)
        if values is None:
            continue
        ids = values.groupdict()["idlist"]
        file.close()
        print(ids)
        sys.exit(0)
    file.close()
    sys.exit(0)
