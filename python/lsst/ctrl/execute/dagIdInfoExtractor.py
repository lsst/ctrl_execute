#!/usr/bin/env python
# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
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

import re, sys, os

class DagIdInfoExtractor(object):
    def extract(self, dagname, filename):
        file = open(filename)
        for line in file:
            line = line.rstrip(' \n')
            # look for the line with the dagnode name in it
            #ex = r'VARS %s var1=(?P<idlist>.+?)($)' % dagname
            ex = r'VARS %s var1=\"(?P<idlist>.+?)\"' % dagname
            values = re.search(ex,line)
            if values is None:
                continue
            ids = values.groupdict()['idlist']
            file.close()
            return ids
        file.close()
        return None
