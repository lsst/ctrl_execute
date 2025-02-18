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


class TemplateWriter:
    """Class to take a template file, substitute values through it, and
    write a new file with those values.
    """

    def rewrite(self, input: ResourcePathExpression, output: ResourcePathExpression, pairs):
        """Given a input template, take the keys from key/values in the config
        object and substitute the values, and write those to the output file.
        @param input - the input template name
        @param output - the output file name
        @param pairs of values to substitute in the template
        """
        with ResourcePath(output).open(mode="w") as f_out:
            with ResourcePath(input).open(mode="r") as f_in:
                for line in f_in.readlines():
                    # replace the user defined names
                    for name in pairs:
                        key = "$" + name
                        val = str(pairs[name])
                        line = line.replace(key, val)
                    f_out.write(line)
