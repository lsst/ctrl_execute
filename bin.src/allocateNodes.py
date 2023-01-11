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

import os
import sys

import lsst.utils
from lsst.ctrl.execute import envString
from lsst.ctrl.execute.allocatorParser import AllocatorParser
from lsst.ctrl.execute.condorConfig import CondorConfig
from lsst.ctrl.execute.namedClassFactory import NamedClassFactory


def main():
    """Allocates Condor glide-in nodes a scheduler on a remote Node."""

    p = AllocatorParser(sys.argv[0])
    platform = p.getPlatform()

    # load the CondorConfig file
    platformPkgDir = lsst.utils.getPackageDir("ctrl_platform_" + platform)
    execConfigName = os.path.join(platformPkgDir, "etc", "config", "execConfig.py")

    resolvedName = envString.resolve(execConfigName)
    configuration = CondorConfig()
    configuration.load(resolvedName)

    # create the plugin class
    schedulerName = configuration.platform.scheduler
    schedulerClass = NamedClassFactory.createClass(
        "lsst.ctrl.execute." + schedulerName + "Plugin"
    )

    # create the plugin
    scheduler = schedulerClass(
        platform, p.getArgs(), configuration, "$HOME/.lsst/condor-info.py"
    )

    # submit the request
    scheduler.submit(platform, platformPkgDir)


if __name__ == "__main__":
    main()
