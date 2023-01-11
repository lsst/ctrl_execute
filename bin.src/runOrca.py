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
import os.path
import sys

import lsst.utils
from lsst.ctrl.execute.configurator import Configurator
from lsst.ctrl.execute.runOrcaParser import RunOrcaParser


def main():
    p = RunOrcaParser(sys.argv[0])
    args = p.getArgs()
    creator = Configurator(args, "$HOME/.lsst/condor-info.py")

    platformPkgDir = lsst.utils.getPackageDir("ctrl_platform_" + creator.platform)
    if args.platformConfig is None:
        configFileName = os.path.join(platformPkgDir, "etc", "config", "execConfig.py")
    else:
        configFileName = args.platformConfig

    creator.load(configFileName)

    genericConfigName = creator.getGenericConfigFileName()
    generatedConfigFile = creator.createConfiguration(genericConfigName)

    runid = creator.getRunId()

    print("runid for this run is ", runid)

    cmd = "orca.py %s %s" % (generatedConfigFile, runid)
    print(cmd)
    cmd_split = cmd.split()
    pid = os.fork()
    if not pid:
        sys.stdin.close()
        sys.stdout.close()
        sys.stderr.close()
        if not creator.isVerbose:
            os.close(0)
            os.close(1)
            os.close(2)
        os.execvp(cmd_split[0], cmd_split)
    os.wait()[0]


if __name__ == "__main__":
    main()
