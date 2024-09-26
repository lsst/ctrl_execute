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

import logging
import os
import sys
from typing import Any

import lsst.utils
from lsst.ctrl.execute import envString
from lsst.ctrl.execute.allocatorParser import AllocatorParser
from lsst.ctrl.execute.condorConfig import CondorConfig
from lsst.ctrl.execute.namedClassFactory import NamedClassFactory

_LOG = logging.getLogger("lsst.ctrl.execute")


def setup_logging(options: dict[str, Any] | None = None) -> None:
    """Configure logger.

    Parameters
    ----------
    options : dict[str, Any]
       Logger settings. The key/value pairs it contains will be used to
       override corresponding default settings.  If empty or None (default),
       logger will be set up with default settings.
    """
    settings = {
        "datefmt": "%Y-%m-%dT%H:%M:%S%z",
        "format": "%(levelname)s %(asctime)s %(name)s - %(message)s",
        "level": logging.INFO,
        "stream": sys.stderr,
    }
    if options is not None:
        settings |= options
    logging.basicConfig(**settings)


def main():
    """Allocates Condor glide-in nodes a scheduler on a remote Node."""

    p = AllocatorParser(sys.argv[0])

    options = {}
    if p.args.verbose:
        options = {"level": logging.DEBUG}
    setup_logging(options)

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
