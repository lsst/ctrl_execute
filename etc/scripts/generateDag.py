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

import argparse
import sys
from shlex import split as cmd_split


def _line_to_args(self, line):
    for arg in cmd_split.split(line, comments=True, posix=True):
        if not arg.strip():
            continue
        yield arg


def makeArgumentParser(description, inRootsRequired=True, addRegistryOption=True):
    parser = argparse.ArgumentParser(
        description=description,
        fromfile_prefix_chars="@",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=" \nly.",
    )
    parser.convert_arg_line_to_args = _line_to_args

    parser.add_argument("-s", "--source", dest="source", help="Source site for file transfer.")

    parser.add_argument("-w", "--workerdir", dest="workerdir", help="workers directory")

    parser.add_argument("-t", "--template", dest="template", help="template file")

    parser.add_argument("-p", "--prescript", dest="prescript", help="pre shell script")

    parser.add_argument("-r", "--runid", dest="runid", help="runid of production")

    parser.add_argument("-i", "--idsPerJob", dest="idsPerJob", help="number of ids to run per job")

    return parser


def writeVarsInfo(output, count, myDataTotal, visit, runid):
    output.write("VARS A" + count + ' var1="' + myDataTotal + '" \n')
    output.write("VARS A" + count + ' var2="' + count + '" \n')
    output.write("VARS A" + count + ' visit="' + visit + '" \n')
    output.write("VARS A" + count + ' runid="' + runid + '" \n')
    output.write("VARS A" + count + ' workerid="' + count + '" \n')


def writeMapInfo(output, count, newDataTotal, myDataTotal):
    output.write(count + "  " + newDataTotal + "\n")
    output.write(count + "  " + myDataTotal + "\n")


def writeDagFile(pipeline, templateFile, infile, workerdir, prescriptFile, runid, idsPerJob):
    """
    Write Condor Dag Submission files.
    """

    print("Writing DAG file ")
    print("idsPerJob")
    print(idsPerJob)

    listSize = idsPerJob

    outname = pipeline + ".diamond.dag"
    mapname = pipeline + ".mapping"
    configname = pipeline + ".config"

    print(outname)

    mapObj = open(mapname, "w")
    outObj = open(outname, "w")
    configObj = open(configname, "w")

    configObj.write("DAGMAN_MAX_SUBMITS_PER_INTERVAL=1000\n")
    configObj.write("DAGMAN_SUBMIT_DELAY=0\n")
    configObj.write("DAGMAN_USER_LOG_SCAN_INTERVAL=5\n")

    outObj.write(f"CONFIG {configname}\n")
    outObj.write("JOB A " + workerdir + "/" + pipeline + ".pre\n")
    outObj.write("JOB B " + workerdir + "/" + pipeline + ".post\n")
    outObj.write(" \n")

    print("prescriptFile = ", prescriptFile)
    if prescriptFile is not None:
        outObj.write("SCRIPT PRE A " + prescriptFile + "\n")

    #
    # note: we make multiple passes through the input file because it could be
    # quite large
    #

    #
    # A first pass through the Input File to define the individual Jobs
    # Loop over input entries
    #
    fileObj = open(infile)
    count = 0
    acount = 0
    myDataTotal = ""
    myDataList = []
    newDataTotal = ""
    newDataList = []
    for aline in fileObj:
        acount += 1
        myData = aline.rstrip()

        #
        # Construct a string without spaces from the dataids for a job
        # suitable for a unix file name
        #
        # Searching for a space detects
        # extended input like :  visit=887136081 raft=2,2 sensor=0,1
        # If there is no space, the dataid is something simple like
        # a skytile id
        newData = myData
        visit = str(count // 100)

        myDataList.append(myData)
        newDataList.append(newData)

        # For example:
        # VARS A1 var1="visit=887136081 raft=2,2 sensor=0,1"
        # VARS A1 var2="visit-887136081:raft-2_2:sensor-0_1"

        if acount == listSize:
            count += 1
            outObj.write("JOB A" + str(count) + " " + workerdir + "/" + templateFile + "\n")
            myDataTotal = " X ".join(myDataList)
            newDataTotal = "_".join(newDataList)
            writeVarsInfo(outObj, str(count), myDataTotal, visit, runid)
            writeMapInfo(mapObj, str(count), newDataTotal, myDataTotal)
            # PARENT A CHILD A1
            # PARENT A1 CHILD B
            outObj.write("PARENT A CHILD A" + str(count) + " \n")
            outObj.write("PARENT A" + str(count) + " CHILD B \n")

            acount = 0
            myDataTotal = ""
            newDataTotal = ""
            myDataList = []
            newDataList = []
            outObj.write("\n")

    # if acount != 0, then we have left over ids to deal with, and need
    # to create one more worker to do so.
    if acount != 0:
        count += 1
        outObj.write("JOB A" + str(count) + " " + workerdir + "/" + templateFile + "\n")
        myDataTotal = " X ".join(myDataList)
        newDataTotal = "_".join(newDataList)
        writeVarsInfo(outObj, str(count), myDataTotal, visit, runid)
        writeMapInfo(mapObj, str(count), newDataTotal, myDataTotal)
        outObj.write("PARENT A CHILD A" + str(count) + " \n")
        outObj.write("PARENT A" + str(count) + " CHILD B \n")
        outObj.write("\n")

    fileObj.close()
    configObj.close()
    outObj.close()
    mapObj.close()


def main():
    print("Starting generateDag.py")
    parser = makeArgumentParser(
        description="generateDag.py write a Condor DAG for job submission"
        "by reading input list and writing the attribute as an argument."
    )
    print("Created parser")
    ns = parser.parse_args()
    print("Parsed Arguments")
    print(ns)
    print(ns.idsPerJob)

    pipeline = "Workflow"

    writeDagFile(
        pipeline,
        ns.template,
        ns.source,
        ns.workerdir,
        ns.prescript,
        ns.runid,
        int(ns.idsPerJob),
    )

    sys.exit(0)


if __name__ == "__main__":
    main()
