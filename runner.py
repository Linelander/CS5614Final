#NOTE: User must:
#       Use stampedRDD
#       use stamp() instead of regular pyspark operations

import sys
import re

# PARSER - puts lines of pyspark job into a list
# Get job name from user
try:
    jobName = sys.argv[1]
except: sys.exit("INPUT ERROR: must specify a job name from jobs folder ending in .py")

job = open("./jobs/"+jobName, "r")
lines = [line.rstrip() for line in job]

# put the line number into each stamp call
i = 0
while i < (len(lines)):
    if "currLineStamp" in lines[i]:
        lines[i].replace("currLineStamp", str(i))
        exec(lines[i])
    i += 1


# TODO: keep it from appending the tag after comments written at the end of lines like this:
#   code.example() # test.map(lambda x:(x, 7))