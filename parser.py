import sys
import re

# PARSER - puts lines of pyspark job into a list
# Get job name from user
# try:
#     jobName = sys.argv[1]
# except: sys.exit("INPUT ERROR: must specify a job name from jobs folder ending in .py")
jobName = "job1.py"

job = open("./jobs/"+jobName, "r")
lines = [line.rstrip() for line in job]

# associating line number with lambda functions
i = 0
while i < (len(lines)):
    if "lambda" in lines[i] and lines[i][0] != '#':
        while lines[i][len(lines[i]) - 1] == "\\": # skip to the end of multi-line expressions
            i += 1
        lines[i] += ".map(lambda x: (x, " + str(i) + "))"
    i += 1


# for i in range(len(lines)):
#     print(lines[i])

# TODO: keep it from appending the tag after comments written at the end of lines like this:
#   code.example() # test.map(lambda x:(x, 7))
