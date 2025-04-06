import sys

# PARSER - puts lines of pyspark job into a list
# Get job name from user
try:
    jobName = sys.argv[1]
except: sys.exit("INPUT ERROR: must specify a job name from jobs folder ending in .py")
job = open("./jobs/"+jobName, "r")
lines = [line.rstrip() for line in job]
# END PARSER


# MAIN LOGIC - loop through the lines
for i in range(len(lines)):
    # This is where we need to start associating line number with operations