import sys

# Get job name from user

try:
    jobName = sys.argv[1]
except: sys.exit("INPUT ERROR: must specify a job name from jobs folder ending in .py")

# User specifies a string for job name
job = open("./jobs/"+jobName, "r")
lines = [line.rstrip() for line in job]
