from faultLocalization import recordTestExecution, printSuspiciousnessScores
import re

# get code lines
with open('rddtest.py', 'r') as f:
    code_lines = [line.rstrip('\n') for line in f]

# get code output
with open('output.txt', 'r') as file:
    log_output = file.read()

# get line number list
matches = re.findall(r'\[(\d+(?:,\s*\d+)*)\]', log_output)
line_number_list = [list(map(int, match.split(','))) for match in matches]

# getting suspicious line numbers
testDataResults = []
faulty_lines = {104} # can change this to any line number(s)

for i, line_list in enumerate(line_number_list):
    passed = not any(line in faulty_lines for line in line_list)
    recordTestExecution(testDataResults, line_list, passed)


printSuspiciousnessScores(code_lines, testDataResults)

