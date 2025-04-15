# Format:
# {
#   'lines_visited': [...],
#   'test_pass': bool
# }
testDataResults = []

#sample code lines, in the actual code, these would be based on the code being tested
code_lines = [
    "int m;",
    "m = z;",
    "if (y < z):",
    "if (x < y):",
    "m = y;",
    "elif (x < z):",
    "m = y; #error",
    "else:",
    "if (x > y):",
    "m = y;",
    "elif (x > z):",
    "m = x;",
    "return m;"
]

def recordTestExecution(lines_visited, test_pass):
    """
    Lines visited is a list of line indices that were covered by the test
    (line indices start at 0), and test_pass is True/False.
    """
    testDataResults.append({
        'lines_visited': lines_visited,
        'test_pass': test_pass
    })


def computeSuspiciousness():
    """
      suspiciousness(i) = 
         (failCoverage(i) / totalFailTests) 
         -----------------------------------
         (failCoverage(i) / totalFailTests) + (passCoverage(i) / totalPassTests)

    where:
      failCoverage(i) = Number of failing tests that visited line i
      passCoverage(i) = Number of passing tests that visited line i
      totalFailTests  = Total number of failing tests
      totalPassTests  = Total number of passing tests
    """
    totalLineNumber = len(code_lines)

    # Counts total number of pass/fail tests
    totalPasses = sum(1 for t in testDataResults if t['test_pass'])
    totalFails = sum(1 for t in testDataResults if not t['test_pass'])

    # Count how much each line has passed/failed tests
    linePassCoverage = [0] * totalLineNumber
    lineFailCoverage = [0] * totalLineNumber
    suspiciousness_scores = [0.0] * totalLineNumber

    for t in testDataResults:
        if t['test_pass']:
            for ln in t['lines_visited']:
                linePassCoverage[ln] += 1
        else:
            for ln in t['lines_visited']:
                lineFailCoverage[ln] += 1

    for i in range(totalLineNumber):
        # Avoid crash if the line was never ran
        if totalFails == 0 and totalPasses == 0:
            suspiciousness_scores[i] = 0.0
            continue
        numerator = (lineFailCoverage[i] / float(totalFails)) if totalFails else 0.0
        denominator_pass_part = (linePassCoverage[i] / float(totalPasses)) if totalPasses else 0.0
        denom = numerator + denominator_pass_part

        if denom == 0:
            # Avoid crash if the line was never ran
            suspiciousness_scores[i] = 0.0
        else:
            suspiciousness_scores[i] = numerator / denom

    return suspiciousness_scores


def printSuspiciousnessScores(scores):
    """
    Prints each lines' suspiciousness score, as well as the most likely fault line.
    """
    for i, line in enumerate(code_lines):
        print(f"{line} => Suspiciousness: {scores[i]:.2f}")

    max_score = max(scores)
    most_likely_indices = [i for i, score in enumerate(scores) if score == max_score]

    print()
    for idx in most_likely_indices:
        print(f"Most likely fault line: Line {idx+1}: {code_lines[idx]}")


# --------------------------------------------------------------------------
# Sample run based on the slides:
# --------------------------------------------------------------------------
recordTestExecution([0,1,2,3,5,6,12], True)
recordTestExecution([0,1,2,3,4,12], True)
recordTestExecution([0,1,2,7,8,9,12], True)
recordTestExecution([0,1,2,7,8,10,12], True)
recordTestExecution([0,1,2,3,5,12], True)
recordTestExecution([0,1,2,3,5,6,12], False)

# Now compute and print the suspiciousness for each line
scores = computeSuspiciousness()
printSuspiciousnessScores(scores)
