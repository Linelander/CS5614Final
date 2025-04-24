# Format:
# {
#   'lines_visited': [...],
#   'test_pass': bool
# }
import math

def recordTestExecution(dataResults: list[dict[str, bool]], lines_visited: list[str], test_pass: bool):
    """
    Lines visited is a list of line indices that were covered by the test
    (line indices start at 0), and test_pass is True/False.
    """
    dataResults.append({
        'lines_visited': lines_visited,
        'test_pass': test_pass
    })


def computeSuspiciousness(codeLines, dataResults):
    """
    method:
       'tarantula'  – uses the Tarantula formula
       'spectra'    – uses the Ochiai Spectra formula
       'kulczynski' - uses the Kulczynski formula

    Formulas:

    Tarantula:
    (failCoverage(i) / totalFailTests)
    —————————————————————————————
    (failCoverage(i) / totalFailTests) + (passCoverage(i) / totalPassTests)

    -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    Spectra (Ochiai):
    failCoverage(i)
    ——————————————————————————
    sqrt( totalFailTests * (failCoverage(i) + passCoverage(i)) )

    -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    Kulczynski:
      0.5 * (failCoverage(i) / totalFailTests) + 0.5 * (failCoverage(i) / (failCoverage(i) + passCoverage(i)))

    """
    totalLineNumber = len(codeLines)
    totalPasses = 0
    totalFails = 0
    # Counts total number of pass/fail tests
    for t in dataResults:
        if not t['test_pass']:
            totalFails += 1
        else:
            totalPasses += 1

    # Initialize per-line coverage
    linePassCoverage = [0] * totalLineNumber
    lineFailCoverage = [0] * totalLineNumber

    # Tally coverage
    for t in testDataResults:
        if t['test_pass']:
            for ln in t['lines_visited']:
                linePassCoverage[ln] += 1
        else:
            for ln in t['lines_visited']:
                lineFailCoverage[ln] += 1

    # Prepare score containers
    tarantula_scores = [0.0] * totalLineNumber
    spectra_scores   = [0.0] * totalLineNumber
    kulczynski_scores = [0.0] * totalLineNumber

    for i in range(totalLineNumber):
        # --- compute Tarantula ---
        if totalFails:
            failFrac = lineFailCoverage[i] / float(totalFails)
        else:
            failFrac = 0.0

        if totalPasses:
            passFrac = linePassCoverage[i] / float(totalPasses)
        else:
            passFrac = 0.0

        denom_t = failFrac + passFrac
        if denom_t:
            tarantula_scores[i] = failFrac / denom_t
        else:
            tarantula_scores[i] = 0.0

        # --- compute Spectra (Ochiai-style) ---
        denom_s = totalFails * (lineFailCoverage[i] + linePassCoverage[i])
        if totalFails and denom_s > 0:
            spectra_scores[i] = lineFailCoverage[i] / math.sqrt(denom_s)
        else:
            spectra_scores[i] = 0.0

        # --- compute Kulczynski ---
        covered = lineFailCoverage[i] + linePassCoverage[i]
        if totalFails and covered > 0:
            part1 = lineFailCoverage[i] / float(totalFails)
            part2 = lineFailCoverage[i] / float(covered)
            kulczynski_scores[i] = 0.5 * part1 + 0.5 * part2
        else:
            kulczynski_scores[i] = 0.0

    return tarantula_scores, spectra_scores, kulczynski_scores

def computeMostSuspiciousLine(model):
    max_value = max(model)
    culprits = []
    for i, s in enumerate(model):
        if s == max_value:
            culprits.append(i + 1)
    #returns most suspicious line(s) in a list
    return culprits

def printSuspiciousnessScores(codeLines: list[str], dataResults: list[dict[str, any]]):
    """
    Prints each lines' suspiciousness score, as well as the most likely fault line.
    """
    tarantula, spectra, kulczynski = computeSuspiciousness(codeLines, dataResults)
    header = f"{'Line':>4}  {'Tarantula':>10}  {'Spectra':>10}  {'Kulczynski':>11}    Code"
    print(header)
    print("-" * len(header))
    for i, line in enumerate(codeLines):
        print(f"{i+1:4d}  {tarantula[i]:10.3f}  {spectra[i]:10.3f}  {kulczynski[i]:11.3f}   {line}")

    # Identify most suspicious lines per metric

    print("")
    print(f"Most suspicious by Tarantula:    line(s) {', '.join(map(str, computeMostSuspiciousLine(tarantula)))}")
    print(f"Most suspicious by Spectra:      line(s) {', '.join(map(str, computeMostSuspiciousLine(spectra)))}")
    print(f"Most suspicious by Kulczynski:   line(s) {', '.join(map(str, computeMostSuspiciousLine(kulczynski)))}")


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

# --------------------------------------------------------------------------
# Sample run based on the slides:
# --------------------------------------------------------------------------
#Empty list that will be filled up with the necessary input data
testDataResults = []

recordTestExecution(testDataResults, [0,1,2,3,5,6,12], True)
recordTestExecution(testDataResults, [0,1,2,3,4,12], True)
recordTestExecution(testDataResults, [0,1,2,7,8,9,12], True)
recordTestExecution(testDataResults, [0,1,2,7,8,10,12], True)
recordTestExecution(testDataResults, [0,1,2,3,5,12], True)
recordTestExecution(testDataResults, [0,1,2,3,5,6,12], False)

"""
code_lines: The visual lines of code that are being tested as a list of lines in order.
testDataResults: The results of the test executions, including which lines were visited and whether the test passed or failed.
                        Fill in using recordTestExecution() helper function.

testDataResults Format:
{
  'lines_visited': [...],
  'test_pass': bool
}
"""
printSuspiciousnessScores(code_lines, testDataResults)
