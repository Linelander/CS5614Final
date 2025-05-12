# Format:
# {
#   'lines_visited': [...],
#   'test_pass': bool
# }
import math

def recordTestExecution(dataResults: list[dict[str, bool]], lines_visited: list[str], test_pass: bool):

    dataResults.append({
        'lines_visited': lines_visited,
        'test_pass': test_pass
    })


def computeSuspiciousness(codeLines, dataResults, line_nos):
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
    line_number_to_index = {ln: idx for idx, ln in enumerate(line_nos)}
    totalLineNumber = len(codeLines)
    totalPasses = 0
    totalFails = 0

    for t in dataResults:
        if not t['test_pass']:
            totalFails += 1
        else:
            totalPasses += 1

    linePassCoverage = [0] * totalLineNumber
    lineFailCoverage = [0] * totalLineNumber

    for t in dataResults:
        for ln in t['lines_visited']:
            if ln in line_number_to_index:
                idx = line_number_to_index[ln]
                if t['test_pass']:
                    linePassCoverage[idx] += 1
                else:
                    lineFailCoverage[idx] += 1

    tarantula_scores = [0.0] * totalLineNumber
    spectra_scores   = [0.0] * totalLineNumber
    kulczynski_scores = [0.0] * totalLineNumber

    for i in range(totalLineNumber):
        failFrac = lineFailCoverage[i] / float(totalFails) if totalFails else 0.0
        passFrac = linePassCoverage[i] / float(totalPasses) if totalPasses else 0.0

        denom_t = failFrac + passFrac
        tarantula_scores[i] = failFrac / denom_t if denom_t else 0.0

        denom_s = totalFails * (lineFailCoverage[i] + linePassCoverage[i])
        spectra_scores[i] = lineFailCoverage[i] / math.sqrt(denom_s) if totalFails and denom_s > 0 else 0.0

        covered = lineFailCoverage[i] + linePassCoverage[i]
        if totalFails and covered > 0:
            part1 = lineFailCoverage[i] / float(totalFails)
            part2 = lineFailCoverage[i] / float(covered)
            kulczynski_scores[i] = 0.5 * part1 + 0.5 * part2
        else:
            kulczynski_scores[i] = 0.0

    return tarantula_scores, spectra_scores, kulczynski_scores

def computeMostSuspiciousLine(model, line_nos):
    max_value = max(model)
    return [line_nos[i] for i, score in enumerate(model) if score == max_value]

def printSuspiciousnessScores(codeLines: list[str], dataResults: list[dict[str, any]], line_nos: list[int]):

    tarantula, spectra, kulczynski = computeSuspiciousness(codeLines, dataResults, line_nos)
    header = f"{'Line':>4}  {'Tarantula':>10}  {'Spectra':>10}  {'Kulczynski':>11}    Code"
    print(header)
    print("-" * len(header))
    for i, line in zip(line_nos, codeLines):
        idx = line_nos.index(i)
        print(f"{i:4d}  {tarantula[idx]:10.3f}  {spectra[idx]:10.3f}  {kulczynski[idx]:11.3f}   {line}")

    print("")
    print(f"Most suspicious by Tarantula:    line(s) {', '.join(map(str, computeMostSuspiciousLine(tarantula, line_nos)))}")
    print(f"Most suspicious by Spectra:      line(s) {', '.join(map(str, computeMostSuspiciousLine(spectra, line_nos)))}")
    print(f"Most suspicious by Kulczynski:   line(s) {', '.join(map(str, computeMostSuspiciousLine(kulczynski, line_nos)))}")


# #sample code lines, in the actual code, these would be based on the code being tested
# code_lines = [
#     "int m;",
#     "m = z;",
#     "if (y < z):",
#     "if (x < y):",
#     "m = y;",
#     "elif (x < z):",
#     "m = y; #error",
#     "else:",
#     "if (x > y):",
#     "m = y;",
#     "elif (x > z):",
#     "m = x;",
#     "return m;"
# ]

# # --------------------------------------------------------------------------
# # Sample run based on the slides:
# # --------------------------------------------------------------------------
# #Empty list that will be filled up with the necessary input data
# testDataResults = []

# recordTestExecution(testDataResults, [0,1,2,3,5,6,12], True)
# recordTestExecution(testDataResults, [0,1,2,3,4,12], True)
# recordTestExecution(testDataResults, [0,1,2,7,8,9,12], True)
# recordTestExecution(testDataResults, [0,1,2,7,8,10,12], True)
# recordTestExecution(testDataResults, [0,1,2,3,5,12], True)
# recordTestExecution(testDataResults, [0,1,2,3,5,6,12], False)

# """
# code_lines: The visual lines of code that are being tested as a list of lines in order.
# testDataResults: The results of the test executions, including which lines were visited and whether the test passed or failed.
#                         Fill in using recordTestExecution() helper function.

# testDataResults Format:
# {
#   'lines_visited': [...],
#   'test_pass': bool
# }
# """
# printSuspiciousnessScores(code_lines, testDataResults)
