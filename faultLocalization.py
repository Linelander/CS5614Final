

#Init variables, totalLineNumber is static defined at the run of program, rest are dynamic
totalLineNumber = 13 # This should be set to the number of lines in the program being analyzed
totalFails = 0
totalPasses = 0

def faultLocalization(testLines = [0], testPass = False):
    """
    This function is a placeholder for fault localization logic.
    It currently does not implement any specific functionality.
    """
    global totalPasses, totalFails, linePasses, lineFails
    if testPass:
        totalPasses += 1
        for i in testLines:
            try:
                linePasses[i] += 1
            except:
                print("Line number out of range or totalLineNumber not initialized")
                return False
    else:
        totalFails += 1
        for i in testLines:
            try:
                lineFails[i] += 1
            except:
                print("Line number out of range or totalLineNumber not initialized")
                return False
    return True

def printLocalization(LineList = ["null"]):
    """
    This function is a placeholder for printing fault localization results.
    It currently does not implement any specific functionality.
    """
    index = 0
    for i in LineList:
        try:
            print(i, " Suspiciouness: ", "{:.2f}".format((lineFails[index]*1.0 / totalFails) / ((lineFails[index]*1.0 / totalFails) + (linePasses[LineList.index(i)]*1.0 / totalPasses))))
            index += 1
        except:
            print("Line number out of range or totalLineNumber not initialized")
            return False

    

# Initialize the arrays to store line passes and fails
linePasses = [0] * totalLineNumber
lineFails = [0] * totalLineNumber

#Test for line 2 causing crash
faultLocalization([0,1,2,3,5,6,12], True)
faultLocalization([0,1,2,3,4,12], True)
faultLocalization([0,1,2,7,8,9,12], True)
faultLocalization([0,1,2,7,8,10,12], True)
faultLocalization([0,1,2,3,5,12], True)
faultLocalization([0,1,2,3,5,6,12], False)

code_lines = [
    "int m;",
    "m = z;",
    "if (y < z):",
    "if (x < y):",
    "m = y;",
    "elif (x < z):",
    "m = x;",
    "else:",
    "if (x > y):",
    "m = y;",
    "elif (x > z):",
    "m = x;",
    "return m;"
]

printLocalization(code_lines)
