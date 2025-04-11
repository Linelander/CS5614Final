import operator

class StampedValue:
    def __init__(self, val):
        self.value = val
        self.line_numbers = []

def arithmetic(lineNo, operation, *args):
    # empty list lines inherits line numbers of all number arguments
    lines = []
    if type(args[0]) == StampedValue:
        x = args[0].value
        lines += [num for num in args[0].line_numbers]
    else:
        x = args[0]
    
    i = 1
    while i < len(args):
        if type(args[i]) == StampedValue:
            x = operation(x, args[i].value)
            lines += [num for num in args[i].line_numbers]
        else:
            x = operation(x, args[i])
        i+=1
    
    # add the line number this function was called
    lines += [lineNo]

    # Stamp x with [lines]
    final = StampedValue(x)
    final.line_numbers = lines
    return final

def getWordCount(line_numbers):
    total_word_count = 0
    for line_number in line_numbers:
        print('line number:', line_number)
        total_word_count += readFileLineWordCount('ArithmeticTest.py', line_number)

    return total_word_count

def readFileLineWordCount(filepath, line_number):
    with open(filepath, 'r') as file:
        lines = file.readlines()

    word_count = len(lines[line_number - 1].split())
    print(lines[line_number - 1].split(), '| length:', word_count)
    return word_count