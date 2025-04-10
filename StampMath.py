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