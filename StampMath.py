import operator
import inspect

class StampedValue:
    def __init__(self, val, line_num):
        self.value = val
        self.line_numbers = line_num

def arithmetic(operation, *args):
    # empty list lines inherits line numbers of all number arguments
    lines = []
    
    frame = inspect.currentframe()
    caller_frame = frame.f_back
    line_num = caller_frame.f_lineno
    
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
    lines += [line_num]

    # Stamp x with [lines]
    final = StampedValue(x, lines)
    return final


# Arbitrate one to many pyspark methods
# NOTE: this does not account for data in the RDD already being stamped. need to account for that in the stampedvalue constructor
def oneToMany(resilient, methodstr, *args):
    # Get line number of caller
    frame = inspect.currentframe()
    caller_frame = frame.f_back
    line_num = caller_frame.f_lineno

    # TODO: get line numbers of resilient
    rdd_lines = []
    def accrueLines(stamped):
        rdd_lines += stamped.line_numbers
    resilient.foreach(accrueLines)

    # TODO: Apply the user supplied method. NOTE: use getattr()
    method = # ...
    original = # ...

    # TODO: Stamp all line numbers on the data
    lines_list = rdd_lines += [line_num]
    return original.map(lambda x: (StampedValue(x, lines_list)))



