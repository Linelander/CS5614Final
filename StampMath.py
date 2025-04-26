import operator
import inspect

# Will hold an RDD and relevant line numbers
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



def rddArithmetic(operation, resilient):
    # empty list lines inherits line numbers of all number arguments
    args = resilient.collect()
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


# run an operatoin functionally with no tainting. Use in tandem with arithemetic()
def functionalOp(resilient, methodstr, *args):
    method = getattr(resilient, methodstr)
    return method(*args)


# IMPORTANT TODO:
# This method can stamp asymmetric operations, but only if every single line number in the RDD should be included in all
# tuples of the output.
#
# What if you had an RDD with data having these line numbers:
#
# (a,[1,3]) (a,[5,7]) (b,[9,11])
#
# And then you did some by-key operation?
#
# This function in its current state would not realize that the tuple resulting in the operation carried out on all tuples
# having the key a shouldn't contain b's line numbers 9 & 11. It also doesn't realize that things resulting from b shouldn't
# have line numbers associated with a.
#
# CORRECT OUTPUT (our goal): [(a+a,[1,3,5,7]), (b,[9,11])]
# OUR OUTPUT (incorrect): [(a+a,[1,3,5,7,9,11]), (b,[1,3,5,7,9,11])]
#
# Method stub for solution after this method
#
def asymOp(resilient, methodstr, *args):
    lines_list = []
    
    # Get line number of caller
    frame = inspect.currentframe()
    caller_frame = frame.f_back
    line_num = caller_frame.f_lineno

    # look for stamped values in rdd
    if not resilient.filter(lambda x: isinstance(x, StampedValue)).isEmpty():
        lines_list = resilient.flatMap(lambda x: x.line_numbers).distinct().collect()
        print("LINES LIST: " + str(lines_list))
        precursor = resilient.map(lambda x: (x.value))
    else:
        precursor = resilient
    
    # Apply line numbers from input RDD + caller line to output
    method = getattr(precursor, methodstr)
    original = method(*args)
    lines_list += [line_num]
    return original.map(lambda x: (StampedValue(x, lines_list)))


# Perform an operation by key. Only spread taint within keys.
# def opByKey(resilient, methodstr, *args):
    # 1. Unstamp input
    # 2. Group it
    # 3. Split it
    # 4. perform operation
    # 5. stamp
    # 6. recombine



# Stamp everything in a vanilla RDD with the caller line number (use for SparkContext.textFile() and the like)
def stampRDD(resilient):
    frame = inspect.currentframe()
    caller_frame = frame.f_back
    line_num = caller_frame.f_lineno
    
    return resilient.map(lambda x: StampedValue(x, [line_num]))