import operator
import inspect


# TODO: toString() for StampedValues
# TODO: use copy() to prevent issues with mutable state


# TODO: Perform an operation by key. Only spread taint within keys. Needs to work
# def manyToOne(resilient, methodstr, *args):
    # map stamped values in resilient to tuples. add caller line number here
    # perform keyed operation. allow the operation itself to combine line numbers
    # rewrap data into stamped values

# TODO: Maybe this is a better approach? for things like reduce by key
# def keyedOp(resilient, methodstr, *args):

# def stampJoin(rdd1, rdd2, join):


# Will hold an RDD and relevant line numbers
class StampedValue:
    def __init__(self, val, line_num):
        self.value = val
        self.line_numbers = line_num

# Perform simple arithmetic with stamping outside of RDDs
def arithmetic(operation, *args):
    # [lines] inherits line numbers of all number arguments
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


def oneToMany(resilient, methodstr, *args):
    lines_list = []
    
    # Get line number of caller
    frame = inspect.currentframe()
    caller_frame = frame.f_back
    line_num = caller_frame.f_lineno

    # look for stamped values in rdd
    if not resilient.filter(lambda x: isinstance(x, StampedValue)).isEmpty():
        lines_list = resilient.flatMap(lambda x: x.line_numbers).distinct().collect()
        # print("LINES LIST: " + str(lines_list))
        precursor = resilient.map(lambda x: (x.value))
    else:
        precursor = resilient
    
    # Apply line numbers from input RDD + caller line to output
    method = getattr(precursor, methodstr)
    processed = method(*args)
    lines_list += [line_num]
    return processed.map(lambda x: (StampedValue(x, lines_list)))

# NOTE: Meant for map and flatMap. Haven't checked out the others
def oneToOne(resilient, methodstr, *args):
    frame = inspect.currentframe()
    caller_frame = frame.f_back
    line_num = caller_frame.f_lineno

    unwrapped = resilient.map(lambda x: (x.line_numbers + [line_num], x.value))
    # format: (([lines], 'key'), values, values, values, ...),
    method = getattr(unwrapped, methodstr)
    processed = method(*args)
    print("COLLECT!!!!!!! " + str(line_num))
    rewrapped = processed.map(lambda x: StampedValue((x[0][1], x[1:]), x[0][0]))
    print(rewrapped.collect())
    return rewrapped


# Stamp everything in a vanilla RDD with the caller line number ad hoc. Use when instantiating an RDD with parallelize, textFile, etc
def stampNewRDD(resilient):
    frame = inspect.currentframe()
    caller_frame = frame.f_back
    line_num = caller_frame.f_lineno
    return resilient.map(lambda x: StampedValue(x, [line_num]))


# ---------------- THE GRAVEYARD ---------------- 

# run an operation functionally with no tainting. Use in tandem with arithmetic()
# NOTE: not working as expected. ignore for now
# def functionalOp(resilient, methodstr, *args):
#     # look for stamped values in rdd
#     if not resilient.filter(lambda x: isinstance(x, StampedValue)).isEmpty():
#         # print("LINES LIST: " + str(lines_list))
#         precursor = resilient.map(lambda x: (x.value))
#     else:
#         precursor = resilient
    
#     # Apply line numbers from input RDD + caller line to output
#     method = getattr(precursor, methodstr)
#     return method(*args)