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

    def __repr__(self):
        return f"${self.value}, {self.line_numbers}$"

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
    rewrapped = processed.map(lambda x: StampedValue((x[0][1], *(y for y in x[1:])), x[0][0]))
    return rewrapped

def manyToMany(resilient, methodstr, *args):
    frame = inspect.currentframe()
    caller_frame = frame.f_back
    line_num = caller_frame.f_lineno

    unwrapped = resilient.map(lambda x: (x.value[0], (x.value[1], x.line_numbers + [line_num])))

    match methodstr:
        case "reduceByKey":
            user_function, = args
            
            def combine_lines(a, b):
                value_a, line_no_a = a
                value_b, line_no_b = b
                return (user_function(value_a, value_b), list(sorted(set(line_no_a + line_no_b))))
            reduced = unwrapped.reduceByKey(combine_lines)
            stamped = reduced.map(lambda x: (StampedValue((x[0], x[1][0]), x[1][1])))
            return stamped
        case "groupByKey":
            grouped = unwrapped.groupByKey()
        
            def collapse(iterable):
                datas = []
                lines = set()
                for d, ls in iterable:
                    datas.append(d)
                    lines.update(ls)
                # for groupByKey, we collect all data into a list;
                # if you wanted a different aggregate, swap this out.
                return (datas, sorted(lines))
            
            processed = grouped.mapValues(collapse)
            stamped = processed.map(lambda x: (StampedValue((x[0], x[1][0]), x[1][1])))
            return stamped
        
        case _:
            print("Method currently not supported.")
            exit(1)            

def stampNewRDD(resilient):
    frame = inspect.currentframe()
    caller_frame = frame.f_back
    line_num = caller_frame.f_lineno
    return resilient.map(lambda x: StampedValue(x, [line_num]))


# For when the user wants to join rdd1 with rdd2. They supply the join as a str
def stampedJoin(rdd1, rdd2, joinStr):
    # Get caller line number
    frame = inspect.currentframe()
    caller_frame = frame.f_back
    line_num = caller_frame.f_lineno

    # Unwrap RDDs of stamped values
    unwrapped1 = rdd1.map(lambda x: (x.value[0], (x.value[1], x.line_numbers + [line_num])))
    unwrapped2 = rdd2.map(lambda x: (x.value[0], (x.value[1], x.line_numbers + [line_num])))

    # Get join attribute from rdd1
    method = getattr(unwrapped1, joinStr)
    joined = method(rdd2)
    joinedRewrapped = joined.map(lambda x: StampedValue((x[0][1], *(y for y in x[1:])), x[0][0]))
    return joinedRewrapped