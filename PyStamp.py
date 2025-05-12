import inspect

# Will hold an RDD and relevant line numbers
class StampedValue:
    def __init__(self, val, line_num):
        self.value = val
        self.line_numbers = line_num

    def __repr__(self):
        return f"${self.value}, {self.line_numbers}$"

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

# NOTE: supports map and flatmap
def stampMap(resilient, methodstr, argF):
    frame = inspect.currentframe()
    caller_frame = frame.f_back
    line_num = caller_frame.f_lineno

    # Deals with the stamped value directly (no unpacking required)
    def extendStamp(stamped):
        result = argF(stamped.value)  # argF is user's mapping function
        new_lines = stamped.line_numbers + [line_num]

        if methodstr == "flatMap": # don't cut up strings, etc
            return [StampedValue(value, new_lines) for value in result] # flatmap returns a list of stamped values (chops strings)
        else:
            return StampedValue(result, new_lines)

    # Call extendStamp on everything in the RDD
    return resilient.map(extendStamp) if methodstr != "flatMap" else resilient.flatMap(extendStamp)

def stampFilter(resilient, argF):
    frame = inspect.currentframe()
    caller_frame = frame.f_back
    line_num = caller_frame.f_lineno

    def evaluate(val):
        if argF(val.value):
            return StampedValue(val.value, val.line_numbers + [line_num])
        else:
            return
        
    rewrapped = resilient.map(evaluate).filter(lambda x: x is not None)
    return rewrapped
    

def manyToMany(resilient, methodstr, *args):
    frame = inspect.currentframe()
    caller_frame = frame.f_back
    line_num = caller_frame.f_lineno

    unwrapped = resilient.map(lambda x: (x.value[0], (x.value[1], x.line_numbers + [line_num])))

    match methodstr:
        # RDD.reduceByKey(func: Callable[[V, V], V], numPartitions: Optional[int] = None, 
        # partitionFunc: Callable[[K], int] = <function portable_hash>) → pyspark.rdd.RDD[Tuple[K, V]][source]
        case "reduceByKey":
            user_function, = args
            args = [lambda a, b: (user_function(a[0], b[0]), sorted(set(a[1] + b[1])))]

        # RDD.aggregateByKey(zeroValue: U, seqFunc: Callable[[U, V], U], combFunc: Callable[[U, U], U], 
        # numPartitions: Optional[int] = None, partitionFunc: Callable[[K], int] = <function portable_hash>) → pyspark.rdd.RDD[Tuple[K, U]]
        case "aggregateByKey":
            zeroValue, seqFunc, combFunc = args
            args = [(zeroValue, []), lambda acc, val: (seqFunc(acc[0], val[0]), sorted(set(acc[1] + val[1]))),
                    lambda a, b: (combFunc(a[0], b[0]), sorted(set(a[1] + b[1])))]
        
        # RDD.foldByKey(zeroValue: V, func: Callable[[V, V], V], numPartitions: Optional[int] = None, 
        # partitionFunc: Callable[[K], int] = <function portable_hash>) → pyspark.rdd.RDD[Tuple[K, V]]
        case "foldByKey":
            zeroValue, func = args
            args = [(zeroValue, []), lambda acc, val: (func(acc[0], val[0]), sorted(set(acc[1] + val[1])))]
            
        # RDD.combineByKey(createCombiner: Callable[[V], U], mergeValue: Callable[[U, V], U], mergeCombiners: Callable[[U, U], U], 
        # numPartitions: Optional[int] = None, partitionFunc: Callable[[K], int] = <function portable_hash>) → pyspark.rdd.RDD[Tuple[K, U]]
        # Three functions???
        # createCombiner = handling of first val per key
        # mergeValue = handling val merged into first val: val --> accumulator acc
        # mergeCombiners = handling merge vals from different partitions
        case "combineByKey":
            createCombiner, mergeValue, mergeCombiners = args
            args = [lambda val: (createCombiner(val[0]), val[1]), 
                    lambda acc, val: (mergeValue(acc[0], val[0]), sorted(set(acc[1] + val[1]))),
                    lambda a, b: (mergeCombiners(a[0], b[0]), sorted(set(a[1] + b[1])))]

        # RDD.groupByKey(numPartitions: Optional[int] = None, partitionFunc: 
        # Callable[[K], int] = <function portable_hash>) → pyspark.rdd.RDD[Tuple[K, Iterable[V]]]
        case "groupByKey" | _:
            pass

    # Actual func execution happens here
    method = getattr(unwrapped, methodstr)
    processed = method(*args)

    # Postprocessing, unwrapping Iterabble[V] for StampedValue processing
    if methodstr == "groupByKey":
        processed = processed.mapValues(lambda iterable: ([val for val, _ in iterable], 
                                                          sorted({line for _, lines in iterable for line in lines})))

    rewrapped = processed.map(lambda x: (StampedValue((x[0], x[1][0]), x[1][1])))
    return rewrapped



# NOTE: User passes the sort they want + args
def stampSort(resilient, methodStr, *args, **kwargs):
    frame = inspect.currentframe()
    caller_frame = frame.f_back
    line_num = caller_frame.f_lineno

    unwrapped = resilient.map(lambda x: (x.value, x.line_numbers+[line_num])) # line numbers held at the end
    # format ((original), lines)

    method = getattr(unwrapped, methodStr)

    processed = method(*args, **kwargs)

    # rewrap
    rewrapped = processed.map(lambda x : (x[0], x[1])) # line numbers held at the end

    return rewrapped


# Turns newly created vanilla RDDs into RDDs of stamped values. Use inline with parallelize, textFile, etc.
def stampNewRDD(resilient):
    frame = inspect.currentframe()
    caller_frame = frame.f_back
    line_num = caller_frame.f_lineno
    return resilient.map(lambda x: StampedValue(x, [line_num]))


# Handles joins and cartesian
def stampMeld(rdd1, rdd2, methodStr):
    frame = inspect.currentframe()
    caller_frame = frame.f_back
    line_num = caller_frame.f_lineno

    def wrap(pair):
        print("pair key: " + str(pair[0]))
        print("pair value: " + str(pair[1]))
        key, values = pair

        # values contains the two things that were joined on the same key

        # Handle None values from outer joins
        # left is at [0], right is at [1]
        # use this function after pyspark takes care of the join (chosen by user)


        left = values[0] if values[0] is not None else (None, [])
        right = values[1] if values[1] is not None else (None, [])

        value_left, lines_left = left
        value_right, lines_right = right

        # Tuple valueLeft and value_right to match vanilla pyspark
        # Lay them out the same way pyspark does normally (key off to the side, left and right val tupled together)
        accrued_values = (key, (value_left, value_right))
        accrued_lines = sorted(set(lines_left + lines_right))
        return StampedValue(accrued_values, accrued_lines)

    if methodStr == "cartesian":
        unwrapped1 = rdd1.map(lambda x: (x.value, x.line_numbers + [line_num]))
        unwrapped2 = rdd2.map(lambda x: (x.value, x.line_numbers + [line_num]))

        cartesian = unwrapped1.cartesian(unwrapped2)

        return cartesian.map(lambda x: 
            StampedValue((x[0][0], x[1][0]), sorted(set(x[0][1] + x[1][1])))
        )

    else:
        unwrapped1 = rdd1.map(lambda x: (x.value[0], (x.value[1], x.line_numbers + [line_num])))
        unwrapped2 = rdd2.map(lambda x: (x.value[0], (x.value[1], x.line_numbers + [line_num])))

        method = getattr(unwrapped1, methodStr)
        joined = method(unwrapped2)

        return joined.map(wrap)


# Very simple parser for union
def stampUnion(rdd1, rdd2):
    frame = inspect.currentframe()
    caller_frame = frame.f_back
    line_num = caller_frame.f_lineno
    unioned = rdd1.union(rdd2)
    return unioned.map(lambda x: StampedValue(x.value, x.line_numbers + [line_num]))

# Add the current line number to all StampedValues in RDD resilient
def adHocStamp(resilient):
    frame = inspect.currentframe()
    caller_frame = frame.f_back
    line_num = caller_frame.f_lineno
    
    return resilient.map(lambda x: StampedValue(x.value, x.line_numbers + [line_num]))
