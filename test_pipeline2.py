from pyspark.sql import SparkSession
from pyspark import SparkConf
import PyStamp
import faultLocalization
import os
import sys


# Lines: 10-30 - TESTING A PIPELINE WITH MULTIPLE BAD LINES
def example2(input_rdd):
    stamped_rdd = PyStamp.stampNewRDD(input_rdd)
    count_rdd = PyStamp.stampMap(stamped_rdd, "map", lambda x: (x, 1))

    # Branching logic
    checkBrown = PyStamp.stampMap(count_rdd, "map", lambda x: (x[0].lower() == "brown"))
    hasBrown = any([tfval.value for tfval in checkBrown.collect()])
    
    # This is meant to filter out "brown"
    if hasBrown:
        agg_prep = PyStamp.stampFilter(count_rdd, lambda x: x[0].lower() == "brown") # This line is wrong
    else:
        agg_prep = PyStamp.adHocStamp(count_rdd) # trying something new here: adHocStamping assignments

    # Going to change the name of blue so it won't be counted properly
    agg_prep = PyStamp.stampMap(agg_prep, "map", lambda x: ("bluee", x[1]) if x[0] == "blue" else x) # causes errors

    agg_rdd = PyStamp.manyToMany(agg_prep, "reduceByKey", lambda x, y: x+y)
    result = agg_rdd.collect()
    
    return [x.value for x in result], list(sorted(set(line for y in result for line in y.line_numbers)))


if __name__ == "__main__":
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = SparkSession.builder \
        .appName("WordCount2") \
        .master("local[4]") \
        .getOrCreate()
    sc = spark.sparkContext


    test1_rdd = sc.parallelize(['red', 'blue', 'blue'])
    test2_rdd = sc.parallelize(['brown', 'blue', 'blue'])
    test3_rdd = sc.parallelize(['red', 'yellow', 'yellow'])

    # Tests
    test1_result, test1_lines = example2(test1_rdd)
    sorted_test1 = sorted(test1_result, key=lambda x: x[0])
    print("test1 output", sorted_test1, test1_lines)
    test1_bool = test1_result == [('blue', 2), ('red', 1)]
    print(test1_lines)


    test2_result, test2_lines = example2(test2_rdd)
    sorted_test2 = sorted(test2_result, key=lambda x: x[0])
    print("test2 output", sorted_test2, test2_lines)
    test2_bool = test2_result == [('blue', 2)]
    print(test2_lines)

    test3_result, test3_lines = example2(test3_rdd)
    sorted_test3 = sorted(test3_result, key=lambda x: x[0])
    print("test3 output", sorted_test3, test3_lines)
    test3_bool = test3_result == [('yellow', 2), ('red', 1)]
    print(test3_lines)
    
    test_data_results = []
    line_nos = [11, 12, 15, 16, 19, 20, 21, 22, 25, 27, 28, 30]
    code_lines = [
        "stamped_rdd = PyStamp.stampNewRDD(input_rdd)"
        "count_rdd = PyStamp.stampMap(stamped_rdd, 'map', lambda x: (x, 1))"
        "checkBrown = PyStamp.stampMap(count_rdd, 'map', lambda x: (x[0].lower() == 'brown'))"
        "hasBrown = any([tfval.value for tfval in checkBrown.collect()])"
        "if hasBrown:"
            "agg_prep = PyStamp.stampFilter(count_rdd, lambda x: x[0].lower() == 'brown') # This line is wrong"
        "else:"
            "agg_prep = PyStamp.adHocStamp(count_rdd) # trying something new here"
        "agg_prep = PyStamp.stampMap(agg_prep, 'map', lambda x: ('blue_', x[1]) if x[0] == 'blue' else x)"
        "agg_rdd = PyStamp.manyToMany(agg_prep, 'reduceByKey', lambda x, y: x+y)"
        "result = agg_rdd.collect()"
        "return [x.value for x in result], list(sorted(set(line for y in result for line in y.line_numbers)))"
    ]

    faultLocalization.recordTestExecution(test_data_results, test1_lines, test1_bool)
    faultLocalization.recordTestExecution(test_data_results, test2_lines, test2_bool)
    faultLocalization.recordTestExecution(test_data_results, test3_lines, test3_bool)


    faultLocalization.printSuspiciousnessScores(code_lines, test_data_results, line_nos)