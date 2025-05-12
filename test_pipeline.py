from pyspark.sql import SparkSession
from pyspark import SparkConf
import PyStamp
import faultLocalization
import operator
import os
import sys

# Lines: 10-25
def example1(input_rdd):
    stamped_rdd = PyStamp.stampNewRDD(input_rdd)
    count_rdd = PyStamp.stampMap(stamped_rdd, "map", lambda x: (x, 1))

    # Branching logic
    checkBrown = PyStamp.stampMap(count_rdd, "map", lambda x: (x[0].lower() == "brown"))
    hasBrown = any([tfval.value for tfval in checkBrown.collect()])
    
    # This is meant to filter out "brown"
    if hasBrown:
        agg_prep = PyStamp.stampFilter(count_rdd, lambda x: x[0].lower() == "brown") # This line is wrong
    else:
        agg_prep = count_rdd
        
    agg_rdd = PyStamp.manyToMany(agg_prep, "reduceByKey", lambda x, y: x+y)
    result = agg_rdd.collect()
    
    return [x.value for x in result], list(sorted(set(line for y in result for line in y.line_numbers)))


if __name__ == "__main__":
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = SparkSession.builder \
        .appName("WordCount") \
        .master("local[4]") \
        .getOrCreate()
    sc = spark.sparkContext
    
    test1_rdd = sc.parallelize(['red', 'blue', 'blue'])
    test2_rdd = sc.parallelize(['red', 'red', 'blue', 'blue', 'brown'])
    test3_rdd = sc.parallelize(['blue', 'blue', 'blue'])
    test4_rdd = sc.parallelize(['blue', 'blue', 'blue', 'red', 'brown', 'brown'])
    test5_rdd = sc.parallelize(['blue', 'blue', 'red', 'green', 'yellow', 'green'])
    test6_rdd = sc.parallelize(['blue', 'blue', 'blue', 'red', 'brown', 'yellow'])
    
    test1_result, test1_lines = example1(test1_rdd)
    sorted_test1 = sorted(test1_result, key=lambda x: x[0])
    print("test1 output", sorted_test1, test1_lines)
    test1_bool = test1_result == [('blue', 2), ('red', 1)]
    test2_result, test2_lines = example1(test2_rdd)
    sorted_test2 = sorted(test2_result, key=lambda x: x[0])
    print("test2 output", sorted_test2, test2_lines)
    test2_bool = test2_result == [('blue', 2), ('red', 2)]
    test3_result, test3_lines = example1(test3_rdd)
    sorted_test3 = sorted(test3_result, key=lambda x: x[0])
    print("test3 output", sorted_test3, test3_lines)
    test3_bool = test3_result == [('blue', 3)]
    test4_result, test4_lines = example1(test4_rdd)
    sorted_test4 = sorted(test4_result, key=lambda x: x[0])
    print("test4 output", sorted_test4, test4_lines)
    test4_bool = test4_result == [('blue', 3), ('red', 1)]
    test5_result, test5_lines = example1(test5_rdd)
    sorted_test5 = sorted(test5_result, key=lambda x: x[0])
    print("test5 output", sorted_test5, test5_lines)
    test5_bool = test5_result == [('blue', 2), ('green', 2), ('red', 1), ('yellow', 1)]
    test6_result, test6_lines = example1(test6_rdd)
    sorted_test6 = sorted(test6_result, key=lambda x: x[0])
    print("test6 output", sorted_test6, test6_lines)
    test6_bool = test6_result == [('blue', 3), ('red', 1), ('yellow', 1)]
    
    test_data_results = []
    line_nos = [11, 12, 15, 16, 19, 20, 21, 22, 24, 25, 27]
    code_lines = [
        "stamped_rdd = PyStamp.stampNewRDD(input_rdd)",
        "count_rdd = PyStamp.stampMap(stamped_rdd, 'map', lambda x: (x, 1))",
        "checkBrown = PyStamp.stampMap(count_rdd, 'map', lambda x: (x[0].lower() == 'brown'))",
        "hasBrown = any([tfval.value for tfval in checkBrown.collect()])",
        "if hasBrown:",
        "    agg_prep = PyStamp.stampFilter(count_rdd, lambda x: x[0].lower() == 'brown')  # This line is wrong",
        "else:",
        "    agg_prep = count_rdd",
        "agg_rdd = PyStamp.manyToMany(agg_prep, 'reduceByKey', lambda x, y: x + y)",
        "result = agg_rdd.collect()",
        "return [x.value for x in result], list(sorted(set(line for y in result for line in y.line_numbers)))"
    ]

    faultLocalization.recordTestExecution(test_data_results, test1_lines, test1_bool)
    faultLocalization.recordTestExecution(test_data_results, test2_lines, test2_bool)
    faultLocalization.recordTestExecution(test_data_results, test3_lines, test3_bool)
    faultLocalization.recordTestExecution(test_data_results, test4_lines, test4_bool)
    faultLocalization.recordTestExecution(test_data_results, test5_lines, test5_bool)
    faultLocalization.recordTestExecution(test_data_results, test6_lines, test6_bool)
    
    faultLocalization.printSuspiciousnessScores(code_lines, test_data_results, line_nos)