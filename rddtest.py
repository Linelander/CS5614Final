from pyspark.sql import SparkSession
from pyspark import SparkConf
import PyStamp
import os
import sys


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .appName("WordCount") \
    .master("local[4]") \
    .getOrCreate()
sc = spark.sparkContext

text = PyStamp.stampNewRDD(sc.textFile("./data/words.txt"))
words = PyStamp.oneToMany(text, 'flatMap', lambda line: line.split(" "))
print("stage1:", str(words.collect()))

print("--------------------------")

words2 = PyStamp.stampMap(words, "map", lambda word: (word, 1))
print("stage2:", str(words2.collect()))

print("--------------------------")

words2 = PyStamp.manyToMany(words2, "reduceByKey", lambda x, y: x+y)
print("stage2.5:", str(words2.collect()))

print("--------------------------")

counts = PyStamp.manyToMany(words2, "groupByKey")
print("stage3:", str(counts.collect()))

print()




print("-------- NORMAL --------")

text = sc.textFile("./data/words.txt")
words = text.flatMap(lambda line: line.split(" "))
print("stage1:", str(words.collect()))

print("--------------------------")

words2 = words.map(lambda word: (word, 2))
print("stage2:", str(words2.collect()))

print("--------------------------")

counts = words2.groupByKey()
print("stage3:", str(counts.collect()))








print()




print("--------------- REGULAR TEST 2 --------------")
rdd = sc.parallelize([('dog', 1), ('cat', 1), ('dog', 1)])
grouped = rdd.groupByKey()
print("SEMIFINAL")
print(grouped.collect())
print("FINAL")
result = print(grouped.map(lambda kv: (kv[0], len(kv[1]))).collect())
# Output: [('dog', 2), ('cat', 1)]

print()

print("--------------- MODDED TEST 3 --------------")
rdd2 = PyStamp.stampNewRDD(sc.parallelize([('dog', 1), ('cat', 1), ('dog', 1)]))
grouped2 = PyStamp.manyToMany(rdd2, "groupByKey")
print("SEMIFINAL")
print(grouped2.collect())
result2 = PyStamp.stampMap(grouped2, "map", lambda kv: (kv[0], len(kv[1])))
print("FINAL")
print(result2.collect())


print("----------- MANY-TO-MANY TEST, AGGREGATE FOLD AND COMBINE ----------")
rdd_many1reg = sc.parallelize([("a", 1), ("b", 1), ("a", 2)])
rdd_many1 = PyStamp.stampNewRDD(sc.parallelize([("a", 1), ("b", 1), ("a", 2)]))
seqFunc = (lambda x, y: (x[0] + y, x[1] + 1))
combFunc = (lambda x, y: (x[0] + y[0], x[1] + y[1]))
grouped_many = PyStamp.manyToMany(rdd_many1, "aggregateByKey", (0, 0), seqFunc, combFunc)
print(sorted(rdd_many1reg.aggregateByKey((0, 0), seqFunc, combFunc).collect()))
print(grouped_many.collect())

def to_list(a):
    return [a]

def append(a, b):
    a.append(b)
    return a

def extend(a, b):
    a.extend(b)
    return a

grouped_many = PyStamp.manyToMany(rdd_many1, "combineByKey", to_list, append, extend)
print(grouped_many.collect())
print(sorted(rdd_many1reg.combineByKey(to_list, append, extend).collect()))
exit(1)


print("----------- REGULAR FLATMAP TEST ----------")
rdd2 = sc.parallelize(['str', 'abc', '123'])
# print(rdd2.flatMap(lambda x: range(1, x)).collect())
# [1, 1, 1, 2, 2, 3]
print(rdd2.flatMap(lambda x: [(x, x), (x, x)]).collect())
# [(2, 2), (2, 2), (3, 3), (3, 3), (4, 4), (4, 4)]
print()

print("----------- MODDED FLATMAP TEST ----------")
rdd3 = PyStamp.stampNewRDD(sc.parallelize(['str', 'abc', '123']))
# print(PyStamp.stampMap(rdd3, "flatMap", lambda x: range(1, x)).collect())
print(PyStamp.stampMap(rdd3, "flatMap", lambda x: [(x, x), (x, x)]).collect())

print()



# NOTE: it seems like an error, but recall vanilla pyspark flatmap messes up strings. Our version needs to do that too.
# faithful to the original
print("------------ STRING CHOPPING TEST ---------------")
rdd = sc.parallelize(['alpha', 'beta'])
stamped = PyStamp.stampNewRDD(rdd)
chopped = PyStamp.stampMap(stamped, "flatMap", lambda x: x.upper())
print(chopped.collect())

print()

print("------------ REGULAR STRING CHOPPING TEST ---------------")
rdd = sc.parallelize(['alpha', 'beta'])
result = rdd.flatMap(lambda x: x.upper())
print(result.collect())