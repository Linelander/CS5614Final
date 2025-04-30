from pyspark.sql import SparkSession
from pyspark import SparkConf
import StampMath
import operator
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .appName("WordCount") \
    .master("local[4]") \
    .getOrCreate()
sc = spark.sparkContext

text = StampMath.stampNewRDD(sc.textFile("./data/words.txt"))
words = StampMath.oneToMany(text, 'flatMap', lambda line: line.split(" "))
print("stage1:", str(words.collect()))

print("--------------------------")

words2 = StampMath.oneToOne(words, "map", lambda word: (word, 1))
print("stage2:", str(words2.collect()))

print("--------------------------")

words2 = StampMath.manyToMany(words2, "reduceByKey", lambda x, y: x+y)
print("stage2.5:", str(words2.collect()))

print("--------------------------")

counts = StampMath.manyToMany(words2, "groupByKey")
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
rdd2 = StampMath.stampNewRDD(sc.parallelize([('dog', 1), ('cat', 1), ('dog', 1)]))
grouped2 = StampMath.manyToMany(rdd2, "groupByKey")
print("SEMIFINAL")
print(grouped2.collect())
result2 = StampMath.oneToOne(grouped2, "map", lambda kv: (kv[0], len(kv[1])))
print("FINAL")
print(result2.collect())

# --- Harder test: single RDDs containing data from different places ---
# JOIN TEST

# one = StampMath.stampRDD(sc.parallelize(['a', 'a', 'a', 'b', 'c']).map(lambda x: (x, 1)))
# two = StampMath.stampRDD(sc.parallelize(['b', 'b', 'b', 'a']).map(lambda x: (x, 1)))

# # TODO: need a way to handle joins
# # joined = # TODO

# print("values join: " + str(joined.map(lambda x: (x.value)).collect()))
# print("lines join: " + str(joined.map(lambda x: (x.line_numbers)).collect()))
