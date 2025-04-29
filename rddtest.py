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

words2 = StampMath.oneToOne(words, "map", lambda word: (word, 2))
print("stage2:", str(words2.collect()))

print("--------------------------")

counts = StampMath.manyToMany(words2, "groupByKey", lambda x, y: x*2*y)
print("stage3:", str(counts.collect()))


# --- Harder test: single RDDs containing data from different places ---
# JOIN TEST

# one = StampMath.stampRDD(sc.parallelize(['a', 'a', 'a', 'b', 'c']).map(lambda x: (x, 1)))
# two = StampMath.stampRDD(sc.parallelize(['b', 'b', 'b', 'a']).map(lambda x: (x, 1)))

# # TODO: need a way to handle joins
# # joined = # TODO

# print("values join: " + str(joined.map(lambda x: (x.value)).collect()))
# print("lines join: " + str(joined.map(lambda x: (x.line_numbers)).collect()))
