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

# text = PyStamp.stampNewRDD(sc.textFile("./data/words.txt"))
# words = PyStamp.oneToMany(text, 'flatMap', lambda line: line.split(" "))
# words2 = PyStamp.oneToOne(words, "map", lambda word: (word, 1))


one = PyStamp.stampNewRDD(sc.parallelize([('b', 2, 2, 2), ('a', 1, 1, 1), ('c', 3, 3, 3)]))

sort = PyStamp.stampSort(one, "sortByKey")

print(sort.collect())

sort2 = PyStamp.stampSort(one, "sortBy", lambda x: x[1])
print(sort2.collect())


# one1 = sc.parallelize([('b', 2, 2, 2), ('a', 1, 1, 1), ('c', 3, 3, 3)])
# sort1 = one1.sortBy(lambda x: x[0])
# print(sort1.collect())


