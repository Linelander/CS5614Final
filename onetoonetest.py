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

# text = StampMath.stampNewRDD(sc.textFile("./data/words.txt"))
# words = StampMath.oneToMany(text, 'flatMap', lambda line: line.split(" "))
# words2 = StampMath.oneToOne(words, "map", lambda word: (word, 1))


one = StampMath.stampNewRDD(sc.parallelize([('b', 2, 2, 2), ('a', 1, 1, 1), ('c', 3, 3, 3)]))

sort = StampMath.stampSort(one, "sortByKey")

print(sort.collect())

sort2 = StampMath.stampSort(one, "sortBy", lambda x: x[1])
print(sort2.collect())


# one1 = sc.parallelize([('b', 2, 2, 2), ('a', 1, 1, 1), ('c', 3, 3, 3)])
# sort1 = one1.sortBy(lambda x: x[0])
# print(sort1.collect())


