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


# --- Harder test: single RDDs containing data from different places ---
# JOIN TEST

print("-------------- MODDED --------------")

one = StampMath.stampNewRDD(sc.parallelize(['a', 'b', 'c']).map(lambda x: (x, 1)))
two = StampMath.stampNewRDD(sc.parallelize(['b', 'a']).map(lambda x: (x, 1)))

# TODO: need a way to handle joins
joined = StampMath.stampedJoin(one, two, "join")

print(joined.collect())

print()
print("-------------- REGULAR --------------")

# one = StampMath.stampNewRDD(sc.parallelize(['a', 'a', 'a', 'b', 'c']).map(lambda x: (x, 1)))
# two = StampMath.stampNewRDD(sc.parallelize(['b', 'b', 'b', 'a']).map(lambda x: (x, 1)))
