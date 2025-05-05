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

print(words)
