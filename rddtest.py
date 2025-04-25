
import StampMath

from pyspark.sql import SparkSession
from pyspark import SparkConf

import operator



spark = SparkSession.builder \
    .appName("WordCount") \
    .master("local[4]") \
    .getOrCreate()

sc = spark.sparkContext

text = sc.textFile("./data/words.txt")

words = text.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1))


stamped_words = StampMath.oneToMany(words, RDD.map(), lambda word: (word, 1))

counts = words.reduceByKey(operator.add)
print(counts.collect())