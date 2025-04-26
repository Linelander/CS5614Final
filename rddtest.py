from pyspark.sql import SparkSession
from pyspark import SparkConf

import StampMath

import operator



spark = SparkSession.builder \
    .appName("WordCount") \
    .master("local[4]") \
    .getOrCreate()

sc = spark.sparkContext

text = StampMath.stampRDD(sc.textFile("./data/words.txt"))

# words = text.flatMap(lambda line: line.split(" "))
# words1 = words
# print(words1.collect())

words = StampMath.asymOp(text, 'flatMap', lambda line: line.split(" "))
print("values from 1: " + str(words.map(lambda x: (x.value)).collect()))
print("lines from 1: " + str(words.map(lambda x: (x.line_numbers)).collect()))

print("--------------------------")

# .map(lambda word: (word, 1))
words2 = StampMath.asymOp(words, "map", lambda word: (word, 1))
print("values from 2: " + str(words2.map(lambda x: (x.value)).collect()))
print("lines from 2: " + str(words2.map(lambda x: (x.line_numbers)).collect()))



# counts = words2.reduceByKey(lambda x, y: StampMath.arithmetic(operator.add, x, y))


counts = StampMath.asymOp(words2, "reduceByKey", lambda x, y: (StampMath.arithmetic(operator.add, x, y)))

print("values final: " + str(counts.map(lambda x: (x.value)).collect()))
print("lines final: " + str(counts.map(lambda x: (x.line_numbers)).collect()))