from pyspark.sql import SparkSession
from pyspark import SparkConf

import StampMath

import operator



spark = SparkSession.builder \
    .appName("WordCount") \
    .master("local[4]") \
    .getOrCreate()

sc = spark.sparkContext

text = sc.textFile("./data/words.txt")

# words = text.flatMap(lambda line: line.split(" "))
# words1 = words
# print(words1.collect())

words = StampMath.asymOperation(text, 'flatMap', lambda line: line.split(" "))
print("values from 1: " + str(words.map(lambda x: (x.value)).collect()))
print("lines from 1: " + str(words.map(lambda x: (x.line_numbers)).collect()))

print("--------------------------")

# .map(lambda word: (word, 1))
words2 = StampMath.asymOperation(words, "map", lambda word: (word, 1))
print("values from 2: " + str(words2.map(lambda x: (x.value)).collect()))
print("lines from 2: " + str(words2.map(lambda x: (x.line_numbers)).collect()))


# stamped_words2 = StampMath.asymOperation(words, "map()", lambda word: (word, 1))





# counts = words.reduceByKey(operator.add)

# # counts = StampMath.arithmetic






# print(counts.collect())
