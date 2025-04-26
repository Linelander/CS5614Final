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

words = StampMath.simpleAsymOp(text, 'flatMap', lambda line: line.split(" "))
print("values from 1: " + str(words.map(lambda x: (x.value)).collect()))
print("lines from 1: " + str(words.map(lambda x: (x.line_numbers)).collect()))

print("--------------------------")

words2 = StampMath.simpleAsymOp(words, "map", lambda word: (word, 1))
print("values from 2: " + str(words2.map(lambda x: (x.value)).collect()))
print("lines from 2: " + str(words2.map(lambda x: (x.line_numbers)).collect()))

counts = StampMath.simpleAsymOp(words2, "reduceByKey", lambda x, y: x+y)
print("values final: " + str(counts.map(lambda x: (x.value)).collect()))
print("lines final: " + str(counts.map(lambda x: (x.line_numbers)).collect()))


# --- Harder test: single RDDs containing data from different places ---
# JOIN TEST

one = StampMath.stampRDD(sc.parallelize(['a', 'a', 'a', 'b', 'c']).map(lambda x: (x, 1)))
two = StampMath.stampRDD(sc.parallelize(['b', 'b', 'b', 'a']).map(lambda x: (x, 1)))

# TODO: need a way to handle joins
joined = # TODO

print("values join: " + str(joined.map(lambda x: (x.value)).collect()))
print("lines join: " + str(joined.map(lambda x: (x.line_numbers)).collect()))
