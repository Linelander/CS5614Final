from pyspark.sql import SparkSession
from pyspark import SparkConf
import operator
import StampMath

spark = SparkSession.builder \
    .appName("WordCount") \
    .master("local[4]") \
    .getOrCreate()

sc = spark.sparkContext

text = sc.textFile("./data/words.txt")

words = text.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1))

print(words.reduceByKey(operator.add).collect())