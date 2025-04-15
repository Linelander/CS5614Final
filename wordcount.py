from pyspark.sql import SparkSession
from pyspark import SparkConf
import operator
import StampMath
import re

spark = SparkSession.builder \
    .appName("WordCount") \
    .master("local[4]") \
    .getOrCreate()

sc = spark.sparkContext

text = sc.textFile("./data/words.txt")

# words = text.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1))

# print(words.reduceByKey(operator.add).collect())

def pipeline1():
    """
    count every word twice
    """
    words = text.flatMap(lambda line: line.split(" ")).flatMap(lambda word: [(word, 1), (word, 1)])
    counts = words.reduceByKey(operator.add)

    print(counts.collect())

def pipeline2():
    """
    count lowercase and uppercase as the same word
    """
    words = text.flatMap(lambda line: line.split(" ")).map(lambda word: (words, 1))
    counts = words.reduceByKey(operator.add)

    print(counts.collect())

def pipeline3():
    """
    split every char
    """
    words = text.flatMap(lambda line: line.lower()).map(lambda word: (word, 1))
    counts = words.reduceByKey(operator.add)

    print(counts.collect())


def correct_pipeline():
    """
    lowercases all words & takes in account for punctuation
    """
    words = text.flatMap(lambda line: re.findall(r'\b\w+\b', line.lower())).map(lambda word: (word, 1))
    counts = words.reduceByKey(operator.add)

    print(counts.collect())


pipeline1()
pipeline2()
pipeline3()
# correct_pipeline()