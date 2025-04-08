from pyspark.sql import SparkSession
from pyspark import SparkConf


# A object containing an RDD and a list of its ancestry as a list of line numbers

class StampedRDD(pyspark.rdd.RDD):
    lineNo = []

    # Attempt at a catch-all alternative to overloading

    # NOTE: 

    def stamp(currLineStamp, funcString, *params):
        lineNo += currLine
        self.func(*params)
        return self.map(lambda x: (x, lineNo))

            # NOTE: How do we get the line number?

    def getLineNo():
        return lineNo