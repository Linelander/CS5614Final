# NOTE: This isn't syntactically correct. This is just to test main's parsing ability



tfFareAvg = tfFareGroup.map(lambda x: (x[0],\
sum(x[1])/sum(1 for x in x[1])))




# Demo job 1 goes here
x = "test1"
x = "test2"
x = "test3"



from pyspark.sql import SparkSession

spark = SparkSession.\
        builder.\
        appName("pyspark-notebook").\
        master("spark://spark-master:7077").\
        config("spark.executor.memory", "512m").\
        getOrCreate()

sc = spark.sparkContext

# Q1.1 A: Write a data processing pipeline in pyspark to calculate the average fare amount in each of the
# fare_condition types using the ticket_flights.csv file. Since “average” is a non-associative operation,
# using reduceByKey will give you incorrect output. You are welcome to use any of the following operators: textFile,
# map, flatmap, filter, groupByKey, and any of the actions, i.e., collect, SaveAsTextFille, etc.

ticketFlights = sc.textFile("/data/ticket_flights.csv")

# Index 2 is fare conditions. Make that the key

tfFare = ticketFlights.map\
(lambda x: (x.split(",")[2], int(x.split(",")[3])))
# condition, amount
# 0          3

# tfFare.collect()

tfFareGroup = tfFare.groupByKey()

# check:
# tfFareGroup.collect()

# This gives us iterables. We have to divide the sum of each iterable by the number of things
# that iterable contains.

tfFareAvg = tfFareGroup.map(lambda x: (x[0],\
    sum(x[1])/sum(1 for x in x[1])))
tfFareAvg.collect()