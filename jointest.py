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

print("-------------- Join MODDED --------------")

one = StampMath.stampNewRDD(sc.parallelize(['a', 'b', 'c']).map(lambda x: (x, 1)))
two = StampMath.stampNewRDD(sc.parallelize(['b', 'a']).map(lambda x: (x, 1)))

# TODO: need a way to handle joins
joined = StampMath.stampedMeld(one, two, "join")

print(joined.collect())

print()
print("-------------- Join REGULAR --------------")

one1 = sc.parallelize(['a', 'b', 'c']).map(lambda x: (x, 1))
two1 = sc.parallelize(['b', 'a']).map(lambda x: (x, 1))

joined2 = one1.join(two1)

print(joined2.collect())

print()

print("---------- Cartesian MODDED ----------")
one2 = StampMath.stampNewRDD(sc.parallelize(['a', 'b', 'c']))
two2 = StampMath.stampNewRDD(sc.parallelize(['b', 'a']))
cartesian = StampMath.stampedMeld(one2, two2, "cartesian")
print(cartesian.collect())

print()

print("---------- Cartesian REGULAR ----------")
one3 = sc.parallelize(['a', 'b', 'c'])
two3 = sc.parallelize(['b', 'a'])
cartesian1 = one3.cartesian(two3)
print(cartesian1.collect())

print()

print("---------- Union MODDED ----------")
one4 = StampMath.stampNewRDD(sc.parallelize(['a', 'b']))
two4 = StampMath.stampNewRDD(sc.parallelize(['c']))
union4 = StampMath.stampedUnion(one4, two4)
print(union4.collect())



print("---------- Union REGULAR ----------")
one5 = sc.parallelize(['a', 'b'])
two5 = sc.parallelize(['c'])
union5 = one5.union(two5)
print(union5.collect())

print()

print("-------------- Join2 MODDED --------------")
one6 = StampMath.stampNewRDD(sc.parallelize(['a']).map(lambda x: (x, 1)))
two6 = StampMath.stampNewRDD(sc.parallelize(['b', 'a']).map(lambda x: (x, 1)))
three6 = StampMath.stampNewRDD(sc.parallelize(['b', 'a']).map(lambda x: (x, 1)))

joined6 = StampMath.stampedMeld(one6, two6, "fullOuterJoin")
joined6_2 = StampMath.stampedMeld(joined6, three6, "join")

print(joined6_2.collect())

print()

print("-------------- Join2 REGULAR --------------")
one7 = sc.parallelize(['a']).map(lambda x: (x, 1))
two7 = sc.parallelize(['b', 'a']).map(lambda x: (x, 1))
three7 = sc.parallelize(['b', 'a']).map(lambda x: (x, 1))

joined7 = one7.fullOuterJoin(two7)
joined7_2 = joined7.join(three7)

print(joined7_2.collect())