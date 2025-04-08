from pyspark.sql import SparkSession
from pyspark import SparkConf
import csv
from datetime import datetime
from math import radians, cos, sin, asin, sqrt, comb





from pyspark.sql import SparkSession

spark = SparkSession.\
        builder.\
        appName("pyspark-notebook").\
        master("local[4]").\
        getOrCreate()

sc = spark.sparkContext



# pre-written code given to us for hw4

def parse_csv(path):
    return sc.textFile(path).mapPartitionsWithIndex(
        lambda idx, it: iter(list(it)[1:]) if idx == 0 else it
    ).map(lambda line: next(csv.reader([line])))

def parse_time(ts):
    return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")


# Haversine distance in miles
def haversine(lat1, lon1, lat2, lon2):
    R = 3956  # radius of Earth in miles
    dlat, dlon = radians(lat2 - lat1), radians(lon2 - lon1)
    a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
    return 2 * R * asin(sqrt(a))

# Total number of landing sequence (at most 4 planes can land simultaneously)
def sequence(n):
    if n == 0: return 1
    return sum(comb(n - 1, g - 1) * sequence(n - g) for g in range(1, min(5, n) + 1))


    # Load airports and flights
airports_rdd = parse_csv("../data/airports_data.csv").map(
    lambda row: (row[0], (float(row[3]), float(row[4])))  # airport_code -> (lat, lon)
).cache()

flights_rdd = parse_csv("../data/flights.csv").filter(lambda x : x[9] !='').map(
    lambda row: (row[5], parse_time(row[9]))  # (arrival_airport, actual_arrival)
)


# Step 1: Get top 10 airports by arrival volume
top_airports = flights_rdd.map(lambda x: (x[0], 1)) \
                          .reduceByKey(lambda a, b: a + b) \
                          .takeOrdered(10, key=lambda x: -x[1])
top_airport_codes = set([a[0] for a in top_airports])
top_airport_codes_bc = sc.broadcast(top_airport_codes)



# Step 2: Create (top_airport, [nearby_airports_within_radius])
X = 200  # miles
top_airports_rdd = airports_rdd.filter(lambda x: x[0] in top_airport_codes_bc.value)

# Join each top airport with every airport to compute distance
region_airport_map = top_airports_rdd.cartesian(airports_rdd) \
    .filter(lambda pair: haversine(pair[0][1][0], pair[0][1][1], pair[1][1][0], pair[1][1][1]) <= X) \
    .map(lambda pair: (pair[0][0], pair[1][0]))  # (region_center_airport, nearby_airport)



# Step 3: Invert map to  (airport, [region_centers])
airport_to_regions = region_airport_map.map(lambda x: (x[1], x[0])) \
                                       .groupByKey().mapValues(set)




# Step 4: Join flight arrivals with region mapping

flights_by_region = flights_rdd.join(airport_to_regions)




# Step 5: Create hourly keys (region_center, hour) → 1 for each region the arrival belongs to
def assign_to_regions(record):
    airport, (arrival_time, regions) = record
    date = arrival_time.strftime("%Y-%m-%d")
    hour = arrival_time.strftime("%H")
    return [((region, date, hour), 1) for region in regions]

region_hour_counts = flights_by_region.flatMap(assign_to_regions) \
                                      .reduceByKey(lambda x, y: x + y)
# Output: (region_center_airport, date, hour) count_of_landings




from pyspark.rdd import portable_hash

num_partitions = 50
def region_hour_partitioner(key):
    region, data, hour = key
    return portable_hash(region) % num_partitions

# Enforces 50 partitions based on the airport code.
region_hour_counts = region_hour_counts.partitionBy(num_partitions, region_hour_partitioner) 


# Step 6: Compute Count of Landing Sequences
region_hour_date_seq = region_hour_counts.map(lambda x: (x[0][0], x[0][1], x[1], sequence(x[1])))
# Output: (region_center_airport, date, hour, count_of_landings, landing_sequences)
# for item in region_hour_date_seq.collect():
#     print(item)


# print(type(region_hour_date_seq))