import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

start_time = time.time()

if len(sys.argv) != 8:
 print("Neadevkatan broj argumenata.")
 exit(-1)

spark = SparkSession.builder.appName("Demo").master("local[*]").getOrCreate()

# Ucitavanje podataka
df = spark.read.option("inferSchema", True).option("header", True).csv(sys.argv[1])

print("Kolone u skupu podataka:", df.columns)
print("Broj slogova:", df.count())
print("Inicijalni skup podataka:")
df.show(10, False)

latitude_lower = sys.argv[2]
latitude_upper = sys.argv[3]
longitude_lower = sys.argv[4]
longitude_upper = sys.argv[5]
date_lower = sys.argv[6]
date_upper = sys.argv[7]

# Filtriranje redova za datu oblast i dat vremenski interval
filtered_df = df.filter( (df["latitude"] > latitude_lower) & (df["latitude"] < latitude_upper) & (df["longitude"] < longitude_upper) &
                    (df["longitude"] > longitude_lower) & (df["check_in_time"] > date_lower) & (df["check_in_time"] < date_upper))

# Grupisanje po id-evima korisnika
print("Podaci grupisani po korisnicima, za odredjenu oblast i odrjenji vremenski interval")
grouped_df = filtered_df.groupBy("user").count().alias("count")
result = grouped_df.select("user", "count")
result.show(10, False)
result.write.format("csv").option("header", True).save("output/user_area_period")

# Grupisanje po lokacijama korisnika
print("Podaci grupisani po lokacijama, za odredjenu oblast i odrjenji vremenski interval")
grouped_df = filtered_df.groupBy("location_id").count().alias("count")
result = grouped_df.select("location_id", "count")
result.show()
result.write.format("csv").option("header", True).save("output/location_area_period")

# Dodavanje vestacki generisane kolone - koliko je korisnik proveo vremena na nekoj lokaciji
df = df.withColumn("time_spent", floor(rand() * 360))
print("Skup podataka nakon dodate time_spent kolone")
df.show()

# Dodavanje nove kolone koja ce da sadrzi samo informaciji o datumu
df = df.withColumn("check_in_date", to_date(col("check_in_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

# Grupisanje po lokaciji/oblasti i vremenu
grouped_df = df.groupBy("location_id", "check_in_date").agg(
    min("time_spent").alias("min_time_spent"),
    max("time_spent").alias("max_time_spent"),
    avg("time_spent").alias("avg_time_spent"),
    coalesce(stddev("time_spent"), lit(0.0)).alias("stddev_time_spent")
)
print("Statisticki parametri za podatke grupisane po lokacijama i datumima:")
grouped_df.show()
grouped_df.write.format("csv").option("header", True).save("output/stastics_location_date")

# Grupisanje po lokaciji/oblasti i vremenu za odredjenog korisnika
grouped_df = df.groupBy("user", "location_id", "check_in_date").agg(
    min("time_spent").alias("min_time_spent"),
    max("time_spent").alias("max_time_spent"),
    avg("time_spent").alias("avg_time_spent"),
    coalesce(stddev("time_spent"), lit(0.0)).alias("stddev_time_spent")
)
print("Statisticki parametri za podatke koji su grupisani po lokacijama i datumima, za odredjenog korisnika:")
grouped_df.show()
grouped_df.write.format("csv").option("header", True).save("output/stastics_user_location_date")

# Najposecenija lokacija svakog korisnika
print("Najposecenija lokacija za svakog korisnika:")
user_location_count = df.groupBy("user", "location_id").count()
max_visits = user_location_count.groupBy("user").agg(
    max("count").alias("max_visits"),
    first("location_id").alias("most_visited_location")
)
max_visits.show()
max_visits.write.format("csv").option("header", True).save("output/user_max_visits")

# Datum kada je neka lokacija najposecenija
print("Datumi kada je svaka od lokacija bila najposecenija:")
location_date_count = df.groupBy("location_id", "check_in_date").count()
max_visits = location_date_count.groupBy("location_id").agg(
    max("count").alias("max_visits"),
    first("check_in_date").alias("most_busiest_date")
)
max_visits.show()
max_visits.write.format("csv").option("header", True).save("output/location_max_visits")

# Prosecno vreme provedeno na svakoj lokaciji
print("Prosecno vreme provedeno na svakoj lokaciji:")
grouped_df = df.groupBy("location_id").agg(avg("time_spent").alias("avg_time_spent"))
grouped_df.select("location_id", "avg_time_spent").show()
grouped_df.write.format("csv").option("header", True).save("output/location_avgtime")

end_time = time.time()

print("Vreme potrebno za izvrsenje aplikacije:", end_time - start_time)

