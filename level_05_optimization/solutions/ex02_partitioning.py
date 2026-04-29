import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

spark = SparkSession.builder.appName("ex02-solution").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

tracks = spark.read.option("header", True).option("inferSchema", True).csv("../../data/tracks.csv")
streams = spark.read.option("header", True).option("inferSchema", True).csv("../../data/daily_streams.csv")

# Q1
print("Partitions initiales:", streams.rdd.getNumPartitions())
streams_8 = streams.repartition(8)
print("Après repartition(8):", streams_8.rdd.getNumPartitions())
streams_8.groupBy("track_id").agg(sum("stream_count")).explain()

# Q2 — repartition par colonne : le groupBy suivant n'a plus besoin de shuffle
streams_by_track = streams.repartition(8, "track_id")
streams_by_track.groupBy("track_id").agg(sum("stream_count")).explain()
# Il n'y a plus d'Exchange dans le plan pour ce groupBy

# Q3 — avec cache
enriched = streams.join(tracks, on="track_id")
enriched.cache()

t0 = time.time()
enriched.count()
print(f"count (with cache, first): {time.time() - t0:.2f}s")

t0 = time.time()
enriched.groupBy("artist_id").agg(sum("stream_count")).count()
print(f"agg (with cache, second): {time.time() - t0:.2f}s")

enriched.unpersist()

# Q4 — coalesce avant écriture
result = streams.groupBy("track_id").agg(sum("stream_count").alias("total_streams"))
result.coalesce(1).write.mode("overwrite").parquet("/tmp/gold_output/")
print("Écrit en 1 fichier Parquet dans /tmp/gold_output/")

spark.stop()
