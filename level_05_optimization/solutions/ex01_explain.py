from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, sum

spark = SparkSession.builder.appName("ex01-solution").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")

artists = spark.read.option("header", True).option("inferSchema", True).csv("../../data/artists.csv")
tracks = spark.read.option("header", True).option("inferSchema", True).csv("../../data/tracks.csv")
streams = spark.read.option("header", True).option("inferSchema", True).csv("../../data/daily_streams.csv")

# Q1 — 2 shuffles (join + groupBy), SortMergeJoin
streams.join(tracks, on="track_id").groupBy("artist_id").agg(sum("stream_count")).explain()

# Q2 — 1 shuffle (groupBy seulement), BroadcastHashJoin — le join ne shufflera plus
streams.join(broadcast(tracks), on="track_id").groupBy("artist_id").agg(sum("stream_count")).explain()

# Q3 — 2 actions = 2 lectures. Fix avec cache :
df = streams.join(broadcast(tracks), on="track_id").filter(col("popularity") > 50)
df.cache()
df.count()   # peuple le cache
df.show(5)   # lit depuis le cache
df.unpersist()

# Q4 — A est plus efficace : le filter avant groupBy réduit les données AVANT le shuffle.
# B fait le shuffle sur toutes les données, puis filtre.
# Catalyst peut optimiser A automatiquement (predicate pushdown).
a = streams.filter(col("stream_count") > 1000).groupBy("track_id").agg(sum("stream_count"))
b = streams.groupBy("track_id").agg(sum("stream_count").alias("total")).filter(col("total") > 1000)
a.explain()
b.explain()

spark.stop()
