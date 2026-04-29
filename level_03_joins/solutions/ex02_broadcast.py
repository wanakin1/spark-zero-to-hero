from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import broadcast, col, sum

spark = SparkSession.builder.appName("ex02-solution").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")

artists = spark.read.option("header", True).option("inferSchema", True).csv("../../data/artists.csv")
tracks = spark.read.option("header", True).option("inferSchema", True).csv("../../data/tracks.csv")
streams = spark.read.option("header", True).option("inferSchema", True).csv("../../data/daily_streams.csv")

# Q1 — avec broadcast
result_broadcast = streams.join(broadcast(tracks), on="track_id")
result_broadcast.explain()  # cherche "BroadcastHashJoin"

# Q2 — sans broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
result_no_broadcast = streams.join(tracks, on="track_id")
result_no_broadcast.explain()  # cherche "SortMergeJoin"
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # reset

# Q3
tiers_data = [
    Row(min_pop=0, max_pop=33, tier="low"),
    Row(min_pop=34, max_pop=66, tier="mid"),
    Row(min_pop=67, max_pop=100, tier="high"),
]
tiers_df = spark.createDataFrame(tiers_data)
classified = tracks.join(
    broadcast(tiers_df),
    on=(col("popularity") >= col("min_pop")) & (col("popularity") <= col("max_pop")),
    how="left",
)
classified.select("title", "popularity", "tier").show()

# Q4
(
    streams
    .join(broadcast(tracks), on="track_id")
    .join(broadcast(artists), on="artist_id")
    .groupBy("genre")
    .agg(sum("stream_count").alias("total_streams"))
    .orderBy(col("total_streams").desc())
    .show()
)

spark.stop()
