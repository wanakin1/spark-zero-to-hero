from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, round, sum

spark = SparkSession.builder.appName("ex01-solution").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")

artists = spark.read.option("header", True).option("inferSchema", True).csv("../../data/artists.csv")
tracks = spark.read.option("header", True).option("inferSchema", True).csv("../../data/tracks.csv")
streams = spark.read.option("header", True).option("inferSchema", True).csv("../../data/daily_streams.csv")

# Q1
enriched = tracks.join(artists, on="artist_id", how="inner")
enriched.select("title", "name", "genre", "popularity").show()

# Q2
enriched.groupBy("genre").agg(round(avg("popularity"), 2).alias("avg_popularity")).orderBy(col("avg_popularity").desc()).show()

# Q3
streams.join(tracks, on="track_id", how="inner").groupBy("title").agg(sum("stream_count").alias("total_streams")).orderBy(col("total_streams").desc()).show(10)

# Q4
tracks.join(streams, on="track_id", how="left_anti").select("track_id", "title").show()

# Q5
(
    streams
    .join(tracks, on="track_id")
    .join(artists, on="artist_id")
    .groupBy("name", "genre")
    .agg(sum("stream_count").alias("total_streams"))
    .orderBy(col("total_streams").desc())
    .show()
)

spark.stop()
