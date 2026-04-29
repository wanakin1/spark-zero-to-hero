from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, rank, round, sum, to_date
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("ex03-solution").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")

artists = spark.read.option("header", True).option("inferSchema", True).csv("../../data/artists.csv")
tracks = spark.read.option("header", True).option("inferSchema", True).csv("../../data/tracks.csv")
streams = spark.read.option("header", True).option("inferSchema", True).csv("../../data/daily_streams.csv")
streams = streams.withColumn("date", to_date(col("date")))

# Q1
window_cumul = Window.partitionBy("track_id").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
streams.withColumn("cumulative_streams", sum("stream_count").over(window_cumul)).filter(col("track_id") == "T001").show()

# Q2
daily = streams.groupBy("date").agg(sum("stream_count").alias("total_streams")).orderBy("date")
window_7d = Window.orderBy("date").rowsBetween(-3, 3)
daily.withColumn("moving_avg_7d", round(avg("total_streams").over(window_7d), 0)).show()

# Q3
track_streams = streams.groupBy("track_id").agg(sum("stream_count").alias("total_streams_track"))
enriched = track_streams.join(tracks, on="track_id").join(artists, on="artist_id")
window_genre = Window.partitionBy("genre").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
enriched.withColumn("total_streams_genre", sum("total_streams_track").over(window_genre)).withColumn("pct_of_genre", round(col("total_streams_track") / col("total_streams_genre") * 100, 2)).select("track_id", "genre", "total_streams_track", "total_streams_genre", "pct_of_genre").show()

# Q4
window_best = Window.partitionBy("track_id").orderBy(col("stream_count").desc())
streams.withColumn("rank", rank().over(window_best)).filter(col("rank") == 1).select("track_id", "date", "stream_count").show()

spark.stop()
