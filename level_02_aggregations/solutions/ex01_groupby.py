from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, max, month, round, sum, to_date

spark = SparkSession.builder.appName("ex01-solution").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")

artists = spark.read.option("header", True).option("inferSchema", True).csv("../../data/artists.csv")
tracks = spark.read.option("header", True).option("inferSchema", True).csv("../../data/tracks.csv")
streams = spark.read.option("header", True).option("inferSchema", True).csv("../../data/daily_streams.csv")

# Q1
tracks.groupBy("release_year").agg(count("*").alias("nb_tracks")).orderBy("release_year").show()

# Q2
artists.groupBy("genre").agg(
    count("*").alias("nb_artists"),
    round(avg("followers"), 0).alias("avg_followers"),
    max("followers").alias("max_followers"),
).orderBy(col("avg_followers").desc()).show()

# Q3
streams.groupBy("track_id").agg(sum("stream_count").alias("total_streams")).orderBy(col("total_streams").desc()).show(10)

# Q4
streams.withColumn("month", month(to_date(col("date")))).groupBy("month").agg(sum("stream_count").alias("total_streams")).orderBy("month").show()

# Q5
tracks.groupBy("genre").agg(round(avg("popularity"), 2).alias("avg_popularity")).filter(col("avg_popularity") > 55).show()

spark.stop()
