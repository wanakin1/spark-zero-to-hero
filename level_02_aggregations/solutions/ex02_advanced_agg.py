from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, collect_set, count, countDistinct, min, max, month, round, sum, to_date

spark = SparkSession.builder.appName("ex02-solution").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")

artists = spark.read.option("header", True).option("inferSchema", True).csv("../../data/artists.csv")
tracks = spark.read.option("header", True).option("inferSchema", True).csv("../../data/tracks.csv")
streams = spark.read.option("header", True).option("inferSchema", True).csv("../../data/daily_streams.csv")

# Q1
artists.groupBy("country").agg(collect_set("genre").alias("genres")).show(truncate=False)

# Q2
streams.withColumn("month", month(to_date(col("date")))).groupBy("month").agg(countDistinct("track_id").alias("distinct_tracks")).orderBy("month").show()

# Q3
streams.groupBy("date").agg(sum("stream_count").alias("total")).orderBy(col("total").desc()).show(5)

# Q4
tracks.groupBy("release_year").agg(
    count("*").alias("nb_tracks"),
    round(min("popularity"), 0).alias("min_pop"),
    round(max("popularity"), 0).alias("max_pop"),
    round(avg("popularity"), 1).alias("avg_pop"),
    sum(col("explicit").cast("int")).alias("nb_explicit"),
).orderBy("release_year").show()

# Q5
streams.groupBy("track_id").agg(countDistinct("date").alias("nb_days")).filter(col("nb_days") > 50).orderBy(col("nb_days").desc()).show()

spark.stop()
