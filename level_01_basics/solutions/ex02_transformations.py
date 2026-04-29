from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, when

spark = SparkSession.builder.appName("ex02-solution").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

artists = spark.read.option("header", True).option("inferSchema", True).csv("../../data/artists.csv")
tracks = spark.read.option("header", True).option("inferSchema", True).csv("../../data/tracks.csv")

# Q1
artists.filter(col("followers") > 1_000_000).select("name", "followers").orderBy(col("followers").desc()).show()

# Q2
artists.withColumn("is_famous", col("followers") > 1_000_000).select("name", "genre", "followers", "is_famous").show()

# Q3
tracks.withColumn("duration_min", round(col("duration_sec") / 60, 2)).select("title", "duration_sec", "duration_min").show()

# Q4
tracks.filter((col("release_year") > 2021) & (col("popularity") > 70)).select("title", "release_year", "popularity").orderBy(col("popularity").desc()).show()

# Q5
tracks.withColumn(
    "tier",
    when(col("popularity") >= 80, "star")
    .when(col("popularity") >= 50, "mid")
    .otherwise("unknown")
).select("title", "popularity", "tier").show(10)

spark.stop()
