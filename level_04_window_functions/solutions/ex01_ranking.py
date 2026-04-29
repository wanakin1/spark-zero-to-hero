from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dense_rank, rank, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("ex01-solution").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")

artists = spark.read.option("header", True).option("inferSchema", True).csv("../../data/artists.csv")
tracks = spark.read.option("header", True).option("inferSchema", True).csv("../../data/tracks.csv")

# Q1
enriched = tracks.join(artists, on="artist_id")
window_genre = Window.partitionBy("genre").orderBy(col("popularity").desc())
ranked = enriched.withColumn("rank", rank().over(window_genre))
ranked.select("title", "genre", "popularity", "rank").show()

# Q2
ranked.filter(col("rank") <= 3).select("genre", "rank", "title", "popularity").orderBy("genre", "rank").show()

# Q3 — rank vs dense_rank
window = Window.partitionBy("genre").orderBy(col("popularity").desc())
enriched.withColumn("rank", rank().over(window)).withColumn("dense_rank", dense_rank().over(window)).filter(col("rank") != col("dense_rank")).select("title", "genre", "popularity", "rank", "dense_rank").show()

# Q4
window_rn = Window.partitionBy("genre").orderBy(col("popularity").desc())
enriched.withColumn("rn", row_number().over(window_rn)).filter(col("rn") == 1).select("genre", "title", "popularity").orderBy("genre").show()

# Q5
window_country = Window.partitionBy("country").orderBy(col("followers").desc())
artists.withColumn("rank", rank().over(window_country)).filter(col("rank") == 1).select("country", "name", "followers", "rank").orderBy("country").show()

spark.stop()
