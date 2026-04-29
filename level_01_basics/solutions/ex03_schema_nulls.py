from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType

spark = SparkSession.builder.appName("ex03-solution").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Q1
tracks = spark.read.option("header", True).option("inferSchema", True).csv("../../data/tracks.csv")
tracks.printSchema()

# Q2
tracks_str = spark.read.option("header", True).csv("../../data/tracks.csv")
tracks_casted = tracks_str.withColumn("popularity", col("popularity").cast(IntegerType())).withColumn("duration_sec", col("duration_sec").cast(IntegerType()))
tracks_casted.printSchema()

# Q3
schema = StructType([
    StructField("track_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("artist_id", StringType(), True),
    StructField("duration_sec", IntegerType(), True),
    StructField("popularity", IntegerType(), True),
    StructField("release_year", IntegerType(), True),
    StructField("explicit", BooleanType(), True),
])
tracks_typed = spark.read.schema(schema).option("header", True).csv("../../data/tracks.csv")
tracks_typed.printSchema()

# Q4
tracks = spark.read.option("header", True).option("inferSchema", True).csv("../../data/tracks.csv")
nulls_df = spark.createDataFrame([
    Row(track_id="T999", title=None, artist_id="A001", duration_sec=None, popularity=50, release_year=2022, explicit=False),
    Row(track_id="T998", title="Ghost Track", artist_id=None, duration_sec=200, popularity=None, release_year=2023, explicit=True),
])
tracks_with_nulls = tracks.union(nulls_df)

null_counts = tracks_with_nulls.select([
    spark_sum(col(c).isNull().cast(IntegerType())).alias(c)
    for c in tracks_with_nulls.columns
])
null_counts.show()

# Q5
print("Before:", tracks_with_nulls.count())
cleaned = tracks_with_nulls.dropna(subset=["title", "popularity"])
print("After:", cleaned.count())

spark.stop()
