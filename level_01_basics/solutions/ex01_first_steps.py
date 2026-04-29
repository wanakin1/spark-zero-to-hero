from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ex01-solution").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

artists = spark.read.option("header", True).option("inferSchema", True).csv("../../data/artists.csv")

# Q1
artists.printSchema()

# Q2
print("Total artists:", artists.count())

# Q3
artists.show(5)

# Q4
artists.select("name", "genre").show(10)

# Q5
print("Distinct genres:", artists.select("genre").distinct().count())

spark.stop()
