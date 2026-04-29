from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, lead, round, sum, to_date
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("ex02-solution").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")

streams = spark.read.option("header", True).option("inferSchema", True).csv("../../data/daily_streams.csv")
streams = streams.withColumn("date", to_date(col("date")))

window_track = Window.partitionBy("track_id").orderBy("date")

# Q1
with_prev = streams.withColumn("prev_streams", lag("stream_count", 1).over(window_track))
with_prev.show(20)

# Q2
with_pct = with_prev.withColumn(
    "pct_change",
    round((col("stream_count") - col("prev_streams")) / col("prev_streams") * 100, 1)
)
with_pct.show(20)

# Q3
with_pct.filter(col("pct_change") > 50).orderBy(col("pct_change").desc()).show()

# Q4
with_next = streams.withColumn("next_streams", lead("stream_count", 1).over(window_track))
with_next.filter(col("next_streams") < col("stream_count")).select("date", "track_id", "stream_count", "next_streams").show()

# Q5
daily_total = streams.groupBy("date").agg(sum("stream_count").alias("total_streams")).orderBy("date")
window_global = Window.orderBy("date")
daily_total.withColumn("prev_total", lag("total_streams", 1).over(window_global)).withColumn("variation", col("total_streams") - col("prev_total")).show()

spark.stop()
