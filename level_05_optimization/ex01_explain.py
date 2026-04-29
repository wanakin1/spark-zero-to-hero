"""
Exercise 01 — Reading Explain Plans
=====================================

Objectif : comprendre ce que Spark fait réellement sous le capot.

Dataset : ../data/artists.csv, ../data/tracks.csv, ../data/daily_streams.csv
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, broadcast, col, sum

spark = SparkSession.builder.appName("ex01").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")

artists = spark.read.option("header", True).option("inferSchema", True).csv("../data/artists.csv")
tracks = spark.read.option("header", True).option("inferSchema", True).csv("../data/tracks.csv")
streams = spark.read.option("header", True).option("inferSchema", True).csv("../data/daily_streams.csv")

# --- Q1 ---
# Exécute explain() sur ce pipeline et réponds :
#   - Combien y a-t-il d'étapes "Exchange" (shuffles) ?
#   - Quel type de join est utilisé ?
query1 = streams.join(tracks, on="track_id").groupBy("artist_id").agg(sum("stream_count").alias("total"))
query1.explain()
# Réponds ici en commentaire :
# Nombre de shuffles : ?
# Type de join : ?


# --- Q2 ---
# Même query mais avec broadcast(tracks).
# Compare le plan avec Q1. Qu'est-ce qui change ?
query2 = streams.join(broadcast(tracks), on="track_id").groupBy("artist_id").agg(sum("stream_count").alias("total"))
query2.explain()
# Qu'est-ce qui a changé dans le plan ?


# --- Q3 ---
# Ce code exécute combien d'actions ? Combien de fois les données sont-elles lues ?
df = streams.join(broadcast(tracks), on="track_id").filter(col("popularity") > 50)
count = df.count()
df.show(5)
# Réponds en commentaire, puis modifie le code pour ne lire les données qu'une seule fois.
# TODO


# --- Q4 ---
# Observe la différence entre ces deux requêtes dans leur plan :
#   a) filter avant groupBy
#   b) filter après groupBy
# Laquelle est plus efficace et pourquoi ?
a = streams.filter(col("stream_count") > 1000).groupBy("track_id").agg(sum("stream_count"))
b = streams.groupBy("track_id").agg(sum("stream_count").alias("total")).filter(col("total") > 1000)
print("=== Plan A (filter avant groupBy) ===")
a.explain()
print("=== Plan B (filter après groupBy) ===")
b.explain()
# Laquelle préfères-tu et pourquoi ? (commentaire)


spark.stop()
