"""
Exercise 02 — Lag & Lead
=========================

Objectif : comparer une valeur avec la période précédente/suivante.

Dataset : ../data/tracks.csv, ../data/daily_streams.csv
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, lead, round, sum, to_date
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("ex02").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")

tracks = spark.read.option("header", True).option("inferSchema", True).csv("../data/tracks.csv")
streams = spark.read.option("header", True).option("inferSchema", True).csv("../data/daily_streams.csv")

streams = streams.withColumn("date", to_date(col("date")))

# --- Q1 ---
# Pour chaque track, calcule les streams du jour précédent avec lag().
# Affiche date, track_id, stream_count, prev_streams.
# (important : partitionBy track_id, orderBy date)
# TODO


# --- Q2 ---
# À partir de Q1, calcule la variation en % par rapport au jour précédent :
#   pct_change = (stream_count - prev_streams) / prev_streams * 100
# Arrondi à 1 décimale.
# Les lignes où prev_streams est null sont normales (premier jour de la track).
# TODO


# --- Q3 ---
# Trouve les jours où une track a eu une augmentation de streams > 50% vs la veille.
# Affiche date, track_id, stream_count, prev_streams, pct_change.
# Trie par pct_change décroissant.
# TODO


# --- Q4 ---
# Pour chaque track, affiche les streams d'aujourd'hui ET de demain (lead).
# Identifie les tracks dont les streams vont diminuer le lendemain.
# TODO


# --- Q5 ---
# Agrège d'abord les streams par date (total toutes tracks confondues).
# Puis applique lag(1) pour calculer la variation quotidienne du total.
# Affiche date, total_streams, prev_total, variation.
# TODO


spark.stop()
