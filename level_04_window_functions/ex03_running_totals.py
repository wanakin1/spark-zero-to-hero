"""
Exercise 03 — Running Totals & Moving Averages
================================================

Objectif : sommes cumulées, moyennes glissantes, part du total.

Dataset : ../data/artists.csv, ../data/tracks.csv, ../data/daily_streams.csv
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, round, sum, to_date
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("ex03").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")

artists = spark.read.option("header", True).option("inferSchema", True).csv("../data/artists.csv")
tracks = spark.read.option("header", True).option("inferSchema", True).csv("../data/tracks.csv")
streams = streams = spark.read.option("header", True).option("inferSchema", True).csv("../data/daily_streams.csv")
streams = streams.withColumn("date", to_date(col("date")))

# --- Q1 ---
# Pour chaque track, calcule les streams cumulés au fil du temps.
# La colonne "cumulative_streams" doit augmenter jour après jour pour chaque track.
# (indice : sum("stream_count").over(Window.partitionBy("track_id").orderBy("date").rowsBetween(unboundedPreceding, currentRow)))
# Affiche date, track_id, stream_count, cumulative_streams pour la track "T001".
# TODO


# --- Q2 ---
# Calcule une moyenne glissante sur 7 jours des streams totaux quotidiens.
# Étape 1 : agrège les streams par date (sum de toutes les tracks)
# Étape 2 : applique une moyenne glissante 7 jours (3 jours avant + jour courant + 3 jours après)
# Affiche date, total_streams, moving_avg_7d (arrondie à 0 décimale).
# TODO


# --- Q3 ---
# Pour chaque track, calcule sa part du total des streams de son genre.
# (indice : joins avec tracks+artists pour avoir le genre,
#  puis sum sur toute la partition genre = Window.partitionBy("genre").rowsBetween(unboundedPreceding, unboundedFollowing))
# Affiche track_id, genre, total_streams_track, total_streams_genre, pct_of_genre (arrondi à 2 décimales).
# TODO


# --- Q4 ---
# Pour chaque track, identifie son "meilleur jour" (jour avec le plus de streams).
# (indice : rank() décroissant par stream_count dans chaque track, garde rank = 1)
# Affiche track_id, date, stream_count.
# TODO


spark.stop()
