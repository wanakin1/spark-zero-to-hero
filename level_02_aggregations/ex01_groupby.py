"""
Exercise 01 — GroupBy & Aggregations
======================================

Dataset : ../data/artists.csv, ../data/tracks.csv, ../data/daily_streams.csv
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, max, min, round, sum

spark = SparkSession.builder.appName("ex01").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")

artists = spark.read.option("header", True).option("inferSchema", True).csv("../data/artists.csv")
tracks = spark.read.option("header", True).option("inferSchema", True).csv("../data/tracks.csv")
streams = spark.read.option("header", True).option("inferSchema", True).csv("../data/daily_streams.csv")

# --- Q1 ---
# Combien y a-t-il de tracks par release_year ?
# Affiche release_year, nb_tracks — trié par année croissante.
# TODO


# --- Q2 ---
# Pour chaque genre, calcule :
#   - le nombre d'artistes
#   - le nombre moyen de followers (arrondi à 0 décimale)
#   - le nombre max de followers
# Trie par avg_followers décroissant.
# TODO


# --- Q3 ---
# Calcule le total de streams par track_id.
# Affiche les 10 tracks les plus streamées (track_id, total_streams).
# TODO


# --- Q4 ---
# Calcule le total de streams par mois.
# (indice : la colonne "date" est une string "2024-01-15" — utilise month() ou substring())
# Affiche month, total_streams trié par mois.
# TODO


# --- Q5 ---
# Quels genres ont une popularité moyenne > 55 ?
# (indice : filter APRÈS le groupBy+agg)
# TODO


spark.stop()
