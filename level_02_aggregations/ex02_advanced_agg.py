"""
Exercise 02 — Advanced Aggregations
=====================================

Objectif : collect_list, pivot, combinaisons multi-niveaux.

Dataset : ../data/artists.csv, ../data/tracks.csv, ../data/daily_streams.csv
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, collect_list, collect_set, count,
    countDistinct, round, sum, to_date
)

spark = SparkSession.builder.appName("ex02").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")

artists = spark.read.option("header", True).option("inferSchema", True).csv("../data/artists.csv")
tracks = spark.read.option("header", True).option("inferSchema", True).csv("../data/tracks.csv")
streams = spark.read.option("header", True).option("inferSchema", True).csv("../data/daily_streams.csv")

# --- Q1 ---
# Pour chaque pays, collecte la liste des genres d'artistes présents.
# (indice : collect_set pour avoir des valeurs uniques)
# Affiche country, genres.
# TODO


# --- Q2 ---
# Compte le nombre de tracks distinctes streamées par mois.
# (indice : to_date + month + countDistinct)
# TODO


# --- Q3 ---
# Trouve les 5 jours avec le plus de streams au total (date + total).
# TODO


# --- Q4 ---
# Pour chaque release_year, calcule :
#   - nombre de tracks
#   - popularité min, max, moyenne
#   - nombre de tracks "explicit"
# (indice : sum(col("explicit").cast("int")) pour compter les True)
# TODO


# --- Q5 ---
# Identifie les tracks "consistantes" : streamées plus de 50 jours distincts.
# Affiche track_id, nb_days — trié par nb_days décroissant.
# TODO


spark.stop()
