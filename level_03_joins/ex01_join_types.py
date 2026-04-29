"""
Exercise 01 — Join Types
=========================

Dataset : ../data/artists.csv, ../data/tracks.csv, ../data/daily_streams.csv
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum

spark = SparkSession.builder.appName("ex01").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")

artists = spark.read.option("header", True).option("inferSchema", True).csv("../data/artists.csv")
tracks = spark.read.option("header", True).option("inferSchema", True).csv("../data/tracks.csv")
streams = spark.read.option("header", True).option("inferSchema", True).csv("../data/daily_streams.csv")

# --- Q1 ---
# Joins tracks avec artists pour ajouter le nom de l'artiste et son genre à chaque track.
# Affiche : title, name (artiste), genre, popularity.
# TODO


# --- Q2 ---
# En partant du résultat de Q1, calcule la popularité moyenne par genre.
# (un seul enchaînement de transformations)
# TODO


# --- Q3 ---
# Joins streams avec tracks pour avoir le titre de chaque track streamée.
# Calcule ensuite le total de streams par titre.
# Affiche les 10 titres les plus streamés.
# TODO


# --- Q4 ---
# Trouve les tracks qui n'ont JAMAIS été streamées.
# (indice : left_anti join entre tracks et streams)
# Affiche track_id, title.
# TODO


# --- Q5 ---
# Triple join : streams → tracks → artists
# Pour chaque artiste, calcule son total de streams.
# Affiche name, genre, total_streams — trié par total_streams décroissant.
# TODO


spark.stop()
