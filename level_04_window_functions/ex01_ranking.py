"""
Exercise 01 — Ranking
======================

Objectif : rank, dense_rank, row_number, top N par groupe.

Dataset : ../data/artists.csv, ../data/tracks.csv
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dense_rank, rank, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("ex01").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")

artists = spark.read.option("header", True).option("inferSchema", True).csv("../data/artists.csv")
tracks = spark.read.option("header", True).option("inferSchema", True).csv("../data/tracks.csv")

# --- Q1 ---
# Joins tracks avec artists pour avoir le genre sur chaque track.
# Puis ajoute une colonne "rank" qui classe les tracks par popularité décroissante
# DANS chaque genre.
# Affiche title, genre, popularity, rank.
# TODO


# --- Q2 ---
# Garde uniquement le Top 3 des tracks par genre (rank <= 3).
# Affiche genre, rank, title, popularity.
# TODO


# --- Q3 ---
# Quelle est la différence entre rank() et dense_rank() ?
# Crée un exemple où les deux donnent un résultat différent :
# ajoute les deux colonnes sur le même DataFrame et trouve des lignes où elles diffèrent.
# TODO


# --- Q4 ---
# Pour chaque genre, identifie la track #1 (la plus populaire).
# Utilise row_number() pour garantir un seul résultat par genre même en cas d'ex-aequo.
# Affiche genre, title, popularity.
# TODO


# --- Q5 ---
# Classe les artistes par followers dans chaque pays.
# Affiche country, name, followers, rank — uniquement les artistes classés 1er dans leur pays.
# TODO


spark.stop()
