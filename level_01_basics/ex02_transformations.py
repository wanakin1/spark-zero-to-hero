"""
Exercise 02 — Transformations
==============================

Objectif : filter, select, withColumn, orderBy.

Dataset : ../data/artists.csv, ../data/tracks.csv
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, when

spark = SparkSession.builder.appName("ex02").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

artists = spark.read.option("header", True).option("inferSchema", True).csv("../data/artists.csv")
tracks = spark.read.option("header", True).option("inferSchema", True).csv("../data/tracks.csv")

# --- Q1 ---
# Filtre les artistes qui ont plus de 1 million de followers.
# Affiche leur nom et leur nombre de followers, trié par followers décroissant.
# TODO


# --- Q2 ---
# Ajoute une colonne "is_famous" qui vaut True si followers > 1_000_000, False sinon.
# Affiche name, genre, followers, is_famous.
# TODO


# --- Q3 ---
# Ajoute une colonne "duration_min" qui convertit duration_sec en minutes,
# arrondie à 2 décimales.
# (indice : from pyspark.sql.functions import round)
# TODO


# --- Q4 ---
# Filtre les tracks sorties après 2021 ET avec une popularité > 70.
# Affiche title, release_year, popularity — triés par popularity décroissant.
# TODO


# --- Q5 ---
# Ajoute une colonne "tier" :
#   - "star"    si popularity >= 80
#   - "mid"     si popularity >= 50
#   - "unknown" sinon
# (indice : when(...).when(...).otherwise(...))
# Affiche title, popularity, tier pour les 10 premiers.
# TODO


spark.stop()
