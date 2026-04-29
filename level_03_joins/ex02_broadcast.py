"""
Exercise 02 — Broadcast Join
==============================

Objectif : comprendre quand et comment utiliser broadcast pour éviter un shuffle.

Dataset : ../data/artists.csv, ../data/tracks.csv, ../data/daily_streams.csv
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, sum

spark = SparkSession.builder.appName("ex02").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")

artists = spark.read.option("header", True).option("inferSchema", True).csv("../data/artists.csv")
tracks = spark.read.option("header", True).option("inferSchema", True).csv("../data/tracks.csv")
streams = spark.read.option("header", True).option("inferSchema", True).csv("../data/daily_streams.csv")

# --- Q1 ---
# Joins streams (grande table) avec tracks (petite table) en utilisant broadcast.
# Affiche le plan d'exécution avec .explain() et repère "BroadcastHashJoin" dedans.
# TODO


# --- Q2 ---
# Fais le même join SANS broadcast et compare le plan d'exécution.
# (indice : désactive le broadcast auto avec spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1"))
# Repère "SortMergeJoin" dans le plan.
# TODO


# --- Q3 ---
# Crée un petit DataFrame de "labels" pour classifier les popularités :
#   popularity_tier: 0-33 → "low", 34-66 → "mid", 67-100 → "high"
# Joins-le avec tracks en broadcast.
# (indice : crée le DataFrame avec spark.createDataFrame)
# TODO
tiers = [
    (0, 33, "low"),
    (34, 66, "mid"),
    (67, 100, "high"),
]


# --- Q4 ---
# Joins streams → tracks → artists avec broadcast sur tracks et artists.
# Calcule le total de streams par genre.
# TODO


spark.stop()
