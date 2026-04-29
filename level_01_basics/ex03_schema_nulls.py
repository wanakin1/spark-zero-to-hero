"""
Exercise 03 — Schema, Types & Nulls
=====================================

Objectif : contrôler le schéma, gérer les nulls, caster les types.

Dataset : ../data/tracks.csv
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

spark = SparkSession.builder.appName("ex03").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# --- Q1 ---
# Charge tracks.csv avec inferSchema=True.
# Affiche le schéma. Le type de "explicit" est-il correct ?
# TODO


# --- Q2 ---
# Charge tracks.csv SANS inferSchema (tout sera StringType par défaut).
# Caste manuellement "popularity" en IntegerType et "duration_sec" en IntegerType.
# (indice : withColumn + col("...").cast(IntegerType()))
# Affiche le schéma après cast pour vérifier.
# TODO


# --- Q3 ---
# Charge tracks.csv avec le schéma défini manuellement via StructType.
# Définis au moins : track_id (String), title (String), popularity (Integer), duration_sec (Integer).
# (indice : spark.read.schema(mon_schema).csv(...))
# TODO
schema = StructType([
    # à compléter
])


# --- Q4 ---
# Avec inferSchema=True, ajoute des nulls manuellement pour tester :
tracks = spark.read.option("header", True).option("inferSchema", True).csv("../data/tracks.csv")
from pyspark.sql import Row
nulls_df = spark.createDataFrame([
    Row(track_id="T999", title=None, artist_id="A001", duration_sec=None, popularity=50, release_year=2022, explicit=False),
    Row(track_id="T998", title="Ghost Track", artist_id=None, duration_sec=200, popularity=None, release_year=2023, explicit=True),
])
tracks_with_nulls = tracks.union(nulls_df)

# Compte le nombre de nulls par colonne.
# (indice : utilise une boucle sur les colonnes et col(...).isNull())
# TODO


# --- Q5 ---
# Supprime les lignes qui ont un null dans "title" ou "popularity".
# (indice : dropna avec subset)
# Vérifie que le count a diminué.
# TODO


spark.stop()
