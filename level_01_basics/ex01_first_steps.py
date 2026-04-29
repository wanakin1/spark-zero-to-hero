"""
Exercise 01 — First Steps
=========================

Objectif : charger les données et les explorer.

Dataset : ../data/artists.csv
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ex01").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

artists = spark.read.option("header", True).option("inferSchema", True).csv("../data/artists.csv")

# --- Q1 ---
# Affiche le schéma du DataFrame artists.
# Combien de colonnes y a-t-il ? Quels sont leurs types ?
# TODO


# --- Q2 ---
# Combien y a-t-il d'artistes au total ?
# TODO


# --- Q3 ---
# Affiche les 5 premiers artistes (toutes les colonnes).
# TODO


# --- Q4 ---
# Affiche uniquement les colonnes "name" et "genre" pour les 10 premiers artistes.
# TODO


# --- Q5 ---
# Combien y a-t-il de genres différents ?
# (indice : distinct + count)
# TODO


spark.stop()
