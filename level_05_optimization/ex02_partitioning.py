"""
Exercise 02 — Partitioning & Cache
=====================================

Objectif : comprendre repartition, coalesce, cache/persist.

Dataset : ../data/tracks.csv, ../data/daily_streams.csv
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

spark = SparkSession.builder.appName("ex02").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

tracks = spark.read.option("header", True).option("inferSchema", True).csv("../data/tracks.csv")
streams = spark.read.option("header", True).option("inferSchema", True).csv("../data/daily_streams.csv")

# --- Q1 ---
# Affiche le nombre de partitions de streams.
# Puis repartitionne à 8 partitions et vérifie.
# Quel est l'impact sur le plan (explain) ?
# TODO


# --- Q2 ---
# Repartitionne streams par "track_id" (repartition sur une colonne).
# Cela garantit que toutes les lignes d'un même track_id sont dans la même partition.
# Vérifie avec explain() qu'un groupBy("track_id") après n'a plus besoin de shuffle.
# (cherche "Exchange" dans le plan — il ne devrait plus y en avoir)
# TODO


# --- Q3 ---
# Ce pipeline est utilisé deux fois. Cache-le pour éviter de relire le CSV deux fois.
# Mesure la différence en ajoutant des print(time.time()) avant/après chaque action.
import time

enriched = streams.join(tracks, on="track_id")

# Sans cache — deux lectures
t0 = time.time()
enriched.count()
print(f"count (no cache): {time.time() - t0:.2f}s")

t0 = time.time()
enriched.groupBy("artist_id").agg(sum("stream_count")).count()
print(f"agg (no cache): {time.time() - t0:.2f}s")

# TODO : refais les mêmes opérations avec cache et compare les temps


# --- Q4 ---
# Différence entre repartition() et coalesce() :
# - repartition(n) : peut augmenter ou réduire, fait un shuffle complet
# - coalesce(n) : peut seulement réduire, pas de shuffle complet (plus rapide)
#
# Tu viens de calculer un résultat final de 30 lignes (petite table).
# Avant d'écrire en Parquet, réduis à 1 partition avec coalesce(1) pour éviter
# des dizaines de petits fichiers.
# Écris le résultat en Parquet dans /tmp/gold_output/.
# TODO
result = streams.groupBy("track_id").agg(sum("stream_count").alias("total_streams"))


spark.stop()
