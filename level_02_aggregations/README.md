# Level 2 — Agrégations

## 1. Le Shuffle : l'opération la plus coûteuse

Quand tu fais un `groupBy`, Spark doit regrouper toutes les lignes ayant la même clé sur le **même executor**. Ça implique de déplacer des données sur le réseau entre executors — c'est ce qu'on appelle un **shuffle**.

```
Executor 1: [Pop, Rock, Jazz]     →  shuffle  →  Executor 1: [tous les Pop]
Executor 2: [Pop, Electronic, R&B] →  shuffle  →  Executor 2: [tous les Rock]
Executor 3: [Rock, Classical, Pop] →  shuffle  →  Executor 3: [tous les Jazz]
```

> Minimise les shuffles dans ton code. Chaque `groupBy`, `join`, `distinct`, `orderBy` en déclenche un.

---

## 2. groupBy + agg

```python
from pyspark.sql.functions import count, sum, avg, min, max, countDistinct

df.groupBy("genre").agg(
    count("*").alias("nb_tracks"),
    avg("popularity").alias("avg_popularity"),
    max("followers").alias("max_followers"),
)
```

**Plusieurs agrégations en une seule passe** — Spark est optimisé pour ça. Ne fais pas plusieurs `groupBy` séparés si tu peux tout regrouper.

---

## 3. Fonctions d'agrégation essentielles

| Fonction | Description |
|----------|-------------|
| `count("*")` | Nombre de lignes (inclut les nulls) |
| `count("col")` | Nombre de valeurs non-nulles |
| `countDistinct("col")` | Nombre de valeurs distinctes |
| `sum("col")` | Somme |
| `avg("col")` | Moyenne |
| `min("col")` / `max("col")` | Min / Max |
| `collect_list("col")` | Agrège les valeurs dans une liste |
| `collect_set("col")` | Agrège les valeurs uniques dans un set |

---

## 4. having — filtrer après agrégation

En SQL tu utilises `HAVING`. En Spark, c'est un `.filter()` **après** le `.agg()` :

```python
df.groupBy("genre").agg(count("*").alias("nb")).filter(col("nb") > 5)
```

---

## 5. spark.sql.shuffle.partitions

Par défaut Spark crée **200 partitions** après un shuffle. Sur un petit dataset local c'est beaucoup trop — ça crée 200 tâches minuscules.

```python
spark.conf.set("spark.sql.shuffle.partitions", "4")  # adapte à ton dataset
```

C'est un des réglages les plus importants pour la perf sur des petits datasets.

---

## Exercices

- [ex01_groupby.py](ex01_groupby.py) — groupBy, agg, fonctions d'agrégation
- [ex02_advanced_agg.py](ex02_advanced_agg.py) — collect_list, having, combinaisons
