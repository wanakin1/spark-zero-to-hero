# Level 1 — Les bases de Spark

## 1. C'est quoi Spark ?

Spark est un moteur de calcul distribué. Il découpe les données en morceaux et les traite en parallèle sur plusieurs machines (ou plusieurs CPU sur ta machine locale).

```
Driver (ton script Python)
    ├── Executor 1 → traite partition 1
    ├── Executor 2 → traite partition 2
    └── Executor 3 → traite partition 3
```

En local (`master("local[*]")`), les executors sont des threads sur ta machine.

---

## 2. Le concept le plus important : Lazy Evaluation

**Spark n'exécute rien tant que tu ne lui demandes pas un résultat.**

```python
df = spark.read.csv("artists.csv")   # rien ne s'exécute
df2 = df.filter(df.followers > 1_000_000)  # rien ne s'exécute
df3 = df2.select("name", "genre")    # rien ne s'exécute

df3.show()  # ICI Spark exécute TOUT d'un coup
```

Spark construit un plan (DAG), l'optimise, puis l'exécute. C'est pour ça que c'est rapide.

---

## 3. Transformations vs Actions

| Type | Description | Exemples | Exécute ? |
|------|-------------|---------|-----------|
| **Transformation** | Crée un nouveau DataFrame | `filter`, `select`, `withColumn`, `groupBy`, `join` | ❌ Non |
| **Action** | Déclenche l'exécution | `show()`, `count()`, `collect()`, `write` | ✅ Oui |

> **Règle d'or** : chaque `action` déclenche une lecture complète des données depuis le début du plan. Si tu fais `df.count()` puis `df.show()`, Spark lit les données **deux fois**. Utilise `.cache()` si tu réutilises le même DataFrame.

---

## 4. Narrow vs Wide transformations

- **Narrow** : chaque partition du résultat ne dépend que d'une partition source. Pas de shuffle. Rapide.
  - `filter`, `select`, `withColumn`, `map`
- **Wide** : une partition du résultat dépend de plusieurs partitions sources. Shuffle réseau. Lent.
  - `groupBy`, `join`, `distinct`, `orderBy`

Le shuffle = déplacement de données entre executors sur le réseau = opération coûteuse.

---

## 5. Créer un DataFrame

```python
# Depuis un fichier
df = spark.read.option("header", True).option("inferSchema", True).csv("artists.csv")

# Depuis du code (utile pour les tests)
from pyspark.sql import Row
df = spark.createDataFrame([
    Row(name="Daft Punk", genre="Electronic"),
    Row(name="Stromae", genre="Pop"),
])

# Voir le schéma
df.printSchema()
df.show(5)          # 5 premières lignes
df.show(truncate=False)  # sans tronquer les colonnes longues
```

---

## 6. Cheat sheet — opérations de base

```python
# Sélection
df.select("name", "genre")
df.select(df.name, df.genre)

# Filtre
df.filter(df.followers > 1_000_000)
df.filter("followers > 1000000")  # SQL style, aussi valide
df.where(df.genre == "Pop")       # alias de filter

# Nouvelle colonne
from pyspark.sql.functions import col, lit, upper
df.withColumn("name_upper", upper(col("name")))
df.withColumn("is_famous", col("followers") > 1_000_000)

# Renommer / Supprimer
df.withColumnRenamed("followers", "nb_followers")
df.drop("followers")

# Trier
df.orderBy("followers")
df.orderBy(col("followers").desc())

# Dédupliquer
df.distinct()
df.dropDuplicates(["genre"])  # dédupliquer sur un sous-ensemble de colonnes
```

---

## Exercices

- [ex01_first_steps.py](ex01_first_steps.py) — Charger les données, explorer le schéma
- [ex02_transformations.py](ex02_transformations.py) — select, filter, withColumn
- [ex03_schema_nulls.py](ex03_schema_nulls.py) — Typage, nulls, cast
