# Level 5 — Optimisation

## 1. Le Catalyst Optimizer

Spark ne fait pas juste exécuter ton code. Il le **transforme** d'abord en un plan optimisé via le **Catalyst Optimizer** :

```
Ton code Python
    → Unresolved Logical Plan  (parsing)
    → Resolved Logical Plan    (vérifie que les colonnes existent)
    → Optimized Logical Plan   (pushdown, pruning, simplifications)
    → Physical Plans           (plusieurs stratégies possibles)
    → Best Physical Plan       (choisit la moins coûteuse)
    → Code Java bytecode        (Tungsten — génère du code optimisé)
```

---

## 2. Lire un explain()

```python
df.explain()          # plan physique
df.explain("extended") # tous les plans
df.explain("cost")    # avec estimation de coût
```

Ce qu'il faut repérer dans le plan physique :

| Ce que tu vois | Ce que ça veut dire |
|----------------|---------------------|
| `FileScan` | Lecture du fichier source |
| `Filter` juste après `FileScan` | Predicate pushdown ✅ (filtre au moment de la lecture) |
| `Exchange` | Shuffle réseau 🔴 (opération coûteuse) |
| `BroadcastHashJoin` | Join sans shuffle ✅ |
| `SortMergeJoin` | Join avec shuffle des deux côtés 🔴 |
| `HashAggregate` | Agrégation locale puis shuffle |
| `Sort` | Tri (déclenche un shuffle si global) |

---

## 3. Predicate Pushdown

Spark pousse les filtres le plus tôt possible dans le plan — idéalement au moment de la lecture du fichier. C'est automatique pour les formats Parquet, Delta, ORC.

```python
# Spark lira UNIQUEMENT les partitions year=2024 si le fichier est partitionné par year
df = spark.read.parquet("data/").filter(col("year") == 2024)
```

---

## 4. Partitionnement

Le nombre de partitions = niveau de parallélisme de Spark.

```python
df.rdd.getNumPartitions()  # voir le nombre actuel
df.repartition(8)          # redistribue les données (déclenche un shuffle)
df.coalesce(2)             # réduit le nombre de partitions (pas de shuffle complet)
```

**Règle de base** : vise ~128 MB par partition. Trop peu → goulot d'étranglement. Trop → overhead.

```python
# Après un shuffle (groupBy, join), Spark crée shuffle.partitions partitions (défaut: 200)
spark.conf.set("spark.sql.shuffle.partitions", "8")  # adapte à ton dataset
```

---

## 5. Cache & Persist

Si tu utilises le même DataFrame plusieurs fois, **cache-le** pour éviter de le recalculer.

```python
df.cache()          # stocke en mémoire (RAM)
df.persist()        # idem, paramétrable (MEMORY_AND_DISK, DISK_ONLY...)
df.unpersist()      # libère la mémoire — important !

# Vérifie que le cache est utilisé
df.explain()  # cherche "InMemoryTableScan"
```

> **Attention** : le cache est lazy ! `df.cache()` n'exécute rien. C'est la première action après qui peuple le cache.

---

## 6. Les pièges classiques

```python
# ❌ Déclenche 2 actions = 2 lectures complètes
count = df.count()
df.show()

# ✅ Cache si tu réutilises
df.cache()
count = df.count()  # peuple le cache
df.show()           # lit depuis le cache
df.unpersist()

# ❌ collect() ramène TOUTES les données sur le driver — dangereux sur gros volume
all_rows = df.collect()

# ✅ Utilise take() ou limit() pour inspecter
df.show(20)
df.limit(20).toPandas()
```

---

## Exercices

- [ex01_explain.py](ex01_explain.py) — lire et interpréter les plans d'exécution
- [ex02_partitioning.py](ex02_partitioning.py) — repartition, coalesce, cache
