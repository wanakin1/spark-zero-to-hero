# Level 4 — Window Functions

C'est le niveau où Spark devient vraiment puissant. Les window functions permettent de faire des calculs **sur un groupe de lignes en rapport avec la ligne courante** — sans réduire le nombre de lignes (contrairement à `groupBy`).

## 1. Anatomie d'une window function

```python
from pyspark.sql.functions import rank, sum, lag
from pyspark.sql.window import Window

# Définir la fenêtre
window = Window.partitionBy("genre").orderBy(col("popularity").desc())

# Appliquer une fonction sur cette fenêtre
df.withColumn("rank", rank().over(window))
```

---

## 2. Les 3 composantes d'une WindowSpec

| Composante | Rôle | Exemple |
|------------|------|---------|
| `partitionBy` | Découpe en groupes indépendants (comme groupBy mais sans réduire) | `.partitionBy("genre")` |
| `orderBy` | Ordre dans chaque partition | `.orderBy(col("popularity").desc())` |
| `rowsBetween` / `rangeBetween` | Définit le cadre (frame) | `.rowsBetween(-2, 0)` = 2 lignes avant + courante |

---

## 3. Les 3 familles de window functions

### Ranking
```python
rank()          # 1, 2, 2, 4 (saute en cas d'ex-aequo)
dense_rank()    # 1, 2, 2, 3 (pas de saut)
row_number()    # 1, 2, 3, 4 (toujours unique)
percent_rank()  # position relative entre 0 et 1
```

### Analytiques
```python
lag("col", n)   # valeur n lignes AVANT (utile pour comparer avec la période précédente)
lead("col", n)  # valeur n lignes APRÈS
first("col")    # première valeur de la fenêtre
last("col")     # dernière valeur de la fenêtre
```

### Agrégats sur fenêtre
```python
sum("col")    # somme cumulée si orderBy est défini
avg("col")    # moyenne glissante avec rowsBetween
count("col")
```

---

## 4. Frame specification (rowsBetween)

```python
# Somme cumulative (début de partition jusqu'à la ligne courante)
Window.partitionBy("artist_id").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Moyenne glissante sur 3 jours (veille, aujourd'hui, demain)
Window.partitionBy("track_id").orderBy("date").rowsBetween(-1, 1)

# Toute la partition (équivalent à un groupBy mais sans réduire)
Window.partitionBy("genre").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
```

---

## 5. Cas d'usage typiques

- **Top N par groupe** : rank les tracks par popularité dans chaque genre, garde le top 3
- **Variation jour/jour** : lag pour comparer les streams d'aujourd'hui avec hier
- **Part du total** : streams d'un artiste / total streams de son genre
- **Moyenne glissante** : lisse les données de streams sur 7 jours

---

## Exercices

- [ex01_ranking.py](ex01_ranking.py) — rank, dense_rank, top N par groupe
- [ex02_lag_lead.py](ex02_lag_lead.py) — comparaison jour/jour, variation
- [ex03_running_totals.py](ex03_running_totals.py) — sommes cumulées, moyennes glissantes, parts du total
