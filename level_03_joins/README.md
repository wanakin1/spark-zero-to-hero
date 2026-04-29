# Level 3 — Joins

## 1. Les types de join

```python
# Inner join — seulement les lignes qui matchent des deux côtés
tracks.join(artists, on="artist_id", how="inner")

# Left join — toutes les lignes de gauche, null si pas de match à droite
tracks.join(artists, on="artist_id", how="left")

# Right join — toutes les lignes de droite, null si pas de match à gauche
tracks.join(artists, on="artist_id", how="right")

# Anti join — lignes de gauche qui N'ONT PAS de match à droite (utile pour trouver des orphelins)
tracks.join(artists, on="artist_id", how="left_anti")

# Cross join — produit cartésien (DANGEREUX sur gros datasets)
tracks.crossJoin(artists)
```

---

## 2. Les 3 stratégies de join Spark

### Broadcast Hash Join ⚡ (le plus rapide)
Quand une des deux tables est **petite** (< 10 MB par défaut), Spark l'envoie entière à chaque executor. Pas de shuffle.

```python
from pyspark.sql.functions import broadcast

tracks.join(broadcast(artists), on="artist_id")
```

Spark le fait **automatiquement** si la table est assez petite. Tu peux forcer avec `broadcast()`.

### Sort-Merge Join (le défaut pour les grandes tables)
Spark trie les deux tables par la clé de join, puis les merge. Implique un shuffle des deux côtés.

### Shuffle Hash Join
Spark hash les clés et envoie les lignes de même hash au même executor. Utile quand une table est moyenne.

> **Règle pratique** : si tu joins une grande table avec une petite table (référentiel, dimension), utilise toujours `broadcast()`.

---

## 3. Ambiguité de colonnes

Quand les deux DataFrames ont des colonnes du même nom, il faut les qualifier :

```python
# Mauvais — ambigü
tracks.join(artists, tracks.artist_id == artists.artist_id)

# Bien — utilise on= avec une string si la clé a le même nom
tracks.join(artists, on="artist_id")

# Ou qualifie explicitement
tracks.join(artists, tracks["artist_id"] == artists["artist_id"])
```

---

## 4. Attention aux duplicats de clé

Si la table de droite a des doublons sur la clé de join, tu vas **multiplier les lignes** côté gauche.
Toujours vérifier : `artists.groupBy("artist_id").count().filter(col("count") > 1).show()`

---

## Exercices

- [ex01_join_types.py](ex01_join_types.py) — inner, left, anti joins
- [ex02_broadcast.py](ex02_broadcast.py) — optimisation avec broadcast
