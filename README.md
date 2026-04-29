# Spark Zero to Hero

Cours pratique PySpark — théorie essentielle + exercices progressifs sur un dataset musical.

## Dataset

Un service de streaming musical fictif avec 3 tables :
- `artists.csv` — artistes (genre, pays, followers)
- `tracks.csv` — morceaux (popularité, durée, année)
- `daily_streams.csv` — streams quotidiens par morceau (Jan-Mar 2024)

```bash
# Générer les données
python data/generate.py
```

## Structure

| Niveau | Thème | Concepts clés |
|--------|-------|---------------|
| [Level 1](level_01_basics/) | Les bases | Lazy evaluation, transformations vs actions, DataFrame API |
| [Level 2](level_02_aggregations/) | Agrégations | groupBy, shuffle, fonctions d'agrégation |
| [Level 3](level_03_joins/) | Joins | Join types, Broadcast join, stratégies |
| [Level 4](level_04_window_functions/) | Window Functions | rank, lag/lead, running totals |
| [Level 5](level_05_optimization/) | Optimisation | explain(), partitioning, cache |

## Setup

```bash
pip install pyspark
# ou avec uv :
uv sync
```

## Comment utiliser ce repo

1. Lis le `README.md` du niveau avant de faire les exercices
2. Ouvre l'exercice, essaie de le résoudre **sans regarder la solution**
3. Compare avec `solutions/` si tu bloques
4. Passe au niveau suivant

> Le livre "Spark Definitive Guide" de Bill Chambers est une excellente référence pour aller plus loin sur chaque concept — mais ce repo te donne l'essentiel pour être opérationnel.
