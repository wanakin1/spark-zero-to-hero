"""Run this script once to generate the CSV datasets used in all exercises."""
import csv
import random
from datetime import date, timedelta

random.seed(42)

GENRES = ["Pop", "Rock", "Hip-Hop", "Jazz", "Electronic", "R&B", "Classical", "Country"]
COUNTRIES = ["France", "USA", "UK", "Canada", "Germany", "Brazil", "Japan", "Australia"]

ARTIST_NAMES = [
    "The Midnight", "Daft Punk", "Stromae", "Christine and the Queens",
    "Phoenix", "Air", "M83", "Kavinsky", "Gesaffelstein", "Polo & Pan",
    "David Bowie", "Radiohead", "Massive Attack", "Portishead", "Björk",
    "Kendrick Lamar", "Frank Ocean", "Tyler the Creator", "J. Cole", "SZA",
    "Miles Davis", "John Coltrane", "Herbie Hancock", "Bill Evans", "Chet Baker",
    "Dua Lipa", "The Weeknd", "Harry Styles", "Billie Eilish", "Taylor Swift",
]

# --- Artists ---
artists = []
for i, name in enumerate(ARTIST_NAMES):
    artists.append({
        "artist_id": f"A{i+1:03d}",
        "name": name,
        "genre": GENRES[i % len(GENRES)],
        "country": COUNTRIES[i % len(COUNTRIES)],
        "followers": random.randint(10_000, 50_000_000),
    })

with open("artists.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["artist_id", "name", "genre", "country", "followers"])
    writer.writeheader()
    writer.writerows(artists)

# --- Tracks ---
PREFIXES = ["Love", "Night", "Dream", "Fire", "Dark", "Blue", "Gold", "Wild", "Lost", "New"]
SUFFIXES = ["Song", "Dance", "Beat", "Wave", "Light", "Road", "Sky", "Rain", "Storm", "Way"]

tracks = []
for i in range(100):
    tracks.append({
        "track_id": f"T{i+1:03d}",
        "title": f"{random.choice(PREFIXES)} {random.choice(SUFFIXES)}",
        "artist_id": random.choice(artists)["artist_id"],
        "duration_sec": random.randint(150, 350),
        "popularity": random.randint(0, 100),
        "release_year": random.randint(2018, 2024),
        "explicit": random.choice([True, False]),
    })

with open("tracks.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["track_id", "title", "artist_id", "duration_sec", "popularity", "release_year", "explicit"])
    writer.writeheader()
    writer.writerows(tracks)

# --- Daily streams (Jan–Mar 2024) ---
streams = []
current = date(2024, 1, 1)
while current <= date(2024, 3, 31):
    for track in random.sample(tracks, k=random.randint(20, 30)):
        streams.append({
            "date": current.isoformat(),
            "track_id": track["track_id"],
            "stream_count": random.randint(100, 50_000),
        })
    current += timedelta(days=1)

with open("daily_streams.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["date", "track_id", "stream_count"])
    writer.writeheader()
    writer.writerows(streams)

print(f"artists: {len(artists)} rows")
print(f"tracks: {len(tracks)} rows")
print(f"daily_streams: {len(streams)} rows")
