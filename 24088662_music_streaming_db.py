
"""
'music_streaming.db' using pandas, python, Faker, sqlite3.

Tables:
 - Artists
 - Albums
 - Songs
 - Users (1000 rows)
 - Plays (composite PK: user_id, song_id, session_no)

Data types included:
 - Nominal: artist_name, genre, user_name, gender
 - Ordinal: user_tier, audio_quality
 - Interval: popularity_index (0-100), listen_score (0-10) -> arbitrary zero meaning
 - Ratio: duration_seconds, total_listens, quantity-like fields
"""

# -------------------- Libraries --------------------
import sqlite3
import random
import numpy as np
import pandas as pd
from faker import Faker

# -------------------- Setup --------------------
fake = Faker()
SEED = 123
random.seed(SEED)
np.random.seed(SEED)
Faker.seed(SEED)

DB_FILENAME = "music_streaming.db"

# -------------------- Parameters --------------------
NUM_ARTISTS = 120
NUM_ALBUMS = 400
NUM_SONGS = 1200
NUM_USERS = 1100     # >= 1000 required
NUM_PLAYS = 3500     # play records (many-to-many)

GENRES = [
    "Pop","Rock","Hip-Hop","Electronic","Jazz","Classical","Country",
    "R&B","Reggae","Metal","Folk","Indie"
]

USER_TIERS = ["Free", "Basic", "Premium", "Family"]   # Ordinal: Free < Basic < Premium < Family
AUDIO_QUALITIES = ["Low", "Medium", "High"]           # Ordinal

# -------------------- Functions --------------------
def random_year(start=1980, end=2024):
    return random.randint(start, end)

def random_duration_seconds():
    # typical song 120s - 420s
    return random.randint(120, 420)

def random_popularity():
    # interval-style (0-100), arbitrary zero
    return round(random.uniform(0, 100), 2)

def random_listen_score():
    # interval-style (0-10)
    return round(random.uniform(0, 10), 2)

# -------------------- Generate DataFrames --------------------

# Artists
artists = []
for aid in range(1, NUM_ARTISTS + 1):
    artists.append({
        "artist_id": aid,
        "artist_name": fake.name(),
        "country": fake.country()
    })
artists_df = pd.DataFrame(artists)

# Albums
albums = []
for alid in range(1, NUM_ALBUMS + 1):
    artist = random.choice(artists_df["artist_id"].tolist())
    albums.append({
        "album_id": alid,
        "album_name": f"{fake.word().capitalize()} {random.choice(['Vol','Collection','Series'])} {random.randint(1,99)}",
        "artist_id": artist,
        "genre": random.choice(GENRES),
        "release_year": random_year(1980, 2024)   # year only
    })
albums_df = pd.DataFrame(albums)

# Songs
songs = []
for sid in range(1, NUM_SONGS + 1):
    album_row = albums_df.sample(1).iloc[0]
    duration = random_duration_seconds()
    songs.append({
        "song_id": sid,
        "title": f"{fake.word().capitalize()} {random.choice(['Love','Night','Dream','Light','Sound'])}",
        "album_id": int(album_row["album_id"]),
        "duration_seconds": duration,               # ratio (meaningful zero)
        "popularity_index": random_popularity()     # interval (0-100)
    })
songs_df = pd.DataFrame(songs)

# Users (1000+)
users = []
for uid in range(1, NUM_USERS + 1):
    name = fake.name()
    gender = random.choice(["Male", "Female", "Non-binary", "Prefer not to say"])   # nominal
    tier = random.choices(USER_TIERS, weights=[0.5, 0.3, 0.15, 0.05])[0]   # ordinal
    reg_year = random_year(2010, 2024)    # registration year only
    favorite_genre = random.choice(GENRES)
    total_listens = 0.0
    users.append({
        "user_id": uid,
        "user_name": name,
        "gender": gender,
        "user_tier": tier,
        "registration_year": reg_year,
        "favorite_genre": favorite_genre,
        "total_listens": total_listens   # ratio
    })
users_df = pd.DataFrame(users)

# Inject some missing contact-like fields (we'll add email column and leave some missing)
users_df["email"] = users_df["user_name"].apply(lambda n: fake.email() if random.random() > 0.05 else None)
# Introduce occasional duplicate names/emails to simulate real data issues
for _ in range(int(len(users_df) * 0.02)):
    i = random.randrange(len(users_df))
    j = random.randrange(len(users_df))
    users_df.at[i, "user_name"] = users_df.at[j, "user_name"]
    if random.random() < 0.5:
        users_df.at[i, "email"] = users_df.at[j, "email"]

# Plays (composite PK: user_id, song_id, session_no)
plays = []
# We'll create multiple play sessions per user/song; session_no increments per (user, song)
session_counter = {}
for pid in range(1, NUM_PLAYS + 1):
    user = int(random.choice(users_df["user_id"].tolist()))
    song = int(random.choice(songs_df["song_id"].tolist()))
    key = (user, song)
    session_counter[key] = session_counter.get(key, 0) + 1
    session_no = session_counter[key]
    play_year = random_year(2018, 2024)   # only year
    listen_score = random_listen_score()   # interval
    audio_quality = random.choice(AUDIO_QUALITIES)  # ordinal
    plays.append({
        "user_id": user,
        "song_id": song,
        "session_no": session_no,
        "play_year": play_year,
        "listen_score": listen_score,
        "audio_quality": audio_quality
    })
plays_df = pd.DataFrame(plays)

# Update users.total_listens aggregate (ratio)
user_listen_counts = plays_df.groupby("user_id").size().reset_index(name="counts")
users_df = users_df.merge(user_listen_counts, how="left", left_on="user_id", right_on="user_id")
users_df["counts"] = users_df["counts"].fillna(0).astype(int)
users_df["total_listens"] = users_df["counts"]
users_df.drop(columns=["counts"], inplace=True)

# -------------------- Create SQLite DB and write tables --------------------

conn = sqlite3.connect(DB_FILENAME)
cur = conn.cursor()
cur.execute("PRAGMA foreign_keys = ON;")

# Create schema with appropriate constraints and composite PK for Plays
schema = """
CREATE TABLE IF NOT EXISTS Artists (
    artist_id INTEGER PRIMARY KEY,
    artist_name TEXT NOT NULL,
    country TEXT
);

CREATE TABLE IF NOT EXISTS Albums (
    album_id INTEGER PRIMARY KEY,
    album_name TEXT NOT NULL,
    artist_id INTEGER NOT NULL,
    genre TEXT,
    release_year INTEGER,
    FOREIGN KEY(artist_id) REFERENCES Artists(artist_id)
);

CREATE TABLE IF NOT EXISTS Songs (
    song_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    album_id INTEGER NOT NULL,
    duration_seconds INTEGER CHECK(duration_seconds >= 0),
    popularity_index REAL,
    FOREIGN KEY(album_id) REFERENCES Albums(album_id)
);

CREATE TABLE IF NOT EXISTS Users (
    user_id INTEGER PRIMARY KEY,
    user_name TEXT NOT NULL,
    gender TEXT,
    user_tier TEXT,
    registration_year INTEGER,
    favorite_genre TEXT,
    total_listens INTEGER CHECK(total_listens >= 0),
    email TEXT
);

CREATE TABLE IF NOT EXISTS Plays (
    user_id INTEGER NOT NULL,
    song_id INTEGER NOT NULL,
    session_no INTEGER NOT NULL,
    play_year INTEGER,
    listen_score REAL,
    audio_quality TEXT,
    PRIMARY KEY (user_id, song_id, session_no),
    FOREIGN KEY(user_id) REFERENCES Users(user_id),
    FOREIGN KEY(song_id) REFERENCES Songs(song_id)
);
"""
cur.executescript(schema)
conn.commit()

# Use pandas to_sql to insert data (index=False)
artists_df.to_sql("Artists", conn, if_exists="append", index=False)
albums_df.to_sql("Albums", conn, if_exists="append", index=False)
songs_df.to_sql("Songs", conn, if_exists="append", index=False)
users_df.to_sql("Users", conn, if_exists="append", index=False)
plays_df.to_sql("Plays", conn, if_exists="append", index=False)

# final commit and close
conn.commit()

# Quick integrity checks and prints
cur.execute("SELECT COUNT(*) FROM Users")
print("Users:", cur.fetchone()[0])
cur.execute("SELECT COUNT(*) FROM Songs")
print("Songs:", cur.fetchone()[0])
cur.execute("SELECT COUNT(*) FROM Plays")
print("Plays:", cur.fetchone()[0])

# Foreign key check
cur.execute("PRAGMA foreign_key_check;")
fk_issues = cur.fetchall()
if fk_issues:
    print("Foreign key issues:", fk_issues)
else:
    print("No foreign key issues detected.")

conn.close()
print("Database generated:", DB_FILENAME)
