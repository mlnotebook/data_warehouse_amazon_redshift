import configparser

# CONFIG
"""Parse the config file."""
config = configparser.ConfigParser()
config.read('dwh.cfg')

# SCHEMA
"""Create the schema and set the search_path."""
schema_create = ("""
CREATE SCHEMA IF NOT EXISTS {};
""".format(config.get('CLUSTER', 'SCHEMA')))
schema_set = ("""
SET search_path TO {};
""".format(config.get('CLUSTER', 'SCHEMA')))

# DROP TABLES
"""Queries for dropping tables when rerunning ETL."""

staging_events_table_drop = "DROP TABLE IF EXISTS stagingEvent"
staging_songs_table_drop = "DROP TABLE IF EXISTS stagingSong"
songplay_table_drop = "DROP TABLE IF EXISTS dimSongplay"
user_table_drop = "DROP TABLE IF EXISTS dimUser"
song_table_drop = "DROP TABLE IF EXISTS dimSong"
artist_table_drop = "DROP TABLE IF EXISTS dimArtist"
time_table_drop = "DROP TABLE IF EXISTS dimTime"

# CREATE TABLES
""" Queries for creating staging, fact and dimension tables.
Staging Tables: stagingEvent, stagingSong
Fact Table: factSongplay
Dimension Tables: dimUser, dimSong, dimArtist, dimTime
Distribution: ALL for Dimension Tables, KEY for Fact Table.
"""
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS stagingEvent
(
    artist VARCHAR,
    auth VARCHAR, 
    firstName VARCHAR,
    gender VARCHAR,   
    itemInSession INTEGER,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR, 
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration BIGINT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts TIMESTAMP,
    userAgent VARCHAR,
    userId INTEGER
)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS stagingSong
(
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INTEGER
)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS factSongplay
(
    sp_songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    sp_start_time TIMESTAMP NOT NULL,
    sp_user_id INTEGER NOT NULL,
    sp_level VARCHAR NOT NULL,
    sp_song_id VARCHAR NOT NULL SORTKEY,
    sp_artist_id VARCHAR NOT NULL DISTKEY,
    sp_session_id INTEGER NOT NULL,
    sp_location VARCHAR NOT NULL,
    sp_user_agent VARCHAR NOT NULL
)
DISTSTYLE KEY;
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS dimUser
(
    u_user_id VARCHAR NOT NULL PRIMARY KEY SORTKEY,
    u_first_name VARCHAR NOT NULL,
    u_last_name VARCHAR NOT NULL,
    u_gender VARCHAR NOT NULL,
    u_level VARCHAR NOT NULL
)
DISTSTYLE ALL;
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS dimSong
(
    s_song_id VARCHAR NOT NULL PRIMARY KEY,
    s_title VARCHAR NOT NULL,
    s_artist_id VARCHAR NOT NULL SORTKEY,
    s_year INT NOT NULL,
    s_duration FLOAT NOT NULL
)
DISTSTYLE ALL;
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS dimArtist
(
    a_artist_id VARCHAR NOT NULL PRIMARY KEY SORTKEY,
    a_name VARCHAR NOT NULL,
    a_location VARCHAR,
    a_latitude FLOAT,
    a_longitude FLOAT
)
DISTSTYLE ALL;
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS dimTime
(
    t_start_time TIMESTAMP NOT NULL PRIMARY KEY SORTKEY,
    t_hour INT NOT NULL,
    t_day INT NOT NULL,
    t_week INT NOT NULL,
    t_month INT NOT NULL,
    t_year INT NOT NULL,
    t_weekday INT NOT NULL
)
DISTSTYLE ALL;
""")

# STAGING TABLES
"""Queries for copying the staging tables from S3 to Redshift."""
staging_events_copy = ("""COPY {}.{} from {} 
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    BLANKSASNULL
    EMPTYASNULL
    FORMAT AS JSON {};
""").format(config.get('CLUSTER', 'SCHEMA'),
            'stagingEvent',
            config.get('S3','LOG_DATA'),
            config.get('IAM_ROLE','ARN'),
            config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""COPY {}.{} from {} 
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    BLANKSASNULL
    EMPTYASNULL
    FORMAT AS JSON 'auto';    
""").format(config.get('CLUSTER', 'SCHEMA'),
            'stagingSong',
            config.get('S3','SONG_DATA'),
            config.get('IAM_ROLE','ARN'))

# FINAL TABLES
"""Queries for inserting rows into Fact and Dimension tables from Staging tabels."""
songplay_table_insert = ("""INSERT INTO factSongplay
(sp_start_time, sp_user_id, sp_level, sp_song_id, sp_artist_id, sp_session_id, sp_location, sp_user_agent)
SELECT DISTINCT(e.ts) AS sp_start_time,
    e.userId AS sp_user_id,
    e.level AS sp_level,
    s.song_id AS sp_song_id,
    s.artist_id AS sp_artist_id,
    e.sessionId AS sp_session_id,
    e.location AS sp_location,
    e.userAgent AS sp_user_agent
FROM stagingSong s
JOIN stagingEvent e ON (e.artist=s.artist_name AND e.song=s.title)
WHERE e.page='NextSong';
""") 

user_table_insert = ("""INSERT INTO dimUser (u_user_id, u_first_name, u_last_name, u_gender, u_level)
SELECT DISTINCT(e.userId) AS u_user_id,
    e.firstName AS u_first_name,
    e.lastName AS u_last_name,
    e.gender AS u_gender,
    e.level AS u_level
FROM stagingEvent e
WHERE (e.userId IS NOT NULL AND e.page='NextSong');
""")

song_table_insert = ("""INSERT INTO dimSong (s_song_id, s_title, s_artist_id, s_year, s_duration)
SELECT DISTINCT(s.song_id) AS s_song_id,
    s.title AS s_title,
    s.artist_id AS s_artist_id,
    s.year AS s_year,
    s.duration AS s_duration
FROM stagingSong s
WHERE s.song_id IS NOT NULL;
""")

artist_table_insert = ("""INSERT INTO dimArtist (a_artist_id, a_name, a_location, a_latitude, a_longitude)
SELECT DISTINCT(s.artist_id) AS a_artist_id,
    s.artist_name AS a_name,
    s.artist_location AS a_location,
    s.artist_latitude AS a_latitude,
    s.artist_longitude AS a_longitude
FROM stagingSong s
WHERE s.artist_id IS NOT NULL;
""")

time_table_insert = ("""INSERT INTO dimTime (t_start_time, t_hour, t_day, t_week, t_month, t_year, t_weekday)
SELECT DISTINCT(e.ts) AS t_start_time,
    EXTRACT(hour FROM e.ts) AS t_hour,
    EXTRACT(day FROM e.ts) AS t_day,
    EXTRACT(week FROM e.ts) AS t_week,
    EXTRACT(month FROM e.ts) AS t_month,
    EXTRACT(year FROM e.ts) AS t_year,
    EXTRACT(weekday FROM e.ts) AS t_weekday
FROM stagingEvent e
WHERE e.ts IS NOT NULL;
""")

# QUERY LISTS
"""Grouping the queries as lists for: creating, dropping, copying and inserting."""
schema_queries = [schema_create, schema_set]
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
