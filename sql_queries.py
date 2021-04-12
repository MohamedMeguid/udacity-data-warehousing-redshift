import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = """
    DROP TABLE IF EXISTS staging_events
"""
staging_songs_table_drop = """
    DROP TABLE IF EXISTS staging_songs
"""
songplay_table_drop = """
    DROP TABLE IF EXISTS songplays
"""
user_table_drop = """
    DROP TABLE IF EXISTS users
"""
song_table_drop = """
    DROP TABLE IF EXISTS songs
"""
artist_table_drop = """
    DROP TABLE IF EXISTS artists
"""
time_table_drop = """
    DROP TABLE IF EXISTS time
"""

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR,
        itemInSession INT,
        lastName VARCHAR,
        length NUMERIC,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration NUMERIC,
        sessionId INT,
        song VARCHAR,
        status SMALLINT,
        ts NUMERIC,
        userAgent VARCHAR,
        userId BIGINT
    )                              
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INT,
        artist_id VARCHAR,
        artist_latitude NUMERIC,
        artist_longitude NUMERIC,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration NUMERIC,
        year SMALLINT
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id BIGINT IDENTITY(0, 1) PRIMARY KEY, 
        start_time NUMERIC NOT NULL SORTKEY DISTKEY, 
        user_id BIGINT NOT NULL, 
        level VARCHAR, 
        song_id VARCHAR NOT NULL, 
        artist_id VARCHAR,
        session_id BIGINT NOT NULL, 
        location VARCHAR, 
        user_agent VARCHAR
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id BIGINT PRIMARY KEY, 
        first_name VARCHAR, 
        last_name VARCHAR, 
        gender VARCHAR, 
        level VARCHAR
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY, 
        title VARCHAR NOT NULL, 
        artist_id VARCHAR, 
        year SMALLINT, 
        duration NUMERIC
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY, 
        name VARCHAR, 
        location VARCHAR, 
        latitude NUMERIC, 
        longitude NUMERIC
    ) DISTSTYLE All
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time NUMERIC PRIMARY KEY SORTKEY, 
        hour SMALLINT NOT NULL, 
        day SMALLINT NOT NULL, 
        week SMALLINT NOT NULL, 
        month SMALLINT NOT NULL, 
        year SMALLINT NOT NULL, 
        weekday SMALLINT NOT NULL
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    IAM_ROLE {}
    region 'us-west-2'
    format as json {}
""").format(config.get('S3', 'LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    IAM_ROLE {}
    region 'us-west-2'
    format as json 'auto'
""").format(config.get('S3', 'SONG_DATA'),
            config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, 
                           artist_id, session_id, location, user_agent)

    SELECT 
        
        DISTINCT
        staging_events.ts,
        staging_events.userId,
        staging_events.level,
        staging_songs.song_id,
        staging_songs.artist_id,
        staging_events.sessionId,
        staging_events.location,
        staging_events.userAgent

    FROM staging_events
    INNER JOIN staging_songs 
        ON staging_events.song = staging_songs.title
        AND staging_events.artist = staging_songs.artist_name
    WHERE staging_events.page='NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT
        DISTINCT
        userId,
        firstName,
        lastName,
        gender,
        level
    FROM staging_events
    WHERE staging_events.page='NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT
        DISTINCT    
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    
    SELECT
    
        DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    
    SELECT 
        
        EXTRACT(EPOCH FROM ts)::BIGINT * 1000 as start_time,
        EXTRACT(HOUR FROM ts) AS hour,
        EXTRACT(DAY FROM ts) AS day,
        EXTRACT(WEEK FROM ts) AS week,
        EXTRACT(MONTH FROM ts) AS month,
        EXTRACT(YEAR FROM ts) AS year,
        EXTRACT(DOW FROM ts) AS weekday
    
    FROM (SELECT 
              TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 SECOND' AS ts 
          FROM staging_events) t
""")

# QUERY DICTIONARIES

create_table_queries = {"staging_events_table_create": staging_events_table_create, 
                        "staging_songs_table_create": staging_songs_table_create, 
                        "songplay_table_create": songplay_table_create, 
                        "user_table_create": user_table_create, 
                        "song_table_create": song_table_create, 
                        "artist_table_create": artist_table_create, 
                        "time_table_create": time_table_create}

drop_table_queries = {"staging_events_table_drop": staging_events_table_drop, 
                      "staging_songs_table_drop": staging_songs_table_drop, 
                      "songplay_table_drop": songplay_table_drop, 
                      "user_table_drop": user_table_drop, 
                      "song_table_drop": song_table_drop, 
                      "artist_table_drop": artist_table_drop, 
                      "time_table_drop": time_table_drop}

copy_table_queries = {"staging_events_copy": staging_events_copy, 
                      "staging_songs_copy": staging_songs_copy}

insert_table_queries = {"songplay_table_insert": songplay_table_insert, 
                        "user_table_insert": user_table_insert, 
                        "song_table_insert": song_table_insert, 
                        "artist_table_insert": artist_table_insert, 
                        "time_table_insert": time_table_insert}
