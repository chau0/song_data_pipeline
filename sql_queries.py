import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop ="DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
   CREATE TABLE staging_events(
      artist varchar distkey,
      auth varchar,
      firstName varchar,
      gender varchar(1),
      itemInSession integer,
      lastName varchar,
      length float ,
      level varchar,
      location text,
      method varchar,
      page varchar,
      registration float,
      sessionId integer,
      song varchar,
      status integer,
      ts bigint,
      userAgent TEXT,
      userId varchar
   )
""")

staging_songs_table_create = ("""
   CREATE TABLE staging_songs(
      artist_id varchar,
      artist_latitude float,
      artist_location text,
      artist_longitude float,
      artist_name varchar distkey,
      duration float ,
      num_songs integer,
      song_id varchar,
      title varchar,
      year integer)
""")

songplay_table_create = ("""
    CREATE TABLE songplays(
       songplay_id integer IDENTITY(0,1),
       start_time timestamp,
       user_id varchar,
       level varchar,
       song_id varchar,
       artist_id varchar,
       session_id integer,
       location text,
       user_agent text
    )                      
""")

user_table_create = ("""
   CREATE TABLE users(
      user_id varchar,
      first_name varchar,
      last_name varchar,
      gender varchar(1),
      level varchar
   )
""")

song_table_create = ("""
   CREATE TABLE songs(
      song_id varchar,
      title varchar,
      artist_id varchar,
      year integer,
      duration float
   )
""")

artist_table_create = ("""
   CREATE TABLE artists(
      artist_id varchar,
      name varchar,
      location text,
      latitude float,
      longitude float
   )
""")

time_table_create = ("""
   CREATE TABLE time(
      start_time bigint,
      hour int,
      day int,
      week int,
      month int,
      year int,
      weekday int
   )
   """)

# STAGING TABLES

staging_events_copy = ("""
   copy staging_events
   from {}
   credentials 'aws_iam_role={}'
   json {};
""").format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE","ARN"), config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""
   copy staging_songs
   from {}
   credentials 'aws_iam_role={}'
   json 'auto';      
""").format(config.get("S3","SONG_DATA"), config.get("IAM_ROLE","ARN"))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
    SELECT DISTINCT timestamp with time zone 'epoch' + t1.ts/1000 *INTERVAL '1 second', 
           t1.userId, t1.level, t2.song_id, t2.artist_id, t1.sessionId, t1.location, t1.userAgent
    FROM staging_events AS t1
    JOIN staging_songs AS t2
    ON t1.song = t2.title AND t1.artist = t2.artist_name AND t1.length = t2.duration
    WHERE t1.page ='NextSong'
""")

user_table_insert = ("""
   INSERT INTO users(user_id,first_name,last_name,gender,level)
   SELECT DISTINCT userId, firstName, lastName, gender, level
   FROM staging_events
   WHERE staging_events.page ='NextSong'
""")

song_table_insert = ("""
   INSERT INTO songs(song_id,title,artist_id,year,duration)
   SELECT song_id,title,artist_id,year,duration 
   FROM staging_songs
""")

artist_table_insert = ("""
   INSERT INTO artists(artist_id,name,locattion,latitue,longitude)
   SELECT DISTINCT artist_id,artist_name,artist_location,artist_latitude,artist_longitude
   FROM staging_songs
""")

time_table_insert = ("""
   INSERT INTO time(start_time, hour, day, week, month, year, weekday)
   SELECT start_time, 
         extract(hour from start_time), 
         extract(day from start_time), 
         extract(week from start_time), 
         extract(month from start_time), 
         extract(year from start_time), 
         extract(weekday from start_time)
   FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
