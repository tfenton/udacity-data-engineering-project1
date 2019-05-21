# -*- coding: utf-8 -*-

"""This module is used as an import for the etl and create_tables modules.
It contains sql which is run by these repective modules.

Dependencies: None
"""
__author__ = "Tim Fenton"
__copyright__ = "Copyright 2019"
__credits__ = ["Tim Fenton"]
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "Tim Fenton"
__email__ = "tfenton@gmail.com"
__status__ = "Production"

# DROP TABLES - SQL used to drop tables in the sparkify database

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES - DDL used to create the tables in the sparkify database

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                                                    songplay_id INT NOT NULL UNIQUE,
                                                    start_time BIGINT,
                                                    user_id INT,
                                                    level TEXT,
                                                    song_id TEXT,
                                                    artist_id TEXT,
                                                    session_id INT,
                                                    location TEXT,
                                                    user_agent TEXT,
                                                    PRIMARY KEY(songplay_id)
                                                    )
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                                            user_id INT NOT NULL UNIQUE,
                                            first_name TEXT,
                                            last_name TEXT,
                                            gender CHAR,
                                            level TEXT,
                                            PRIMARY KEY(user_id)
                                            )
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                                            song_id TEXT NOT NULL UNIQUE,
                                            title TEXT,
                                            artist_id TEXT,
                                            year INT,
                                            duration FLOAT,
                                            PRIMARY KEY(song_id)
                                            )
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                                                artist_id TEXT NOT NULL UNIQUE,
                                                name TEXT,
                                                location TEXT,
                                                lattitude FLOAT,
                                                longitude FLOAT,
                                                PRIMARY KEY(artist_id)
                                               )
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                                            start_time BIGINT,
                                            hour INT,
                                            day INT,
                                            week INT,
                                            month INT,
                                            year INT,
                                            weekday INT
                                          )
""")

# INSERT RECORDS SQL - SQL used to insert records into the sparkify database

songplay_table_insert = ("""INSERT INTO songplays (
                                                    songplay_id,
                                                    start_time,
                                                    user_id,
                                                    level,
                                                    song_id,
                                                    artist_id,
                                                    session_id,
                                                    location,
                                                    user_agent
                                                  )
                                               VALUES
                                                  (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                               ON CONFLICT DO NOTHING
""")

user_table_insert = ("""INSERT INTO users (
                                            user_id,
                                            first_name,
                                            last_name,
                                            gender,
                                            level
                                          )
                                        VALUES
                                          (%s,%s,%s,%s,%s)
                                        ON CONFLICT DO NOTHING
""")

song_table_insert = ("""INSERT INTO songs (
                                            song_id,
                                            title,
                                            artist_id,
                                            year,
                                            duration
                                          )
                                        VALUES
                                          (%s,%s,%s,%s,%s)
                                          ON CONFLICT DO NOTHING


""")

artist_table_insert = ("""INSERT INTO artists (
                                                artist_id,
                                                name,
                                                location,
                                                lattitude,
                                                longitude
                                              )
                                            VALUES
                                              (%s,%s,%s,%s,%s)
                                            ON CONFLICT DO NOTHING


""")


time_table_insert = ("""INSERT INTO time (
                                            start_time,
                                            hour,
                                            day,
                                            week,
                                            month,
                                            year,
                                            weekday
                                         )
                                      VALUES
                                         (%s,%s,%s,%s,%s,%s,%s)
                                      ON CONFLICT DO NOTHING


""")

# FIND SONGS - SQL used to find the song and artist id's when doing an insert
# into the songplays table

song_select = ("""SELECT s.song_id, a.artist_id 
       FROM songs s 
       LEFT JOIN artists a ON a.artist_id = s.artist_id 
       WHERE s.title = %s AND a.name = %s AND s.duration = %s
""")

# QUERY LISTS - these are imported into the create_tables model

create_table_queries = [
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create]
drop_table_queries = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop]
