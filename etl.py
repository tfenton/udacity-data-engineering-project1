#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""This module looks for data contained in *.json files and inserts it into a
Postgres database.

Dependencies: This module must be called after the create_tables one as its 
dependent upon this later module to setup the database.
"""
__author__ = "Tim Fenton"
__copyright__ = "Copyright 2019"
__credits__ = ["Tim Fenton"]
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "Tim Fenton"
__email__ = "tfenton@gmail.com"
__status__ = "Production"

import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Opens a single song file and inserts its contents into the song and artist tables.

    Keyword arguments:
    cur - Open database cursor
    filepath - str, location of a song file in json format
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    try:
        song_data = df[['song_id', 'title',
                        'artist_id', 'year', 'duration']].values[0]
        cur.execute(song_table_insert, song_data)
    except Exception as e:
        print('Error when inserting song data into the songs table')
        print(e)

    # insert artist record
    try:
        artist_data = df[['artist_id',
                          'artist_name',
                          'artist_location',
                          'artist_latitude',
                          'artist_longitude']].values[0]
        cur.execute(artist_table_insert, artist_data)
    except Exception as e:
        print('Error when inserting artist data into the artists table')
        print(e)

def process_log_file(cur, filepath):
    """Opens a single log file and inserts its contents into users, time, and songplays tables.
    Does a lookup on the song and artist tables to get get song and artist id's respectively.

    Keyword arguments:
    cur - Open database cursor
    filepath - str, location of a logfile in json format
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')

    # insert time data records
    time_data = (
        df.ts,
        t.dt.hour,
        t.dt.day,
        t.dt.weekofyear,
        t.dt.month,
        t.dt.year,
        t.dt.weekday)
    column_labels = (
        'start_time',
        'hour',
        'day',
        'week',
        'month',
        'year',
        'weekday')
    time_df = pd.DataFrame(
        {column_labels[i]: time_data[i] for i in range(len(time_data))})

    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except Exception as e:
            print('Error when inserting data into the time table')
            print(e)

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        artist = row.artist.replace("'", "''")
        try:
            cur.execute(song_select, (row.song, artist, row.length))
        except Exception as e:
            print('Error when selecting song and artist ids')
            print(e)
        
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row.itemInSession,
            row.ts,
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent)
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except Exception as e:
            print('Error when inserting data into the songplays table')
            print(e)


def process_data(cur, conn, filepath, func):
    """Processes all files in a path and inserts their data into the db.

    Arguments:
    cur - open cursor to the database
    conn - open connection to the database
    filepath - directory string in which to look for any json files
    func - function pointer to insert data (song, artist, songplays)
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """Entry point to this etl module.
    Connects to the data base and calls the functions to populate it with song,
    artist and songplays data.
    """
    try:
        conn = psycopg2.connect(
            "host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
    except Exception as e:
        print('Error connecting to the sparkify database')
        print(e)

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
