#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""This module performs some simple sanity tests on the sparkify db to make sure
data is present and of the correct dimensions.

Dependencies: This module must be called after the create_tables.py module is run
"""
__author__ = "Tim Fenton"
__copyright__ = "Copyright 2019"
__credits__ = ["Tim Fenton"]
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "Tim Fenton"
__email__ = "tfenton@gmail.com"
__status__ = "Production"

from sql_queries import *
from create_tables import *
import psycopg2
import unittest

class SparkifyTests(unittest.TestCase):
    
    def test_connection(self):
        '''Tests ability to connect to the database'''
        try:
            conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
            self.assertIsNotNone(conn)
            conn.close()
        except Exception as e:
            print('failed to create db connection')
            print(e)
    
    def test_cursor(self):
        '''Test the ability to connect to the db and open a cursor'''
        try:
            conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
            cur = conn.cursor()
            self.assertIsNotNone(cur)
            conn.close()
        except Exception as e:
            print('failed to create db cursor')
            print(e)
    
    def test_songs(self):
        '''Test that the songs table is created and has the correct shape'''
        try:
            conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
            cur = conn.cursor()
            cur.execute('SELECT * FROM songs LIMIT 5')
            row = cur.fetchone()
            self.assertTrue(len(row) == 5)
            self.assertTrue(type(row) == tuple)
            conn.close()
        except Exception as e:
            print('failed to validate songs table')
            print(e)
    
    def test_artists(self):
        '''Test that the artists table is created and has the correct shape'''
        try:
            conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
            cur = conn.cursor()
            cur.execute('SELECT * FROM artists LIMIT 5')
            row = cur.fetchone()
            self.assertTrue(len(row) == 5)
            self.assertTrue(type(row) == tuple)
            conn.close()
        except Exception as e:
            print('failed to validate artists table')
            print(e)
    
    def test_songplays(self):
        '''Test that the songplays table is created and has the correct shape'''
        try:
            conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
            cur = conn.cursor()
            cur.execute('SELECT * FROM songplays LIMIT 5')
            row = cur.fetchone()
            self.assertTrue(len(row) == 9)
            self.assertTrue(type(row) == tuple)
            conn.close()
        except Exception as e:
            print('failed to validate songplays table')
            print(e)
            
    def test_users(self):
        '''Test that the users table is created and has the correct shape'''
        try:
            conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
            cur = conn.cursor()
            cur.execute('SELECT * FROM users LIMIT 5')
            row = cur.fetchone()
            self.assertTrue(len(row) == 5)
            self.assertTrue(type(row) == tuple)
            conn.close()
        except Exception as e:
            print('failed to validate users table')
            print(e)
            
    def test_time(self):
        '''Test that the time table is created and has the correct shape'''
        try:
            conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
            cur = conn.cursor()
            cur.execute('SELECT * FROM time LIMIT 5')
            row = cur.fetchone()
            self.assertTrue(len(row) == 7)
            self.assertTrue(type(row) == tuple)
            conn.close()
        except Exception as e:
            print('failed to validate time table')
            print(e)
            
    def test_table_create(self):
        '''Test that the table creation statement wont cause and error if run multiple times'''
        stmt = None
        try:
            conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
            cur = conn.cursor()
            for stmt in create_table_queries:
                cur.execute(stmt)
                cur.execute(stmt)
            conn.close()
        except Exception as e:
            print('Error creating table. SQL erroring follows:')
            print(stmt)
            print(e)

if __name__ == '__main__':
    unittest.main(argv=['ignore'], exit=False)