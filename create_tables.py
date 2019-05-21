#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""This executable module is used as an import for the etl and create_tables 
modules. It contains sql which is run by these repective modules.
"""
__author__ = "Tim Fenton"
__copyright__ = "Copyright 2019"
__credits__ = ["Tim Fenton"]
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "Tim Fenton"
__email__ = "tfenton@gmail.com"
__status__ = "Production"

import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    '''Connect to the local instance of Postgress over TCP
    and create the sparkifydb.
    '''
    # connect to default database
    try:
        conn = psycopg2.connect(
            "host=127.0.0.1 dbname=studentdb user=student password=student")
        conn.set_session(autocommit=True)
        cur = conn.cursor()
    except Exception as e:
        print(e)

    # create sparkify database with UTF8 encoding
    try:
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
        cur.execute(
            "CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    except Exception as e:
        print(e)

    # close connection to default database
    conn.close()

    # connect to sparkify database
    try:
        conn = psycopg2.connect(
            "host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
    except Exception as e:
        print(e)

    return cur, conn


def drop_tables(cur, conn):
    """Drop all tables via the queries stored in the 'drop_table_queries'
    list that is imported from sql_queries module.

    Keyword arguments:
    cur - open cursor to the sparkify database
    conn - open connection to the sparkify database
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(query, e)


def create_tables(cur, conn):
    """Create all tables via the queries stored in the 'create_table_queries'
    list that is imported from sql_queries module.

    Keyword arguments:
    cur - open cursor to the sparkify database
    conn - open connection to the sparkify database
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(query, e)


def main():
    """Create the sparkify database, drop any tables currently in it,
    and then recreate those tables.
    """
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
