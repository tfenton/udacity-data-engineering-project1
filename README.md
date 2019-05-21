# Data Modeling with Postgres

This is the first project in the Udacity Data Engineering nano degree. It covers relational database structures Postgres as the engine and a star scheme to tie data from dimension tables into fact tables. It consists of two python modules the first of which create the database and requiste tables and the second of which performs extract, transform and load (ETL) from JSON formated data contained in numerous files and directories, pushing this data into the database.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. 

### Prerequisites

You must have Postgres version 9.5.16 installed and available on the loopback address (127.0.0.1) of your machine prior to running any scripts. Additionally, you must have an empty database called `studentdb` with a username `student` and password `student`. 

Non-standard library Python dependencies are:
- psycopg2 : Postgres python module
- pandas : Dataframe manipulation module 

```
Check version: psql -V
Connect to db in python
```

### Installing

A `requirements.txt` file is provided to create the test/deployment environment for this project.

1. Create a virtual environment to run this project

```
python3 -m venv /path/to/new/virtual/env
```

2. Activate the virtual environment

```
source /path/to/new/virtual/env/bin/activate
```

3. Install the packages used for this project

```
pip install -r requirements.txt
```

4. (Optional) - Make the files executable

```
chmod a+x create_tables.py etl.py
```

## Create and populate DB

1. Create and initialize the database and tables. This also drops any existing database/tables.

```
python create_tables.py
```

2. Insert the data into the tables

```
python etl.py
```

## Running the tests

Tests are available using python's builtin unittest framework. These tests will verify that all tables exist, the data base can be connected to and that the create statements don't generate an error when run multiple times.

````
python tests.py
........
----------------------------------------------------------------------
Ran 8 tests in 0.082s

OK
```

### And coding style tests

Coding style was checked with the `pycodestyle` module (https://pypi.org/project/pycodestyle/).

```
Install: `pip install pycodestyle`
Usage: pycodestyle --show-source --show-pep *.py
```

Coding style was brought into pep8 compliance based on output of the above module followed by manual fixes as well as use of the `autopep8` module

```
Install: pip install autopep8
Usage: autopep8 -i etl.py
autopep8 -i sql_queries.py
autopep8 -i create_tables.py
```

## Built With

* [pandas](https://pandas.pydata.org) - Data analysis/manipulation
* [psycopg2](http://initd.org/psycopg/docs/) - Postgres database adapter for python

## Authors

* **Tim Fenton** - *Initial work* -

## License

This project is licensed under the GPL License - see the [license.txt](license.txt) file for details