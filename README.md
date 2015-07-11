# td-fdw

Multicorn based PostgreSQL Foreign Data Wrapper for Treasure Data. 
It makes your Treasure Data dataset appear as foreign tables in your PostgreSQL database.

## Installation
Install [Multicorn](http://multicorn.org/#installation) and build this FDW

    $ pgxn install multicorn
    $ cd td_fdw
    $ sudo pip install --upgrade .
      or
    $ python setup.py sdist
    $ sudo python setup.py install

## Setup
Connect to your PostgreSQL and create an extension and foreign server

    CREATE EXTENSION multicorn;
    
    CREATE SERVER td_server FOREIGN DATA WRAPPER multicorn
    OPTIONS (
        wrapper 'tdfdw.tdfdw.TreasureDataFdw'
    );

## Usage
Specify your API key, database, query engine type ('presto' or 'hive') in CREATE FOREIGN TABLE statement. You can specify either your table name or query for Treasure Data directly.

    CREATE FOREIGN TABLE sample_datasets (
        "user" integer,
        host varchar,
        path varchar,
        referer varchar,
        code integer,
        agent varchar,
        size integer,
        method varchar
    )
    SERVER td_server OPTIONS (
        apikey 'your_api_key',
        database 'sample_datasets',
        query_engine 'presto',
        table 'www_access'
    );

    SELECT code, count(1) FROM sample_datasets group BY code;
     code | count
    ------+-------
      404 |    17
      200 |  4981
      500 |     2
    (3 rows)

    CREATE FOREIGN TABLE nginx_status_summary (
        text varchar,
        cnt integer
    )
    SERVER td_server OPTIONS (
        apikey 'your_api_key',
        database 'api_staging',
        query_engine 'presto',
        query 'SELECT c.text, COUNT(1) AS cnt FROM nginx_access n
              JOIN mitsudb.codes c ON CAST(n.status AS bigint) = c.code
              WHERE TD_TIME_RANGE(n.time, ''2015-07-05'')
              GROUP BY c.text'
    );

    SELECT * FROM nginx_status_summary;
         text      |   cnt
    ---------------+----------
     OK            | 10123456
     Forbidden     |       12
     Unauthorized  |     3211
        :
    
    CREATE TABLE imported_summary AS SELECT * FROM nginx_status_summary;
    SELECT * FROM imported_summary;
         text      |   cnt
    ---------------+----------
     OK            | 10123456
     Forbidden     |       12
     Unauthorized  |     3211
        :
    
## Unit test

    $ python -m unittest discover -p tests
