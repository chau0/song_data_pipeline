# Overview

This project build an ETL pipeline load data from S3 and ingest to Amazon Redshift

## Entity relation diagram
![image info](./document/ER_diagram.png)

# Dataset
The [Million Song Dataset](http://millionsongdataset.com/) is a freely-available collection of audio features and metadata for a million contemporary popular music tracks.

# Source code files
- create_tables.py drops and creates tables. Run this file to reset tables before each time you run your ETL scripts.
- etl.py reads and processes files from song_data and log_data and loads them into tables.
- sql_queries.py contains all sql queries, and is imported into the last three files above.

# How to run project

## Setup  Amazon Redshift
https://docs.aws.amazon.com/redshift/index.html
## Setup table data
```
python create_tables.py
```

## Run ETL process
```
python etl.py
```

