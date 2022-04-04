# Overview

This project build an ETL pipeline load data from S3 and ingest to Amazon Redshift

## Entity relation diagram
![image info](./document/ER_diagram.png)

# Dataset
The [Million Song Dataset](http://millionsongdataset.com/) is a freely-available collection of audio features and metadata for a million contemporary popular music tracks.

# Source code files
- etl.py reads and processes files from song_data and log_data and loads them into tables.
- reate_tables.py drops and creates tables. Run this file to reset tables before each time you run your ETL scripts.


# How to run project
## Run ETL process
```
python etl.py
```

