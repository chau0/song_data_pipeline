# Overview

This project build an ETL pipeline load data from S3 and output to parquet format

## Entity relation diagram
![image info](./document/ER_diagram.png)

# Dataset
The [Million Song Dataset](http://millionsongdataset.com/) is a freely-available collection of audio features and metadata for a million contemporary popular music tracks.

# Source code files
- etl.py reads and processes files from song_data and log_data and output to parquet format into S3.


# How to run project
## Run ETL process
```
python etl.py
```

