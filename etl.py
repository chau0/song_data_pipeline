import configparser
import os
from pyspark.sql.functions import udf, col, to_date
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, \
    monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['SERVER']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['SERVER']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    process song data from s3 and write output to s3
    Args:
        spark : spark session
        input_data : input data from s3
        output_data : output data from s3
    """
    # get filepath to song data file
    song_data = f"{input_data}/song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("song_data_view")

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy('year', 'artist_id').parquet(path=output_data + 'songs')

    # extract columns to create artists table
    artists_table = df.select('artist_id', col('artist_name').alias('name')\
                              ,col('artist_location').alias('location')\
                              ,col('artist_latitude').alias('latitude')\
                              ,col('artist_longitude').alias('longitude')).distinct()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(path=output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    """
    process log data

    Args:
        spark : spark session
        input_data : input data from s3
        output_data : output data from s3
    """
    # get filepath to log data file
    log_data = f"{input_data}/log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df['page'] == 'NextSong')

    # extract columns for users table    
    users_table = df.select(col('userId').alias('user_id')\
                            ,col('firstName').alias('first_name')\
                            ,col('lastName').alias('last_name')\
                            ,'gender'\
                            ,'level').distinct()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(path=output_data + 'users')

    # create timestamp column from original timestamp column
    df = df.withColumn('start_time', (df['ts']/1000).cast('timestamp'))
    
    # create datetime column from original timestamp column
    df = df.withColumn('hour', hour(df['start_time']))
    df = df.withColumn('day', dayofmonth(df['start_time']))
    df = df.withColumn('week', weekofyear(df['start_time']))
    df = df.withColumn('weekday', dayofweek(df['start_time']))
    df = df.withColumn('month', month(df['start_time']))
    df = df.withColumn('year', year(df['start_time']))
   
    
    # extract columns to create time table
    time_table = df.select('start_time', 'hour', 'day', 'week','month', 'year', 'weekday').distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(path=output_data + 'time')

    # read in song data to use for songplays table
    song_df = spark.sql("SELECT * FROM song_data_view")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) \
                              & (df.artist == song_df.artist_name) \
                              & (df.length == song_df.duration), "inner")\
                        .distinct()\
                        .select('start_time'
                                ,col('userId').alias('user_id')
                                ,'level'
                                ,'song_id','artist_id'
                                ,col('sessionId').alias('session?id'),'location'
                                ,col('userAgent').alias('user_agent')\
                                ,df['year'].alias('year')\
                                , df['month'].alias('month'))\
                        .withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy('year', 'month').parquet(path=output_data + 'songplays')


def main():
    """
    process song data and log data and write output to s3 setting inf config file
    """
    spark = create_spark_session()
    input_data = config['S3']['INPUT']
    output_data = config['S3']['OUT_PUT']
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
