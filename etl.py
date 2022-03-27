import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from S3 to staging table
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data from staging table to redshift database

    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    - Establishes connection with the redshift database and gets
    cursor to it.
    - load data to redshift and insert to fact and dimesion tables
    - Finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()