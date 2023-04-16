# This python file is used to load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift. 

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# This function loads data from S3 into staging tables on Redshift
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

# This function processes data from staging tables into analytics tables on Redshift
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()