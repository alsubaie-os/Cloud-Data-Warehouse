# This python file is used to create your fact and dimension tables for the star schema in Redshift. 
# You can run this file to reset your tables before each time you run your ETL scripts. 
# You can also add in this functionality to your ETL scripts to create the tables in Redshift before you run your ETL scripts to insert data.

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# This function drops all tables in the Redshift database if they exist
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

# This function creates the tables in the Redshift database
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()