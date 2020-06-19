import configparser
import psycopg2
from time import time
from connect import connect
from sql_queries import copy_table_queries, insert_table_queries, schema_queries


def load_staging_tables(cur, conn):
    """Loads the staging tables
    COPYs the data from S3 to staging table.
    
    Keyword arguments:
    cur -- the cursor for interating with the PostgreSQL database.
    con - the connection object for the PostgreSQL database.
    """
    for idx, query in enumerate(copy_table_queries):
        print('Executing COPY query {}/{}...'.format(idx+1, len(copy_table_queries)), end='', flush=True)
        t0 = time()
        cur.execute(query)
        conn.commit()
        load_time = time() - t0
        print('done! Took: {0:.2f} sec'.format(load_time))


def insert_tables(cur, conn):
    """Fills the fact and dimension tables
    INSERTs the data from staging table into fact and dimension tables.
    
    Keyword arguments:
    cur -- the cursor for interating with the PostgreSQL database.
    con - the connection object for the PostgreSQL database.
    """
    for idx, query in enumerate(insert_table_queries):
        print('Executing INSERT query {}/{}...'.format(idx+1, len(insert_table_queries)), end='', flush=True)
        t0 = time()
        cur.execute(query)
        conn.commit()
        load_time = time() - t0
        print('done! Took: {0:.2f} sec'.format(load_time))


def main():
    """Copies data from S3 .json files into the staging tables,
    then inserts data into fact and dimension tables.
    """
    cur, conn = connect('dwh.cfg')
    
    set_schema = schema_queries[1]
    cur.execute(set_schema)
    
    print('Loading Staging Tables.')
    load_staging_tables(cur, conn)
    
    print('Inserting Rows.')
    insert_tables(cur, conn)

    
    conn.close()


if __name__ == "__main__":
    main()
