import configparser
import psycopg2
import boto3
from connect import connect
from sql_queries import create_table_queries, drop_table_queries, schema_queries


def drop_tables(cur, conn):
    """Drops each table using the queries in `drop_table_queries` list.
    
    Keyword arguments:
    cur -- the cursor for interating with the PostgreSQL database.
    con - the connection object for the PostgreSQL database.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Creates each table using the queries in `create_table_queries` list.
    
    Keyword arguments:
    cur -- the cursor for interating with the PostgreSQL database.
    con - the connection object for the PostgreSQL database.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Drops and creates tables on Redshift cluster.
    - Parses the config file and extracts credentials.
    - Creates a Redshift instance.
    - Checks cluster connects and retrieves endpoint and arn.
    - Establishes PostgreSQL connection to Redshift, gets cursor.  
    - Drops all the tables (if they exist).  
    - Creates all tables needed. 
    - Finally, closes the connection. 
    """
    cur, conn = connect('dwh.cfg')
    
    create_schema = schema_queries[0]
    set_schema = schema_queries[1]
    
    cur.execute(create_schema)
    cur.execute(set_schema)
    drop_tables(cur, conn)
    
    cur.execute(create_schema)
    cur.execute(set_schema)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
