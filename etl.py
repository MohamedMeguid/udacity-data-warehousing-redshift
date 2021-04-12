import configparser
import psycopg2
from datetime import datetime
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Loads data from S3 buckets into Redshift staging tables.
    
    Parameters
    ----------
    cur: `psycopg2.extensions.cursor`
        Postgres database cursor.
    conn: `psycopg2.extensions.connection`
        Postgres database connection handle.
    """
    
    for title, query in copy_table_queries.items():
        try:
            print("Executing query: ", title)
            t0 = datetime.now()
            cur.execute(query)
            conn.commit()
            print("Execution time: ", datetime.now() - t0)
        except psycopg2.Error as e:
            print("Error executing query: ", title)
            print(e)

def insert_tables(cur, conn):
    """Inserts data from the staging redshift tables into Redshift analytics tables.
    
    Parameters
    ----------
    cur: `psycopg2.extensions.cursor`
        Postgres database cursor.
    conn: `psycopg2.extensions.connection`
        Postgres database connection handle.
    """
    
    for title, query in insert_table_queries.items():
        try:
            print("Executing query: ", title)
            t0 = datetime.now()
            cur.execute(query)
            conn.commit()
            print("Execution time: ", datetime.now() - t0)
        except psycopg2.Error as e:
            print("Error executing query: ", title)
            print(e)


def main():
    """Main function to run, creates connection with the Redshift cluster and 
    calls functions that loads data into the data warehouse
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn = psycopg2.connect(
            "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
        )
        cur = conn.cursor()
        print("*** Connected Successfully to Redshift Instance ***")
    except psycopg2.Error as e:
        print(e)
    
    print("\n================ Loading Data into Staging Tables ================\n")
    load_staging_tables(cur, conn)
    
    print("\n================ Inserting Data into Tables ================\n")
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
    