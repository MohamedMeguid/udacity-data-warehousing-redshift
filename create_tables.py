import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drops all tables from Redshift for a clean start.
    
    Parameters
    ----------
    cur: `psycopg2.extensions.cursor`
        Postgres database cursor.
    conn: `psycopg2.extensions.connection`
        Postgres database connection handle.
    """
    
    for title, query in drop_table_queries.items():
        try:
            print("Executing query: ", title)
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error executing query: ", title)
            print(e)


def create_tables(cur, conn):
    """Creates tables in the Redshift cluster.
    
    Parameters
    ----------
    cur: `psycopg2.extensions.cursor`
        Postgres database cursor.
    conn: `psycopg2.extensions.connection`
        Postgres database connection handle.
    """
    
    for title, query in create_table_queries.items():
        try:
            print("Executing query: ", title)
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error executing query: ", title)
            print(e)



def main():
    """Main function to run, creates connection with the Redshift cluster and 
    calls functions that creates tables appropriately the data warehouse
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

    print("\n================ Dropping Tables ================\n")
    drop_tables(cur, conn)

    print("\n================ Creating Tables ================\n")
    create_tables(cur, conn)

    conn.close()
    


if __name__ == "__main__":
    main()
    