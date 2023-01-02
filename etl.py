import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load stage queries"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    print("Staged tables loaded")

def insert_tables(cur, conn):
    """Insert Dim and Star tables"""
    for query in insert_table_queries:
        print(query[:30])
        cur.execute(query)
        conn.commit()
    print("""Print ETL loaded""")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')


    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("ready to load tables")
    # load_staging_tables(cur, conn)
    print("ready to insert tables")
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()