from postgresql import config
import psycopg2
import psycopg2.extras
import pandas as pd

def get(sql_string):
    conn = None
    try:
        params = config()
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql_string)
        db_version = cur.fetchall()
        cur.close()
    except (Exception,psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
    return db_version

def set(sql_string):
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql_string)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def set_batch(sql_string,dataFrame):
    params = config()
    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    psycopg2.extras.execute_batch(cur,sql_string,dataFrame)
    conn.commit()
    cur.close()