import sys
import os
import requests
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import psycopg2
from src.constants import DB_FIELDS

# Database connection parameters
dbname = "postgres"
user = "postgres"
password = os.getenv("POSTGRES_PASSWORD")
host = "localhost"

# Connect to the database
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
cur = conn.cursor()


def try_execute_sql(sql: str):
    try:
        cur.execute(sql)
        conn.commit()
        print(f"Executed table creation successfully")
    except Exception as e:
        print(f"Couldn't execute table creation due to exception: {e}")
        conn.rollback()


def create_table(DB_FIELDS):

    for key in DB_FIELDS:
        create_table_sql = f"""
        CREATE TABLE 
        """
        table_sql = f"{key} ( \n"
        create_table_sql += table_sql

        fields = DB_FIELDS.get(key, [])
        
        for index, field in enumerate(fields):
            if index == len(fields) - 1:
                column_sql = f"{field} text \n"

            else:
                column_sql = f"{field} text, \n"  
          
            create_table_sql += column_sql

        create_table_sql += ");"
        #print(create_table_sql)
        try_execute_sql(create_table_sql)
        #print(create_table_sql)

    cur.close()
    conn.close()


if __name__ == "__main__":
    create_table(DB_FIELDS)
