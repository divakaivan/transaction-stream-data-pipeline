import streamlit as st
import time
import psycopg2
from psycopg2 import OperationalError
import logging

# configure logging
logging.basicConfig(level=logging.INFO)

# define the connection params
DB_HOST = "postgres"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "password"
DB_PORT =  5432

def fetch_data():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM sentences")
        rows = cursor.fetchall()
        conn.close()
        return rows
    except OperationalError as e:
        st.error(f'Operation error: {e}')
        return []
    except Exception as e:
        st.error(f'Error: {e}')
        return []
    
def main():
    st.title('Sentence Data dashboard')
    st.write('This is a simple dashboard for sentiment analysis:')
    unique_id = set()
    while True:
        data = fetch_data()
        if data:
            for row in data:
                if row[0] not in unique_id:
                    st.write(f"ID: {row[0]} - Sentence: {row[1]} - Sentiment: {row[2]}")
                    unique_id.add(row[0])
        time.sleep(3)

if __name__ == '__main__':
    main()