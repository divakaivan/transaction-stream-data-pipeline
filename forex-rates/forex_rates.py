import os
import requests
import schedule
import psycopg2
from datetime import datetime
import time

def get_forex_rates():
    date = datetime.now().strftime("%Y-%m-%d")
    api_url = f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{date}/v1/currencies/gbp.json"
    response = requests.get(api_url)
    data = response.json()
    # top currencies only
    currencies = ["usd", "eur", "jpy", "cad", "aud", "chf", "cny", "sek", "nzd", "mxn"]
    top_currency_rates = {'date': data.get('date'), **{currency: data.get('gbp')[currency] for currency in currencies}}
    return top_currency_rates

def insert_into_db(data):
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )
    cursor = conn.cursor()
    # doing it here so I do not lose the already loaded data into postgres
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS forex_rates (
            date DATE PRIMARY KEY,
            usd FLOAT,
            eur FLOAT,
            jpy FLOAT,
            cad FLOAT,
            aud FLOAT,
            chf FLOAT,
            cny FLOAT,
            sek FLOAT,
            nzd FLOAT,
            mxn FLOAT
        )
    """)

    cursor.execute("SELECT date FROM forex_rates WHERE date = %s", (data['date'],))
    if cursor.fetchone() is None:
        cursor.execute("""
            INSERT INTO forex_rates (date, usd, eur, jpy, cad, aud, chf, cny, sek, nzd, mxn)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data['date'],
            data['usd'],
            data['eur'],
            data['jpy'],
            data['cad'],
            data['aud'],
            data['chf'],
            data['cny'],
            data['sek'],
            data['nzd'],
            data['mxn']
        ))

    conn.commit()
    cursor.close()
    conn.close()

def job():
    data = get_forex_rates()
    insert_into_db(data)
    print('New forex rates fetched and inserted into the database.')

if __name__ == "__main__":
    job()
    schedule.every(24).hours.do(job)
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(1) 
    except KeyboardInterrupt:
        print("Stopping...")
