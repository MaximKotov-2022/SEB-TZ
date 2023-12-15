import datetime
import os
import time

import psycopg2
import requests
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    format='%(asctime)s, %(levelname)s, %(name)s, %(message)s',
    level=logging.INFO,
    filename='main.log',
    filemode='w',
    encoding='utf-8',
)


ACCESS_KEY = os.getenv('ACCESS_KEY')
INITIAL_URLS = 'http://apilayer.net/api/live?access_key='


conn = psycopg2.connect(
    dbname=os.getenv('DBNAME'),
    user=os.getenv('USER'),
    password=os.getenv('PASSWORD'),
    host=os.getenv('HOST'),
    port=int(os.getenv('PORT')),
    client_encoding="utf8"
)
c = conn.cursor()

# Создание таблицы
c.execute('''CREATE TABLE IF NOT EXISTS currency
             (date text, usd real, eur real)''')


def get_currency():
    """
    Получение данных от API и их запись в БД.

    :return: None
    """
    try:
        response = requests.get(
            f'{ INITIAL_URLS }{ ACCESS_KEY }'
            f'&currencies=RUB&source=USD&format=1')
        data = response.json()
    except KeyError:
        logging.error('Ошибка при получении данных')

    try:
        date_unix = data["timestamp"]
        date = datetime.datetime.fromtimestamp(date_unix)
        usd = data['quotes']['USDRUB']
    except KeyError as e:
        logging.error(f'Ошибка {e}')

    time.sleep(1)  # Для обхода лимита запросов

    try:
        response = requests.get(
            f'{ INITIAL_URLS }{ ACCESS_KEY }'
            f'&currencies=RUB&source=EUR&format=1')
    except KeyError:
        logging.error('Ошибка при получении данных')
    data = response.json()
    eur = data['quotes']['EURRUB']

    # Новая запись в БД
    c.execute("INSERT INTO currency VALUES (%s, %s, %s)", (date, usd, eur))
    conn.commit()
    c.execute("SELECT * FROM currency")


def run_pipeline():
    """
    Проверка текущего времени.

    :return: None
    """
    while True:
        now = datetime.datetime.now()
        if now.hour == 13 and now.minute == 49:
            get_currency()
            print('Время')
            time.sleep(60)
        else:
            time.sleep(60)


run_pipeline()
c.close()
conn.close()
