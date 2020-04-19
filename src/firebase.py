from typing import Dict, Union
import json
import logging
import os

import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s:%(levelname)s: %(message)s")

SECRET_KEY = os.environ.get('SECRET_KEY')

if SECRET_KEY is None:
    with open('../hkportfolioanalysis-dev-firebase-adminsdk-ivty2-235716019a.json') as f:
        SECRET_KEY = json.load(f)
else:
    SECRET_KEY = json.loads(SECRET_KEY)

CRED = credentials.Certificate(SECRET_KEY)

firebase_admin.initialize_app(CRED, {
    'databaseURL': r'https://hkportfolioanalysis-dev.firebaseio.com'
})


def patch(url: str, data: Dict):
    logging.info(f'Patching {data} to {url}')
    ref = db.reference(url)
    ref.update(data)
    logging.info(f'Successfully patched {data} to {url}')


def fetch(url: str) -> Union[str, int, float]:
    logging.info(f'Fetching {url}')
    ref = db.reference(url)
    resp = ref.get()
    logging.info(f'Successfully fetched {url}: {resp}')
    return resp


def update_last_update(last_update: int):
    url = r'businessDays'
    data = {'lastUpdate': last_update}
    patch(url, data)


def update_last_update_i(last_update_i: int):
    url = r'businessDays'
    data = {'lastUpdate_i': last_update_i}
    patch(url, data)


def fetch_last_update() -> int:
    resp = fetch('businessDays/lastUpdate')
    return int(resp)


def fetch_last_update_i() -> int:
    resp = fetch('businessDays/lastUpdate_i')
    return int(resp)


def append_to_business_days(date: int):
    url = 'businessDays/data.json'
    i = fetch_last_update_i() + 1
    patch(url, {i: date})
    update_last_update(date)
    update_last_update_i(i)


def fetch_stock_details():
    resp = fetch('stocks')
    return resp


def initiate_database(data: Dict):
    logging.debug(f'Putting data to /stocks')
    start = False
    for key, value in data.items():
        if key == '0405':
            start = True
        if not start:
            continue
        url = f'stocks/{key}'
        ref = db.reference(url)
        ref.set(value)
    logging.debug(f'Successfully put data to {url}')


if __name__ == '__main__':
    print(fetch_last_update())
