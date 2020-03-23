from typing import Dict
import logging
import json
import requests

import constants

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s:%(levelname)s: %(message)s")


def patch(url: str, data: Dict):
    logging.info(f'Patching {data} to {url}')
    data = json.dumps(data)
    resp = requests.patch(url, data=data)
    if resp.status_code >= 400:
        raise RuntimeError(resp.text)
    logging.info(f'Successfully patched {data} to {url}')


def fetch(url: str) -> str:
    logging.info(f'Fetching {url}')
    resp = requests.get(url).json()
    logging.info(f'Successfully fetched {url}')
    return resp


def update_last_update(last_update: int):
    url = f'{constants.FIREBASE_DATABASE_URL}/businessDays.json'
    data = {'lastUpdate': last_update}
    patch(url, data)


def update_last_update_i(last_update_i: int):
    url = f'{constants.FIREBASE_DATABASE_URL}/businessDays.json'
    data = {'lastUpdate_i': last_update_i}
    patch(url, data)


def fetch_last_update() -> int:
    url = f'{constants.FIREBASE_DATABASE_URL}/businessDays/lastUpdate.json'
    resp = fetch(url)
    return int(resp)


def fetch_last_update_i() -> int:
    url = f'{constants.FIREBASE_DATABASE_URL}/businessDays/lastUpdate_i.json'
    resp = fetch(url)
    return int(resp)


def append_to_business_days(date: int):
    url = f'{constants.FIREBASE_DATABASE_URL}/businessDays/data.json'
    i = fetch_last_update_i() + 1
    patch(url, {i: date})
    update_last_update(date)
    update_last_update_i(i)


def fetch_stock_details():
    url = f'{constants.FIREBASE_DATABASE_URL}/stocks.json'
    resp = fetch(url)
    return resp


def initiate_database(data: Dict):
    url = f'{constants.FIREBASE_DATABASE_URL}/stocks.json'
    logging.debug(f'Putting data to {url}')
    start = False
    for key, value in data.items():
        if key == '0405':
            start = True
        if not start:
            continue
        url = f'{constants.FIREBASE_DATABASE_URL}/stocks/{key}.json'
        resp = requests.put(url, json=value)
        if resp.status_code >= 400:
            raise RuntimeError(resp.text)
    logging.debug(f'Successfully put data to {url}')


if __name__ == '__main__':
    import hkex

    obj = hkex.fetch_stock_details_from_hkex()
    initiate_database(obj)
    print(fetch_stock_details())
