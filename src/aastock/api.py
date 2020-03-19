from datetime import datetime
from typing import List
import logging

import requests

try:
    from . import constants
except ImportError:
    import constants

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s:%(levelname)s: %(message)s")


def fetch_aastock() -> str:
    logging.info(f'Fetching AAStock data from {constants.AASTOCK_URL}')
    resp = requests.get(constants.AASTOCK_URL)
    logging.info(f'Successfully fetched AAStock data')
    return resp.content.decode('utf-8')


def extract_dates(content: str) -> List[str]:
    splitted = content.split('|')
    further_split = list(map(lambda x: x.split(';'), splitted))
    data = list(filter(lambda x: len(x) == 7, further_split))
    dates = list(map(lambda x: x[0], data))
    return dates


def format_dates(dates: List[str]) -> List[int]:
    datetime_ = map(lambda date: datetime.strptime(date[-10:], '%m/%d/%Y'), dates)  # has random leading !
    dates = map(lambda date: int(datetime.strftime(date, '%Y%m%d')), datetime_)
    return list(dates)


def fetch_new_business_days() -> List[int]:
    logging.info(f'Fetching new business days')
    content = fetch_aastock()
    dates = extract_dates(content)
    dates = format_dates(dates)
    logging.info(f'Successfully fetched new business days')
    return dates
