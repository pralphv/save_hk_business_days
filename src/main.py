from dotenv import load_dotenv

load_dotenv()

from typing import List
from datetime import datetime
import json
import logging
import os
import requests

try:
    from . import aastock
    from . import firebase
    from . import hkex

except ImportError:
    import aastock
    import firebase
    import hkex

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s:%(levelname)s: %(message)s")


def send_slack_msg(msg):
    slack_hook = os.environ.get("SLACK_HOOK")
    obj = {'text': msg}
    requests.put(slack_hook, data=json.dumps(obj))


def update_business_days() -> List[int]:
    logging.info(f'Starting update')
    new_dates = aastock.fetch_new_business_days()
    last_update_date = firebase.fetch_last_update()
    new_dates = list(filter(lambda date: date > last_update_date, new_dates))
    for date in new_dates:
        firebase.append_to_business_days(date)
    logging.info(f'Updated {len(new_dates)} dates: {new_dates}')
    return new_dates


def update_stock_details():
    logging.info(f'Starting stock details update')
    obj = hkex.fetch_stock_details_from_hkex()
    firebase.initiate_database(obj)
    logging.info(f'Successfully updated stocks details')


def update_stock_details_scheduler():
    now = datetime.utcnow()
    first_day_of_month = now.day == 1
    if first_day_of_month:
        try:
            update_stock_details()
            send_slack_msg(f'HKEX update successful')
        except Exception as e:
            logging.critical(f'HKEX Update Failed. {e}')
            send_slack_msg(f'HKEX Update Failed. Error: {e}')


def main():
    print('Testing Firebase connection...', firebase.fetch_last_update())
    send_slack_msg('Script has been initiated')
    try:
        new_dates = update_business_days()
        send_slack_msg(f'Successfully updated {new_dates}')
    except Exception as e:
        send_slack_msg(f'Error: {e}')
    update_stock_details_scheduler()


if __name__ == '__main__':
    main()
