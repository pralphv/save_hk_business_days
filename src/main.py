from typing import List
from datetime import datetime
import json
import logging
import os
import requests
import time

from dotenv import load_dotenv

try:
    from . import constants
    from . import aastock
    from . import firebase
except ImportError:
    import constants
    import aastock
    import firebase

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s:%(levelname)s: %(message)s")


def update_business_days() -> List[int]:
    logging.info(f'Starting update')
    new_dates = aastock.fetch_new_business_days()
    last_update_date = firebase.fetch_last_update()
    new_dates = list(filter(lambda date: date > last_update_date, new_dates))
    for date in new_dates:
        firebase.append_to_business_days(date)
    logging.info(f'Updated {len(new_dates)} dates: {new_dates}')
    return new_dates


def send_slack_msg(msg):
    slack_hook = os.environ.get("SLACK_HOOK")
    obj = {'text': msg}
    requests.put(slack_hook, data=json.dumps(obj))


def main():
    load_dotenv()
    send_slack_msg('Script has been initiated')
    while True:
        now = datetime.utcnow()
        is_correct_time = now.hour == 8 and now.minute == 31  # HKT 4:30pm
        is_weekday = now.weekday() < 5
        if is_correct_time and is_weekday:
            try:
                updated = update_business_days()
                send_slack_msg(f'Successfully updated {updated}')
                time.sleep(60)  # to prevent running twice
            except Exception as e:
                logging.critical(f'Update Failed. {e}')
                send_slack_msg(f'Error: {e}')
        logging.debug('Sleeping')
        time.sleep(30)


if __name__ == '__main__':
    main()
