from typing import Dict
import logging

import pandas as pd

try:
    from . import constants
except ImportError:
    import constants

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s:%(levelname)s: %(message)s")


def fetch_from_hkex(url):
    logging.info(f'Fetching HKEX data from {url}')
    df = pd.read_excel(url, skiprows=[0, 1])
    logging.info(f'Successfully fetched HKEX data')
    return df


def filter_valid_rows(df):
    df = df.loc[
        (df['Category'] == 'Equity')
        | (df['Category'] == 'Exchange Traded Products')
        | (df['Category'] == 'Real Estate Investment Trusts')
        ]
    return df


def convert_stock_code_to_yahoo_format(element):
    if len(element) == 0:
        return
    elif len(element) == 1:
        element = '000' + element
    elif len(element) == 2:
        element = '00' + element
    elif len(element) == 3:
        element = '0' + element
    element = element
    return element


def convert_df_to_custom_dict(df):
    obj = {}
    # ['Stock Code', Name of Securities' 'Category' 'Sub-Category' 'Board Lot' 'Name of Securitiescn']
    for row in df.itertuples():
        current_obj = {
            row[0]: {
                'name': {
                    'en': row[1],
                    'cn': row[5]
                },
                'category': row[2],
                'lot': row[4]
            }
        }
        if not pd.isnull(row[3]):
            current_obj[row[0]]['subCategory'] = row[3]
        obj = {**obj, **current_obj}
    return obj


def fetch_stock_details_from_hkex() -> Dict:
    df = fetch_from_hkex(constants.URL_EN)
    cn_df = fetch_from_hkex(constants.URL_ZH)

    df = df[['Stock Code', 'Name of Securities', 'Category', 'Sub-Category', 'Board Lot']]
    cn_df = cn_df[['股份代號', '股份名稱']]

    cn_df = cn_df.rename(columns={'股份代號': 'Stock Code', '股份名稱': 'Name of Securities'})
    df = filter_valid_rows(df)

    df['Stock Code'] = df['Stock Code'].astype(str).apply(convert_stock_code_to_yahoo_format)
    cn_df['Stock Code'] = cn_df['Stock Code'].astype(str).apply(convert_stock_code_to_yahoo_format)

    df['Board Lot'] = df['Board Lot'].astype(str).str.replace(',', '').astype(int)

    index_col = 'Stock Code'
    df = df.set_index(index_col).join(cn_df.set_index(index_col), rsuffix="cn")
    stock_details_obj = convert_df_to_custom_dict(df)
    return stock_details_obj


def main():
    stock_details_obj = fetch_stock_details_from_hkex()
    print(stock_details_obj)


if __name__ == '__main__':
    main()
