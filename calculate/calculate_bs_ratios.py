import logging

import pandas as pd

logging.basicConfig(filename='logs/calculate_ratios.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


def calculate(data_client):
    all_balance_sheets = data_client.get_balance_sheets()
    latest = []
    for ticker_balance_sheets in all_balance_sheets:
        ticker = ticker_balance_sheets['_id']
        try:
            latest_entry_key = sorted(ticker_balance_sheets.keys())[-2]
            latest.append(ticker_balance_sheets[latest_entry_key])
        except Exception as err:
            logger.info("latest entry missing for {}. Error: {}".format(ticker, err))

    df_latest = pd.DataFrame(latest)
    df_latest['year'] = pd.DatetimeIndex(df_latest['date']).year
    df_latest['quick_ratio'] = (df_latest['cashAndShortTermInvestments'] + df_latest['netReceivables']) / df_latest[
        'totalCurrentLiabilities']
    df_latest['current_ratio'] = df_latest['totalCurrentAssets'] / df_latest['totalCurrentLiabilities']
    df_latest['working_capital'] = df_latest['totalCurrentAssets'] - df_latest['totalCurrentLiabilities']
    df_latest['debt_to_equity'] = df_latest['totalLiabilities'] / df_latest['totalStockholdersEquity']

    columns_to_send = ['symbol', 'year', 'quick_ratio', 'current_ratio', 'working_capital', 'debt_to_equity']

    return df_latest[columns_to_send]
