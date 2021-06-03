import logging

import pandas as pd

import calculate.get_latest_ticker_prices as prices

logging.basicConfig(filename='logs/calculate_ratios.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


def calculate(data_client):
    df_ticker_prices = prices.get_prices(data_client)
    all_income_statements = data_client.get_income_statements()
    latest_income = []
    for ticker_income_statements in all_income_statements:
        ticker = ticker_income_statements['_id']
        try:
            latest_income_entry_key = sorted(ticker_income_statements.keys())[-2]
            latest_income.append(ticker_income_statements[latest_income_entry_key])
        except Exception as err:
            logger.info("latest entry missing for {}. Error: {}".format(ticker, err))

    df_latest_income = pd.DataFrame(latest_income)
    df_latest = df_latest_income.merge(df_ticker_prices, on=['symbol'])
    df_latest['year'] = pd.DatetimeIndex(df_latest['date']).year
    df_latest['gross_margin'] = (df_latest['grossProfit'] / df_latest['revenue'])
    df_latest['profit_margin'] = (df_latest['netIncome'] / df_latest['revenue'])
    df_latest['operating_margin'] = (df_latest['operatingIncome'] / df_latest['revenue'])

    df_latest['pe_ratio'] = (df_latest['operatingIncome'] / df_latest['revenue'])

    columns_to_send = ['symbol', 'year', 'gross_margin', 'profit_margin', 'operating_margin', 'eps', 'epsdiluted',
                       'price', 'trade_date']

    return df_latest[columns_to_send]
