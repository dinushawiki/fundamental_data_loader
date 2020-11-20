import logging
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from time import time

import yfinance as yf

import company_list
import data_handler.MongoDataHandler as MongoDataHandler

logging.basicConfig(filename='logs/load_company_descriptions.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


def main():
    ts = time()
    logger.info("Company description loading script started at : {}".format(ts))
    data_client = MongoDataHandler.MongoDataHandler()
    tickers = company_list.get_company_list()

    company_des = partial(get_descriptions, data_client)
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(company_des, tickers)
    logging.info('Took %s', time() - ts)


def get_descriptions(data_client, ticker):
    try:
        ticker = yf.Ticker(ticker)
        company_info = ticker.get_info()
        company_profile = {'sector': company_info['sector'],
                           'longBusinessSummary': company_info.get('longBusinessSummary'),
                           'country': company_info.get('country'), 'industry': company_info.get('industry')}
        data_client.save_company_descriptions(ticker, company_profile)
    except Exception as err:
        logger.info("description: {}. Error: {}".format(ticker, err))


if __name__ == '__main__':
    main()
