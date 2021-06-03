import logging
import os

import yfinance as yf
from tqdm import tqdm

import data_handler as MongoDataHandler
from data_handler.mongo import MongoDataHandler

logging.basicConfig(filename='{}/logs/load_company_descriptions.log'.format(os.path.dirname(os.getcwd())),
                    level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


def main():
    logger.info("Company description loading script started")
    data_client = MongoDataHandler()
    tickers = data_client.get_ticker_list()
    [get_and_load_company_descriptions(ticker, data_client) for ticker in tqdm(tickers)]
    data_client.close_client()


def get_and_load_company_descriptions(ticker: str, data_client: MongoDataHandler) -> None:
    try:
        ticker = yf.Ticker(ticker)
        company_info = ticker.get_info()
        company_profile = {'sector': company_info['sector'],
                           'longBusinessSummary': company_info.get('longBusinessSummary'),
                           'country': company_info.get('country'), 'industry': company_info.get('industry')}
        data_client.save_company_descriptions(ticker, company_profile)
    except Exception as err:
        logger.info("Loading descriptions failed for: {} Error: {}".format(ticker, err))


if __name__ == '__main__':
    main()

#     company_des = partial(get_descriptions, data_client)
#     with ThreadPoolExecutor(max_workers=2) as executor:
#         executor.map(company_des, tickers)
#
#
#
# def get_descriptions(data_client, ticker):
#     try:
#         ticker = yf.Ticker(ticker)
#         company_info = ticker.get_info()
#         company_profile = {'sector': company_info['sector'],
#                            'longBusinessSummary': company_info.get('longBusinessSummary'),
#                            'country': company_info.get('country'), 'industry': company_info.get('industry')}
#         data_client.save_company_descriptions(ticker, company_profile)
#     except Exception as err:
#         logger.info("description: {}. Error: {}".format(ticker, err))
