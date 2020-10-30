import logging
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from time import time

import yfinance as yf
from pyfmpcloud import settings
from pyfmpcloud import stock_time_series as sts

import data_handler.MongoDataHandler as MongoDataHandler

logging.basicConfig(filename='logs/company_descriptions.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


def main():
    ts = time()
    logger.info("Company description loading script started at : {}".format(ts))
    data_client = MongoDataHandler.MongoDataHandler()
    settings.set_apikey('4b97d96ad599287589126d979919584e')
    companies = sts.symbol_list()
    company_list = list(companies['symbol'])

    company_des = partial(get_descriptions, data_client)
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(company_des, company_list)
    logging.info('Took %s', time() - ts)


def get_descriptions(data_client, company):
    try:
        ticker = yf.Ticker(company)
        company_info = ticker.get_info()
        company_profile = {'sector': company_info['sector'],
                           'longBusinessSummary': company_info.get('longBusinessSummary'),
                           'country': company_info.get('country'), 'industry': company_info.get('industry')}
        data_client.save_company_descriptions(company, company_profile)
    except Exception as err:
        logger.info("Error in getting company description for: {}. Error details: {}".format(company, err))


if __name__ == '__main__':
    main()

