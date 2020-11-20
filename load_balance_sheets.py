import logging
from datetime import datetime

from tqdm import tqdm

import company_financial_data
import company_list
import data_handler.MongoDataHandler as MongoDataHandler

logging.basicConfig(filename='logs/load_balance_sheets.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


def main():
    now = datetime.now()
    current_year = now.year
    logger.info("Company financial data loading script started at : {}".format(datetime.now()))
    tickers = company_list.get_company_list()
    load_balance_sheet(tickers, current_year)


def load_balance_sheet(tickers, year):
    data_client = MongoDataHandler.MongoDataHandler()
    filed_companies = data_client.get_balance_filed_companies(year)
    unfiled_companies = (set(tickers) - set(filed_companies))
    logger.info("{} unfiled companies to process".format(len(unfiled_companies)))
    for company in tqdm(unfiled_companies):
        company_financial_data.get_company_balance_sheet_data(company, data_client, logger)
    data_client.close_client()


if __name__ == '__main__':
    main()
