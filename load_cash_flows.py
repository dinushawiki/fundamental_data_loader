import logging
from datetime import datetime

from tqdm import tqdm

import company_financial_data
import company_list
import data_handler.MongoDataHandler as MongoDataHandler

logging.basicConfig(filename='logs/load_cash_flows.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


def main():
    now = datetime.now()
    current_year = now.year
    logger.info("Company financial data loading script started at : {}".format(datetime.now()))
    tickers = company_list.get_company_list()
    load_cash_flow(tickers, current_year)


def load_cash_flow(tickers, year):
    data_client = MongoDataHandler.MongoDataHandler()
    filed_companies = data_client.get_cash_filed_companies(year)
    unfiled_companies = (set(tickers) - set(filed_companies))
    logger.info("{} unfiled companies to process".format(len(unfiled_companies)))
    for company in tqdm(unfiled_companies):
        company_financial_data.get_company_cash_flow_data(company, data_client, logger)
    data_client.close_client()


if __name__ == '__main__':
    main()
