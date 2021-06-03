import logging
import os
from datetime import datetime

from tqdm import tqdm

import sys
sys.path.append('..')

from data_handler.mongo import MongoDataHandler
from fmp import financial_data

logging.basicConfig(filename='{}/logs/load_cash_flows.log'.format(os.path.dirname(os.getcwd())),
                    level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


def main():
    now = datetime.now()
    current_year = now.year
    logger.info("Company cash flow loading script started.")
    data_client = MongoDataHandler()
    tickers = data_client.get_ticker_list()
    cash_filed_companies = data_client.get_cash_filed_companies(str(current_year))
    cash_unfiled_companies = (set(tickers) - set(cash_filed_companies))
    logger.info("{} unfiled companies to process".format(len(cash_unfiled_companies)))
    [get_and_load_cash_flows(company, data_client) for company in tqdm(cash_unfiled_companies)]
    data_client.close_client()


def get_and_load_cash_flows(company: str, data_client: MongoDataHandler) -> None:
    try:
        cash_flows = financial_data.get_cash_flows(company)
        if cash_flows:
            data_client.update_cash_flows(company, cash_flows)
        else:
            logger.info("No cash flows found for: {}.".format(company))
    except Exception as err:
        logger.info("Loading cash flows failed for: {}. Error: {}".format(company, err))


if __name__ == '__main__':
    main()
