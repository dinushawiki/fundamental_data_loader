import logging
import os
from datetime import datetime

from tqdm import tqdm

from data_handler.mongo import MongoDataHandler
from fmp import financial_data

logging.basicConfig(filename='{}/logs/load_balance_sheets.log'.format(os.path.dirname(os.getcwd())),
                    level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


def main():
    now = datetime.now()
    current_year = now.year
    logger.info("Company balance sheet loading script started.")
    data_client = MongoDataHandler()
    tickers = data_client.get_ticker_list()
    balance_filed_companies = data_client.get_balance_filed_companies(str(current_year))
    balance_unfiled_companies = (set(tickers) - set(balance_filed_companies))
    logger.info("{} unfiled companies to process".format(len(balance_unfiled_companies)))
    [get_and_load_balance_sheets(company, data_client) for company in tqdm(balance_unfiled_companies)]
    data_client.close_client()


def get_and_load_balance_sheets(ticker: str, data_client: MongoDataHandler) -> None:
    try:
        balance_sheets = financial_data.get_balance_sheets(ticker)
        if balance_sheets:
            data_client.update_balance_sheets(ticker, balance_sheets)
        else:
            logger.info("No balance sheets found for: {}.".format(ticker))
    except Exception as err:
        logger.info("Loading balance sheets failed for: {} Error: {}".format(ticker, err))


if __name__ == '__main__':
    main()
