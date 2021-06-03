import logging
import os
from datetime import datetime

from tqdm import tqdm

from data_handler.mongo import MongoDataHandler
from fmp import financial_data

logging.basicConfig(filename='{}/logs/load_income_statements.log'.format(os.path.dirname(os.getcwd())),
                    level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


def main():
    now = datetime.now()
    current_year = now.year
    logger.info("Company income statement loading script started.")
    data_client = MongoDataHandler()
    tickers = data_client.get_ticker_list()
    income_filed_companies = data_client.get_income_filed_companies(str(current_year))
    income_unfiled_companies = (set(tickers) - set(income_filed_companies))
    logger.info("{} unfiled companies to process".format(len(income_unfiled_companies)))
    [get_and_load_income_statements(company, data_client) for company in tqdm(income_unfiled_companies)]
    data_client.close_client()


def get_and_load_income_statements(company: str, data_client: MongoDataHandler) -> None:
    try:
        income_statements = financial_data.get_income_statements(company)
        if income_statements:
            data_client.update_income_statements(company, income_statements)
        logger.info("No income statements found for: {}.".format(company))
    except Exception as err:
        logger.info("Loading income statements failed for: {} Error: {}".format(company, err))


if __name__ == '__main__':
    main()
