import logging
from datetime import datetime
import calculate.calculate_bs_ratios as bs_ratios
import calculate.calculate_is_ratios as is_ratios
import pandas as pd

import data_handler as MongoDataHandler

logging.basicConfig(filename='logs/calculate_ratios.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


def main():
    logger.info("Company fin ratio calculation script started at : {}".format(datetime.now()))
    data_client = MongoDataHandler.MongoDataHandler()
    calculate_ratios(data_client)


def calculate_ratios(data_client):
    # create a list of latest balance sheets for companies
    balance_sheet_ratios = bs_ratios.calculate(data_client)
    income_statement_ratios = is_ratios.calculate(data_client)
    print(income_statement_ratios.head(5))
    ratios = balance_sheet_ratios.merge(income_statement_ratios, on=['symbol', 'year'])
    pd.set_option('display.max_columns', None)
    print(ratios.head(20))


if __name__ == '__main__':
    main()
