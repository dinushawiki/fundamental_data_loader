import logging
import os
from datetime import datetime

from tqdm import tqdm

from data_handler.mongo import MongoDataHandler
from fmp import financial_data

logging.basicConfig(filename='{}/logs/load_company_profiles.log'.format(os.path.dirname(os.getcwd())),
                    level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


def main():
    logger.info("Company profile loading script started at : {}".format(datetime.now()))
    data_client = MongoDataHandler()
    tickers = data_client.get_ticker_list()
    for ticker in tqdm(tickers):
        get_company_profile(ticker, data_client)
    data_client.close_client()


def get_company_profile(ticker: str, data_client: MongoDataHandler) -> None:
    try:
        company_profile = financial_data.get_company_profiles(ticker)
        if company_profile:
            data_client.update_company_profiles(ticker, company_profile)
        else:
            logger.info("No profile found for : {}".format(ticker))
    except Exception as err:
        logger.info("Loading profile failed for: {} Error: {}".format(ticker, err))


if __name__ == '__main__':
    main()
