import logging
import os

from data_handler.mongo import MongoDataHandler
from fmp import financial_data

logging.basicConfig(filename='{}/logs/load_all_companies.log'.format(os.path.dirname(os.getcwd())),
                    level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


def main():
    try:
        logger.info("Loading company list started")
        # get all companies from fmp
        full_company_list = financial_data.get_companies()
        data_client = MongoDataHandler()
        data_client.save_company_list(full_company_list)
        logger.info("Successfully loaded {} the companies".format(len(full_company_list)))
        data_client.close_client()
    except Exception as err:
        logger.info("Loading company list failed: Error: {}".format(err))


if __name__ == '__main__':
    main()
