import requests
import logging
from datetime import datetime
import data_handler.MongoDataHandler as MongoDataHandler


logging.basicConfig(filename='logs/load_company_list.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)

api_key = '4b97d96ad599287589126d979919584e'
url = 'https://fmpcloud.io/api/v3/company/stock/list?apikey={}'


def main():
    try:
        logger.info("Loading company list: {}".format(datetime.now()))
        data_client = MongoDataHandler.MongoDataHandler()
        company_list = requests.get(url.format(api_key))
        companies = company_list.json()['symbolsList']
        data_client.save_company_list(companies)
    except Exception as err:
        logger.info("Can't load company list: Error: {}".format(err))


if __name__ == '__main__':
    main()