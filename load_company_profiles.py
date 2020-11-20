import logging
from datetime import datetime

import requests
from pyfmpcloud import settings
from pyfmpcloud import stock_time_series as sts
from tqdm import tqdm
import company_list
import data_handler.MongoDataHandler as MongoDataHandler

logging.basicConfig(filename='logs/load_company_profiles.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)

api_key = '4b97d96ad599287589126d979919584e'
url = 'https://fmpcloud.io/api/v3/profile/{}?apikey={}'


def main():
    logger.info("Company profile loading script started at : {}".format(datetime.now()))
    tickers = company_list.get_company_list()
    data_client = MongoDataHandler.MongoDataHandler()
    for ticker in tqdm(tickers):
        get_company_profile(ticker, data_client)
    data_client.close_client()


def get_company_profile(ticker, client):
    try:
        profile = requests.get(url.format(ticker, api_key))
        profile_dict = profile.json()[0]
        if profile_dict:
            client.save_companyprofiles(ticker, profile_dict)
        else:
            logger.info("profile: {}".format(ticker))
    except Exception as err:
        logger.info("profile: {}. Error: {}".format(ticker, err))


if __name__ == '__main__':
    main()
