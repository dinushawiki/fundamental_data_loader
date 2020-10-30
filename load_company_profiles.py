import logging
from datetime import datetime


import requests
from tqdm import tqdm

import data_handler.MongoDataHandler as MongoDataHandler

requests.packages.urllib3.disable_warnings()
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += ':HIGH:!DH:!aNULL'

try:
    requests.packages.urllib3.contrib.pyopenssl.util.ssl_.DEFAULT_CIPHERS += ':HIGH:!DH:!aNULL'
except AttributeError:
    # no pyopenssl support used / needed / available
    pass

from pyfmpcloud import settings
from pyfmpcloud import stock_time_series as sts

logging.basicConfig(filename='logs/company_profile.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)

api_key = 'de010557d283d47912805d7666fed46e'


def main():
    logger.info("Company profile loading script started at : {}".format(datetime.now()))
    data_client = MongoDataHandler.MongoDataHandler()
    settings.set_apikey('4b97d96ad599287589126d979919584e')
    companies = sts.symbol_list()
    company_list = list(companies['symbol'])
    num_loaded = 0
    for company in tqdm(company_list):
        try:
            url = 'https://datafied.api.edgar-online.com/v2/companies?filter=primarysymbol%20eq%20%22{}%22&appkey={}'.format(
                company, api_key)

            page = requests.get(url, verify=False)
            result = page.json()
            items = result['result']['rows'][0]['values']
            company_profile = {}
            for item in items:
                company_profile[item['field']] = item['value']
            data_client.save_company_profiles(company, company_profile)
            num_loaded = num_loaded + 1
        except Exception as err:
            logger.info("Error in getting company profile for: {}. Error details: {}".format(company, err))
    logger.info(("Company profile loading script completed at : {}".format(datetime.now())))
    logger.info("Number of companies loaded: {}".format(num_loaded))


if __name__ == '__main__':
    main()
