import logging

import requests

requests.packages.urllib3.disable_warnings()
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += ':HIGH:!DH:!aNULL'

try:
    requests.packages.urllib3.contrib.pyopenssl.util.ssl_.DEFAULT_CIPHERS += ':HIGH:!DH:!aNULL'
except AttributeError:
    # no pyopenssl support used / needed / available
    pass

from pyfmpcloud import settings
from pyfmpcloud import stock_time_series as sts

logging.basicConfig(filename='logs/myapp.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)

api_key = 'de010557d283d47912805d7666fed46e'


def load_company_profiles():
    settings.set_apikey('4b97d96ad599287589126d979919584e')
    companies = sts.symbol_list()
    company_list = list(companies['symbol'])
    for company in company_list:
        try:
            url = 'https://datafied.api.edgar-online.com/v2/companies?filter=primarysymbol%20eq%20%22{}%22&appkey={}'.format(
                company, api_key)

            page = requests.get(url, verify=False)
            result = page.json()
            items = result['result']['rows'][0]['values']
            print(items)
        except Exception as err:
            print(company)


load_company_profiles()
