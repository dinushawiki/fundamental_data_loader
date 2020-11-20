from pyfmpcloud import settings
from pyfmpcloud import stock_time_series as sts

api_key = '4b97d96ad599287589126d979919584e'


def get_company_list():
    settings.set_apikey(api_key)
    companies = sts.symbol_list()
    company_list = list(companies['symbol'])
    return company_list
