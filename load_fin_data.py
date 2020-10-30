import logging
import multiprocessing

from pyfmpcloud import settings
from pyfmpcloud import stock_time_series as sts
from tqdm import tqdm

import company_financial_data
import data_handler.MongoDataHandler as MongoDataHandler

logging.basicConfig(filename='logs/myapp.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)

from datetime import datetime

period = 'annual'


def main():
    logger.info("Company financial data loading script started at : {}".format(datetime.now()))
    settings.set_apikey('4b97d96ad599287589126d979919584e')
    companies = sts.symbol_list()
    company_list = list(companies['symbol'])
    income_process = multiprocessing.Process(target=income_statement_process,
                                             args=(0, company_list, period))
    balance_process = multiprocessing.Process(target=balance_sheet_process,
                                              args=(1, company_list, period))
    cash_process = multiprocessing.Process(target=cash_flow_process,
                                           args=(2, company_list, period))
    fin_ratos_process = multiprocessing.Process(target=fin_ratios_process,
                                                args=(3, company_list, period))
    jobs = [income_process, balance_process, cash_process, fin_ratos_process]

    for j in jobs:
        j.start()

    for j in jobs:
        j.join()

    print(datetime.now())


def income_statement_process(id, company_list, time_period):
    data_client = MongoDataHandler.MongoDataHandler()
    for company in tqdm(company_list):
        company_financial_data.get_company_income_statement_data(company, time_period, data_client)
    data_client.close_client()


def balance_sheet_process(id, company_list, time_period):
    data_client = MongoDataHandler.MongoDataHandler()
    for company in tqdm(company_list):
        company_financial_data.get_company_balance_sheet_data(company, time_period, data_client)
    data_client.close_client()


def cash_flow_process(id, company_list, time_period):
    data_client = MongoDataHandler.MongoDataHandler()
    for company in tqdm(company_list):
        company_financial_data.get_company_cash_flow_data(company, time_period, data_client)
    data_client.close_client()


def fin_ratios_process(id, company_list, time_period):
    data_client = MongoDataHandler.MongoDataHandler()
    for company in tqdm(company_list):
        company_financial_data.get_company_ratios(company, time_period, data_client)
    data_client.close_client()


if __name__ == '__main__':
    main()
