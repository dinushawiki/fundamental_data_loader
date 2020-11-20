import requests

api_key = '4b97d96ad599287589126d979919584e'


def get_company_balance_sheet_data(ticker, client, logger):
    try:
        url = 'https://fmpcloud.io/api/v3/balance-sheet-statement/{}?apikey={}'
        balance_sheet = requests.get(url.format(ticker, api_key))
        client.save_balance_sheet_data(ticker, balance_sheet.json())
    except Exception as err:
        logger.info("balance: {}. Error: {}".format(ticker, err))


def get_company_income_statement_data(ticker, client, logger):
    try:
        url = 'https://fmpcloud.io/api/v3/income-statement/{}?apikey={}'
        income_statement = requests.get(url.format(ticker, api_key))
        client.save_income_statement_data(ticker, income_statement.json())
    except Exception as err:
        logger.info("income: {}. Error: {}".format(ticker, err))


def get_company_cash_flow_data(ticker, client, logger):
    try:
        url = 'https://fmpcloud.io/api/v3/cash-flow-statement/{}?apikey={}'
        cash_flow = requests.get(url.format(ticker, api_key))
        client.save_cash_flow_data(ticker, cash_flow.json())
    except Exception as err:
        logger.info("cash: {}. Error: {}".format(ticker, err))

# if __name__ == '__main__':
# from datetime import datetime
#     api_key = '4b97d96ad599287589126d979919584e'
#     url = 'https://fmpcloud.io/api/v3/income-statement/{}?apikey={}'
#     income = requests.get(url.format("BAC", api_key))
#     income_statement_sets = income.json()
#     print(income_statement_sets)
#     for income_data in income_statement_sets:
#         income_data_dict = {}
#         dt = datetime.strptime(income_data['date'], '%Y-%m-%d')
#         year = dt.year
#         income_data_dict['company'] = "BAC"
#         income_data_dict['year'] = year
#         income_data_dict['data'] = income_data
#         print(income_data_dict)
