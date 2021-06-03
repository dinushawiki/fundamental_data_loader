from typing import List, Dict

import requests

from . import properties

api_key = properties.api_key


def get_companies() -> List[str]:
    companies = requests.get(properties.company_list_url.format(api_key))
    company_list = companies.json()['symbolsList']
    return company_list


def get_balance_sheets(ticker: str) -> List[Dict]:
    balance_sheets = requests.get(properties.balance_sheet_url.format(ticker, api_key))
    return balance_sheets.json()


def get_income_statements(ticker: str) -> List[Dict]:
    income_statements = requests.get(properties.income_statement_url.format(ticker, api_key))
    return income_statements.json()


def get_cash_flows(ticker: str) -> List[Dict]:
    cash_flows = requests.get(properties.cash_flow_url.format(ticker, api_key))
    return cash_flows.json()


def get_company_profiles(ticker: str) -> Dict:
    company_profile = requests.get(properties.company_profile_url.format(ticker, api_key))
    return company_profile.json()[0]
