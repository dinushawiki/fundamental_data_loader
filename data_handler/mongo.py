from datetime import datetime
from typing import List, Dict

from pymongo import MongoClient

client = MongoClient("localhost", 27017)
db = client['financial_data']


class MongoDataHandler:
    def __init__(self):
        self.client = MongoClient("localhost", 27017)
        self.db = self.client['financial_data']

    # Save and get companies

    def save_company_list(self, company_list: List[str]) -> None:
        companies_db = self.db.companies
        companies_db.insert_many(company_list)

    def save_company_descriptions(self, ticker: str, description_dict: Dict) -> None:
        company_descriptions = self.db.company_descriptions
        company_descriptions.insert_one({'company': ticker, 'data': description_dict})

    def save_all_ticker_prices(self, price_list):
        daily_prices = self.db.daily_prices
        for price in price_list:
            daily_prices.update({'_id': price['_id']}, {"$set": price}, upsert=True)

    def get_ticker_list(self) -> List[str]:
        companies_db = self.db.companies
        companies = companies_db.find({})
        return [company['symbol'] for company in companies]

    # Save and get company balance sheets

    def get_balance_filed_companies(self, year: str) -> List[str]:
        if "balance_sheets" in self.db.list_collection_names():
            balance_sheets = self.db.balance_sheets
            companies = list(balance_sheets.find({"year": year}))
            return [company['company'] for company in companies]
        else:
            return []

    def get_income_filed_companies(self, year: str) -> List[str]:
        if "income_statements" in self.db.list_collection_names():
            income_statements = self.db.income_statements
            companies = list(income_statements.find({"year": year}))
            return [company['company'] for company in companies]
        else:
            return []

    def get_cash_filed_companies(self, year: str) -> List[str]:
        if "cash_flows" in self.db.list_collection_names():
            cash_flows = self.db.cash_flows
            companies = list(cash_flows.find({"year": year}))
            return [company['company'] for company in companies]
        else:
            return []

    def update_balance_sheets(self, ticker: str, balance_sheets: List[Dict]):
        balance_sheet_db = self.db.balance_sheets
        balance_data_dict = {'_id': ticker}
        for balance_sheet in balance_sheets:
            dt = datetime.strptime(balance_sheet['date'], '%Y-%m-%d')
            balance_data_dict[str(dt.year)] = balance_sheet
        balance_sheet_db.update_one({"_id": ticker}, {"$set": balance_data_dict}, upsert=True)

    def update_income_statements(self, ticker: str, income_statements: List[Dict]):
        income_statement_db = self.db.income_statements
        income_statement_dict = {'_id': ticker}
        for income_statement in income_statements:
            dt = datetime.strptime(income_statement['date'], '%Y-%m-%d')
            income_statement_dict[str(dt.year)] = income_statement
        income_statement_db.update_one({"_id": ticker}, {"$set": income_statement_dict}, upsert=True)

    def update_cash_flows(self, ticker: str, cash_flows: List[Dict]):
        cash_flow_db = self.db.cash_flows
        cash_flow_dict = {'_id': ticker}
        for cash_flow in cash_flows:
            dt = datetime.strptime(cash_flow['date'], '%Y-%m-%d')
            cash_flow_dict[str(dt.year)] = cash_flow
        cash_flow_db.update_one({"_id": ticker}, {"$set": cash_flow_dict}, upsert=True)

    def update_company_profiles(self, ticker, profile_dict):
        company_profiles = self.db.company_profiles
        company_profile_dict = {'company': ticker, 'data': profile_dict}
        company_profiles.update_one({"_id": ticker}, {"$set": company_profile_dict}, upsert=True)

    def close_client(self):
        self.client.close()
