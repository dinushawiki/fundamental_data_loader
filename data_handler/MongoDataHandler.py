from datetime import datetime

from pymongo import MongoClient


class MongoDataHandler:
    def __init__(self):
        self.client = MongoClient("localhost", 27017)
        self.db = self.client['financial_data']

    def get_ticker_list(self):
        companies = self.db.companies
        all_companies = companies.find({})
        tickers = []
        for company in all_companies:
            tickers.append(company['symbol'])
        return tickers

    def get_income_filed_companies(self, year):
        income_statement = self.db.income_statement
        companies = list(income_statement.find({"year": year}))
        return [company['company'] for company in companies]

    def get_balance_filed_companies(self, year):
        balance_sheet = self.db.balance_sheet
        companies = list(balance_sheet.find({"year": year}))
        return [company['company'] for company in companies]

    def get_cash_filed_companies(self, year):
        cash_flow = self.db.cash_flow
        companies = list(cash_flow.find({"year": year}))
        return [company['company'] for company in companies]

    def get_balance_sheets(self):
        balance_sheet = self.db.balance_sheet
        return balance_sheet.find({})

    def get_income_statements(self):
        income_statement = self.db.income_statement
        return income_statement.find({})

    def save_balance_sheet_data(self, ticker, balance_data_sets):
        if balance_data_sets:
            balance_sheet = self.db.balance_sheet
            balance_data_dict = {'_id': ticker}
            for balance_data in balance_data_sets:
                dt = datetime.strptime(balance_data['date'], '%Y-%m-%d')
                balance_data_dict[str(dt.year)] = balance_data
            balance_sheet.insert_one(balance_data_dict)

    def save_income_statement_data(self, ticker, income_statement_sets):
        if income_statement_sets:
            income_statement = self.db.income_statement
            income_data_dict = {'_id': ticker}
            for income_data in income_statement_sets:
                dt = datetime.strptime(income_data['date'], '%Y-%m-%d')
                income_data_dict[str(dt.year)] = income_data
            income_statement.insert_one(income_data_dict)

    def save_cash_flow_data(self, ticker, cash_flow_sets):
        if cash_flow_sets:
            cash_flow = self.db.cash_flow
            cash_flow_dict = {'_id': ticker}
            for cash_flow_data in cash_flow_sets:
                dt = datetime.strptime(cash_flow_data['date'], '%Y-%m-%d')
                cash_flow_dict[str(dt.year)] = cash_flow_data
            cash_flow.insert_one(cash_flow_dict)

    def save_company_profiles(self, ticker, profile_dict):
        company_profiles = self.db.company_profiles
        company_profile_dict = {'company': ticker, 'data': profile_dict}
        company_profiles.insert_one(company_profile_dict)

    def save_company_list(self, company_list):
        companies = self.db.companies
        companies.insert_many(company_list)

    def save_company_descriptions(self, ticker, description_dict):
        company_descriptions = self.db.company_descriptions
        company_description_dict = {'company': ticker, 'data': description_dict}
        company_descriptions.insert_one(company_description_dict)

    def close_client(self):
        self.client.close()
