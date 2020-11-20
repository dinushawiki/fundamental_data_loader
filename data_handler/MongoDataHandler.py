from pymongo import MongoClient
from datetime import datetime


class MongoDataHandler:
    def __init__(self):
        self.client = MongoClient("localhost", 27017)
        self.db = self.client['financial_data']

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

    def save_balance_sheet_data(self, ticker, balance_data_sets):
        if balance_data_sets:
            balance_sheet = self.db.balance_sheet
            self.save_latest_balance_sheet(ticker, balance_data_sets[0])
            for balance_data in balance_data_sets:
                balance_data_dict = {}
                dt = datetime.strptime(balance_data['date'], '%Y-%m-%d')
                balance_data_dict['company'] = ticker
                balance_data_dict['year'] = dt.year
                balance_data_dict['data'] = balance_data
                balance_sheet.insert_one(balance_data_dict)

    def save_income_statement_data(self, ticker, income_statement_sets):
        if income_statement_sets:
            income_statement = self.db.income_statement
            self.save_latest_income_statement(ticker, income_statement_sets[0])
            for income_data in income_statement_sets:
                income_data_dict = {}
                dt = datetime.strptime(income_data['date'], '%Y-%m-%d')
                income_data_dict['company'] = ticker
                income_data_dict['year'] = dt.year
                income_data_dict['data'] = income_data
                income_statement.insert_one(income_data_dict)

    def save_cash_flow_data(self, ticker, cash_flow_sets):
        if cash_flow_sets:
            cash_flow = self.db.cash_flow
            self.save_latest_cash_flow(ticker, cash_flow_sets[0])
            for cash_flow_data in cash_flow_sets:
                cash_flow_dict = {}
                dt = datetime.strptime(cash_flow_data['date'], '%Y-%m-%d')
                cash_flow_dict['company'] = ticker
                cash_flow_dict['year'] = dt.year
                cash_flow_dict['data'] = cash_flow_data
                cash_flow.insert_one(cash_flow_dict)

    def save_company_profiles(self, ticker, profile_dict):
        company_profiles = self.db.company_profiles
        company_profile_dict = {'company': ticker, 'data': profile_dict}
        company_profiles.insert_one(company_profile_dict)

    def save_company_descriptions(self, ticker, description_dict):
        company_descriptions = self.db.company_descriptions
        company_description_dict = {'company': ticker, 'data': description_dict}
        company_descriptions.insert_one(company_description_dict)

    def save_latest_income_statement(self, ticker, income_data):
        latest_fin_data = self.db.latest_fin_data
        income_data_dict = {}
        dt = datetime.strptime(income_data['date'], '%Y-%m-%d')
        income_data_dict['year'] = dt.year
        income_data_dict['data'] = income_data
        latest_fin_data.update_one({'_id': ticker}, {'$set': {'income': income_data_dict}}, True)

    def save_latest_balance_sheet(self, ticker, balance_data):
        latest_fin_data = self.db.latest_fin_data
        balance_data_dict = {}
        dt = datetime.strptime(balance_data['date'], '%Y-%m-%d')
        balance_data_dict['year'] = dt.year
        balance_data_dict['data'] = balance_data
        latest_fin_data.update_one({'_id': ticker}, {'$set': {'balance': balance_data_dict}}, True)

    def save_latest_cash_flow(self, ticker, cash_data):
        latest_fin_data = self.db.latest_fin_data
        cash_data_dict = {}
        dt = datetime.strptime(cash_data['date'], '%Y-%m-%d')
        cash_data_dict['year'] = dt.year
        cash_data_dict['data'] = cash_data
        latest_fin_data.update_one({'_id': ticker}, {'$set': {'cash': cash_data_dict}}, True)

    def close_client(self):
        self.client.close()
