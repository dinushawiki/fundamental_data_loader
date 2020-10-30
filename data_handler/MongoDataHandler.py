from pymongo import MongoClient


class MongoDataHandler:
    def __init__(self):
        self.client = MongoClient("localhost", 27017)
        self.db = self.client['financial_data']

    def save_balance_sheet_data(self, ticker, balance_data_df):
        balance_sheet = self.db.balance_sheet
        balance_data_sets = balance_data_df.to_dict(orient='records')
        for balance_data in balance_data_sets:
            balance_data_dict = {}
            year = balance_data['date'].year
            balance_data_dict['company'] = ticker
            balance_data_dict['year'] = year
            balance_data_dict['data'] = balance_data
            balance_sheet.insert_one(balance_data_dict)

    def save_income_statement_data(self, ticker, income_statement_df):
        income_statement = self.db.income_statement
        income_statement_sets = income_statement_df.to_dict(orient='records')
        for income_data in income_statement_sets:
            income_data_dict = {}
            year = income_data['date'].year
            income_data_dict['company'] = ticker
            income_data_dict['year'] = year
            income_data_dict['data'] = income_data
            income_statement.insert_one(income_data_dict)

    def save_cash_flow_data(self, ticker, cash_flow_df):
        cash_flow = self.db.cash_flow
        cash_flow_sets = cash_flow_df.to_dict(orient='records')
        for cash_flow_data in cash_flow_sets:
            cash_flow_dict = {}
            year = cash_flow_data['date'].year
            cash_flow_dict['company'] = ticker
            cash_flow_dict['year'] = year
            cash_flow_dict['data'] = cash_flow_data
            cash_flow.insert_one(cash_flow_dict)

    def save_fin_ratios_data(self, ticker, fin_ratios_df):
        fin_ratios = self.db.fin_ratios
        fin_ratios_sets = fin_ratios_df.to_dict(orient='records')
        for fin_ratios_data in fin_ratios_sets:
            fin_ratios_dict = {}
            year = fin_ratios_data['date'].year
            fin_ratios_dict['company'] = ticker
            fin_ratios_dict['year'] = year
            fin_ratios_dict['data'] = fin_ratios_data
            fin_ratios.insert_one(fin_ratios_dict)

    def save_company_profiles(self, ticker, profile_dict):
        company_profiles = self.db.company_profiles
        company_profile_dict = {'company': ticker, 'data': profile_dict}
        company_profiles.insert_one(company_profile_dict)

    def save_company_descriptions(self, ticker, description_dict):
        company_descriptions = self.db.company_descriptions
        company_description_dict = {'company': ticker, 'data': description_dict}
        company_descriptions.insert_one(company_description_dict)

    def close_client(self):
        self.client.close()
