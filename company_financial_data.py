import logging

from pyfmpcloud import company_valuation as cv

logging.basicConfig(filename='logs/financial_data.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


def get_company_balance_sheet_data(ticker, period, client):
    try:
        balance_data_df = cv.balance_sheet(ticker, period=period, ftype='full')
        if not balance_data_df.empty:
            client.save_balance_sheet_data(ticker, balance_data_df.to_dict(orient='records'))
        else:
            logger.info("Balance sheet data is not available for: {}".format(ticker))
    except Exception as err:
        logger.info("Error in  getting balance sheet data for: {}. Error details: {}".format(ticker, err))


def get_company_income_statement_data(ticker, period, client):
    try:
        income_data_df = cv.income_statement(ticker, period=period, ftype='full')
        if not income_data_df.empty:
            client.save_income_statement_data(ticker, income_data_df.to_dict(orient='records'))
        else:
            logger.info("Income statement data is not available for: {}".format(ticker))
    except Exception as err:
        logger.info("Error in  getting income statement data for: {}. Error details: {}".format(ticker, err))


def get_company_cash_flow_data(ticker, period, client):
    try:
        cash_flow_df = cv.cash_flow_statement(ticker, period=period, ftype='full')
        if not cash_flow_df.empty:
            client.save_cash_flow_data(ticker, cash_flow_df.to_dict(orient='records'))
        else:
            logger.info("Cash flow data is not available for: {}".format(ticker))
    except Exception as err:
        logger.info("Error in  getting cash flow data for: {}. Error details: {}".format(ticker, err))


def get_company_ratios(ticker, period, client):
    try:
        ratios_df = cv.financial_ratios(ticker, period=period, ttm=False)
        if not ratios_df.empty:
            client.save_fin_ratios_data(ticker, ratios_df.to_dict(orient='records'))
        else:
            logger.info("Financial ratios are not available for: {}".format(ticker))
    except Exception as err:
        logger.info("Error in  getting financial ratios for: {}. Error details: {}".format(ticker, err))
