import logging
from datetime import datetime

import dateutil.parser
import yfinance as yf

import data_handler.MongoDataHandler as MongoDataHandler

logging.basicConfig(filename='logs/load_balance_sheets.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


def main():
    now = datetime.now()
    current_year = now.year
    logger.info("Company financial data loading script started at : {}".format(datetime.now()))
    data_client = MongoDataHandler.MongoDataHandler()
    tickers = data_client.get_ticker_list()
    ticker_chunks = divide_chunks(tickers, 1000)
    for chunk in ticker_chunks:
        ticker_chunk_string = ' '.join(chunk)
        ticker_results = yf.download(ticker_chunk_string)
        recent_ticker_results = ticker_results[ticker_results.index > dateutil.parser.parse("2015-01-01")]
        df_rearranged = recent_ticker_results.T.swaplevel(0, 1)
        dict_to_save = df_rearranged.groupby(level=0).apply(lambda df: df.xs(df.name).to_dict()).to_dict()
        print(dict_to_save)


def divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]


if __name__ == '__main__':
    main()
