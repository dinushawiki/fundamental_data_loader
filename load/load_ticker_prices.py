import logging
import os
from datetime import datetime, timedelta

import dateutil.parser
import yfinance as yf

from data_handler.mongo import MongoDataHandler

logging.basicConfig(filename='{}/logs/load_ticker_prices.log'.format(os.path.dirname(os.getcwd())),
                    level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


def main():
    now = datetime.now()
    update_from = datetime.now() - timedelta(365)
    logger.info("Ticker price loading script started at : {}".format(datetime.now()))
    logger.info("Ticker price are retrieved from  : {}".format(datetime.strftime(update_from, '%Y-%m-%d')))
    data_client = MongoDataHandler()
    tickers = data_client.get_ticker_list()
    ticker_chunks = divide_chunks(tickers, 1000)
    for chunk in ticker_chunks:
        ticker_chunk_string = ' '.join(chunk)
        ticker_results = yf.download(ticker_chunk_string)
        recent_ticker_results = ticker_results[
            ticker_results.index > dateutil.parser.parse(datetime.strftime(update_from, '%Y-%m-%d'))]
        df_rearranged = recent_ticker_results.T.swaplevel(0, 1)
        df_rearranged.columns = df_rearranged.columns.map(str)
        prices_dict = df_rearranged.groupby(level=0).apply(lambda df: df.xs(df.name).to_dict()).to_dict()
        price_list = []
        for key, value in prices_dict.items():
            price_dict = {'_id': key, 'prices': value}
            print(price_dict)
            price_list.append(price_dict)
        data_client.save_all_ticker_prices(price_list)


def divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]


if __name__ == '__main__':
    main()
