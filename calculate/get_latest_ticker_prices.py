import logging

import pandas as pd

logging.basicConfig(filename='logs/load_ticker_prices.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


def get_prices(data_client):
    all_ticker_prices = data_client.get_all_ticker_prices()
    latest_ticker_price_list = []
    for ticker_price in all_ticker_prices:
        try:
            last_item = list(ticker_price['prices'])[-1]
            latest_ticker_price = {'symbol': ticker_price['_id'], 'trade_date': last_item,
                                   'price': ticker_price['prices'][last_item]['Adj Close']}
            latest_ticker_price_list.append(latest_ticker_price)
        except Exception as err:
            logger.info("Can't get most recent price: {}. Error: {}".format(ticker_price, err))

    df = pd.DataFrame(latest_ticker_price_list)
    return df.dropna()


# if __name__ == '__main__':
#     data_client = MongoDataHandler.MongoDataHandler()
#     get_prices(data_client)
