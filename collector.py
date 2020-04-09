import sys
import time
import logging
import datetime
import pandas as pd
import numpy as np

from exchange import Exchange

import ccxt

from telegram_handler import TelegramHandler
from telegram_handler.formatters import HtmlFormatter

import config as cfg
import utils.database as db

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger()

BINANCE_OPEN_DATE = 1500076800000
# BINANCE_OPEN_DATE = 1531180800000 # For test
#BINANCE_OPEN_DATE = 1527724800000


def setup_telegram_handler(log_level=logging.ERROR):
    '''
    Setup and return telegram handler
    '''
    telegram_handler = TelegramHandler(cfg.TG_HANDLER_API_TOKEN, cfg.TG_CHAT_ID)
    formatter = HtmlFormatter('%(levelname)s %(message)s')
    telegram_handler.setLevel(log_level)
    telegram_handler.setFormatter(formatter)
    return telegram_handler


def parse_ticker_dataframe(ticker: list) -> pd.DataFrame:
    """
    Analyses the trend for the given ticker history
    :param ticker: See exchange.get_ticker_history
    :return: DataFrame
    """
    cols = ['date', 'open', 'high', 'low', 'close', 'volume']
    frame = pd.DataFrame(ticker, columns=cols)

    frame['date'] = pd.to_datetime(frame['date'],
                                   unit='ms',
                                   infer_datetime_format=True)

    # group by index and aggregate results to eliminate duplicate ticks
    frame = frame.groupby(by='date', as_index=False, sort=True).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'max',
    })
    frame.drop(frame.tail(1).index, inplace=True)     # eliminate partial candle
    return frame


def main(script):
    exchange = Exchange()
    pairs = []

    for k in exchange.get_markets():
        if k['symbol'] is not None:
            pairs.append(k['symbol'])

    for pair in pairs:
        #print(pair)
        result = db.get_last_row_timestamp(pair)

        if result is None:
            last_row_timestamp = BINANCE_OPEN_DATE
        else:
            last_row_dt = result[1]
            last_row_dt = last_row_dt + datetime.timedelta(minutes=1)
            #print('Last row datetime = {}'.format(last_row_dt))
            last_row_timestamp = int(time.mktime(last_row_dt.timetuple())) * 1000
        try:
            data = exchange.get_ticker_history(pair, '1m', last_row_timestamp)
        except Exception as e:
            logger.error(e)

        df = parse_ticker_dataframe(data)
        db.write_market_data(df, pair)


if __name__ == '__main__':
    log_telegram_handler = setup_telegram_handler()
    logger.addHandler(log_telegram_handler)
    main(*sys.argv)
