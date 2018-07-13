from sqlalchemy import Table, Column, ForeignKey, Integer, String, VARCHAR, REAL, SmallInteger, DECIMAL, TIMESTAMP, MetaData, VARCHAR, INTEGER
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.exc import DataError, SQLAlchemyError, DBAPIError
from sqlalchemy import create_engine, inspect, desc, asc
from sqlalchemy.sql import select, and_, or_, not_

import logging

import pandas as pd

import config as cfg

logger = logging.getLogger()

def market_name_to_db_name(market_name):
    market_list = market_name.split('/')

    market_db_name = market_list[1] + '_' + market_list[0]

    return market_db_name

def create_market_table_mapping(market_name):
    '''
    Creating mapping for market table (for better code readability)
    ['date', 'open', 'high', 'low', 'close', 'volume']
    '''
    market_db_name = market_name_to_db_name(market_name)

    global metadata
    return Table(market_db_name, metadata,
                 Column('id', Integer, primary_key=True),
                 Column('date', TIMESTAMP),
                 Column('open', DECIMAL(18, 8)),
                 Column('high', DECIMAL(18, 8)),
                 Column('low', DECIMAL(18, 8)),
                 Column('close', DECIMAL(18, 8)),
                 Column('volume', DECIMAL(26, 8)),
                 schema='binance_data',
                 keep_existing=True
                 )

def write_market_data(market_df, market_name):
    '''
    Writes market_df data to database

    Returns:
        True - success
        False - fail
    '''

    # Creating table first with appropriate data types
    market_table = create_market_table_mapping(market_name)
    market_table.create(checkfirst=True)

    market_db_name = market_name_to_db_name(market_name)
    return_state = True
    global engine, metadata
    try:
        market_df.to_sql(market_db_name, engine, schema='binance_data', index=False, if_exists='append')
    except SQLAlchemyError as e:
        logger.error(e)
        return_state = False

    metadata.remove(market_table)
    return return_state

def create_db_engine(db_name=''):
    '''
    Creating connection to database for next manipulations
    '''
    db_path = 'mysql://{0}:{1}@{2}/{3}{4}'.format(cfg.USER, cfg.PASSWORD, cfg.HOST, db_name, cfg.SOCKET)
    return create_engine(db_path)


def execute_query(query):
    global engine
    try:
        result = engine.execute(query)
    except SQLAlchemyError as e:
        logger.erorr(e)
        logger.error('With query: {}'.format(query))

    return result

engine = create_db_engine()


# Define metadata
metadata = MetaData()
metadata.bind = engine
