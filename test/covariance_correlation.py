import os
import pandas as pd
import numpy as np
from stocks.yahoo_stock_parser import YahooStockParser
import datetime
import logging
from config import Config
from sqlalchemy import create_engine, Table, MetaData, Date, Integer, Float, String, select

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

engine = create_engine(Config.db_uri)
meta = MetaData()
eq_px_tbl = Table("eq_prices", meta, autoload=True, autoload_with=engine)
eq_ret_tbl = Table("eq_returns", meta, autoload=True, autoload_with=engine)
con = engine.connect()

eq_ret_df = pd.read_sql("SELECT * FROM `eq_returns` WHERE `adate` BETWEEN '2019-03-31' and '2020-04-30'", engine)
pvt_df = pd.pivot_table(eq_ret_df, 'daily_ret', 'adate', 'ticker')
pvt_df.fillna(0, inplace=True)
corr_df = pvt_df.corr()
cov_df = pvt_df.cov()
daily_mean_stock_returns = pvt_df.mean()


def generate_weights_old(number_of_stocks):
    used_weight = 0
    weight_max = 100
    weights = []
    for i in range(number_of_stocks):
        if weight_max <= 0:
            logging.info(f"weight max {weight_max} is less than or equal to zero so will append zero ")
            weights.append(0)
        else:
            logging.info(f"{i} processing ...")
            logging.info(f"weight_max is {weight_max}")
            random_weight = np.random.randint(0, weight_max)
            logging.info(f"random weight is {random_weight}")
            used_weight += random_weight
            weight_max -= used_weight
            weights.append(random_weight)
    return np.array(weights)


def generate_weights(number_of_stocks):
    preliminary_weights = np.round(np.random.rand(number_of_stocks) * 10)
    scale_factor = 100 / preliminary_weights.sum()
    weights = preliminary_weights * scale_factor / 100
    return weights


def calc_risk(weights, cov_df):
    risk = np.sqrt(weights.T.dot(cov_df).dot(weights))
    return risk * np.sqrt(252)


def calc_return(weights, stock_returns):
    daily_port_ret = weights.T.dot(stock_returns)
    return np.power((1 + daily_port_ret), 252)-1


should_continue = True

while (should_continue):
    number_of_stocks = len(pvt_df.columns.tolist())
    weights = generate_weights(number_of_stocks)
    port_one_return = calc_return(weights, daily_mean_stock_returns)
    port_one_risk = calc_risk(weights, cov_df)
    logging.info(f"weights : {weights * 100}")
    logging.info(f"Annual Return : {port_one_return * 100}")
    logging.info(f"Annual Risk : {port_one_risk * 100}")
    answer = input("Should I continue?")
    if answer == "n":
        should_continue = False
