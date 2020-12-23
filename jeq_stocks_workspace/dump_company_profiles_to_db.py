import os
import pandas as pd
import numpy as np
from stocks.yahoo_stock_parser import YahooStockParser
import datetime
import logging
from config import Config
from sqlalchemy import create_engine, Table, MetaData, Date, Integer, Float, String

logging.basicConfig(format='%(asctime)s : %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)

engine = create_engine(Config.db_uri)

eq_sm_df = pd.read_sql("SELECT * FROM `eq_sec_master`", engine)
logging.info(f"eq_sec_master has total of {len(eq_sm_df)} records")

profiles = []

for i, ticker in enumerate(eq_sm_df['ticker'].tolist()):
    yahoo_obj = YahooStockParser(ticker, datetime.date(2020, 7, 1), datetime.date(2020, 7, 19))
    profile = yahoo_obj.get_company_profile()
    logger.info(f"{i} : {profile}")
    profiles.append(profile)

profiles_df = pd.DataFrame(profiles)

dtype_dict = {col: String for col in profiles_df.columns.tolist()}
dtype_dict["founded_date"] = Date
dtype_dict["ipo_date"] = Date
dtype_dict["number_of_shares_per_transaction"] = Integer
dtype_dict['number_of_employees_single'] = Integer
dtype_dict['average_annual_salary'] = Integer
dtype_dict['average_age'] = Float

profiles_df.to_sql('eq_company_profile', engine, index=False, dtype=dtype_dict, if_exists='append')
