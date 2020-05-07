from config import Config
import os
import logging
import datetime
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine, Table, MetaData, Date, Integer, Float, String


class EquityReturnCalculator:
    def __init__(self, engine: sqlalchemy.engine, ticker: str, start: datetime.date, end: datetime.date):
        self.engine = engine
        self.ticker = ticker
        self.start = start
        self.end = end
        self.eq_ret_table = "eq_returns"
        self.eq_px_table = "eq_prices"

    def get_eq_prices(self) -> pd.DataFrame:
        """
        Get prices for the ticker for the specified period
        :return: equity prices dataframe
        """
        query = f"SELECT * FROM `{self.eq_px_table}` WHERE `ticker` like '{self.ticker}' AND `adate` between '{self.start}' and '{self.end}'"
        df = pd.read_sql(query, self.engine)
        df.sort_values('adate', inplace=True)
        df.index = range(len(df))
        logging.info(f"Retreived {len(df)} records for {self.ticker} from {self.start} to {self.end}")
        return df

    def generate_equity_returns(self, eq_px_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate daily returns in decimals and return equity prices dataframe with daily returns by removing the first date
        as the first date won't have daily return
        :param eq_px_df:
        :return: equity price dataframe with daily returns
        """
        eq_px_df = eq_px_df.rename(columns={"px_close_after_adj": "price"})
        eq_px_df.loc[1:, 'previous_price'] = eq_px_df.loc[0:eq_px_df.index[-2], 'price'].tolist()
        eq_px_df.loc[1:, 'previous_date'] = eq_px_df.loc[0:eq_px_df.index[-2], 'adate'].tolist()
        eq_px_df['daily_ret'] = eq_px_df['price'] / eq_px_df['previous_price'] - 1
        eq_px_df['adate']=pd.to_datetime(eq_px_df['adate'])
        eq_px_df['previous_date'] = pd.to_datetime(eq_px_df['previous_date'])
        logging.info(f"Finished calculating daily returns for {self.ticker} from {self.start} to {self.end}")
        return eq_px_df.loc[1:, ['ticker', 'adate', 'previous_date', 'price', 'previous_price', 'daily_ret']]

    def save_to_db(self, eq_ret_df: pd.DataFrame):
        """
        Insert daily returns into database table eq_returns
        :param eq_ret_df:
        :return:
        """
        con = self.engine.connect()
        stmt = f"DELETE FROM `{self.eq_ret_table}` WHERE `ticker` like '{self.ticker}' AND `adate` BETWEEN '{eq_ret_df.adate.min()}' AND '{eq_ret_df.adate.max()}'"
        con.execute(stmt)
        eq_ret_df.to_sql("eq_returns", self.engine, index=False, if_exists='append', dtype={"ticker": String, "adate": Date, "previous_date": Date, "price": Float, "previous_price": Float, "daily_ret": Float})
        logging.info(f"Finished inserting {len(eq_ret_df)} daily returns into {self.eq_ret_table}")
