from sqlalchemy import create_engine
from sqlalchemy import Table, MetaData, Column, String, Integer, Float, Date
import os

stock_db_path = os.path.join(r"C:\Users\amrul\PycharmProjects\stocks", "datasets", "stock.db")
db_uri = f"sqlite:///{stock_db_path}"

engine = create_engine(db_uri)

meta = MetaData()

if "eq_prices" not in engine.table_names():
    stock_def_tbl = Table("eq_prices", meta, Column("id", Integer, primary_key=True), Column('adate', Date),
                          Column("ticker", String),
                          Column("px_open", Integer), Column("px_high", Integer), Column("px_low", Integer),
                          Column("px_close", Integer), Column("volume", Integer), Column("px_close_after_adj", Integer))
    meta.create_all(engine)

if "eq_returns" not in engine.table_names():
    eq_ret_tbl = Table("eq_returns", meta, Column("id", Integer, primary_key=True), Column("ticker", String),
                       Column("adate", Date), Column("previous_date", Date),
                       Column("price", Float), Column("previous_price", Float), Column("daily_ret", Float))
    meta.create_all(engine)

if "eq_sec_master" not in engine.table_names():
    eq_sm_tbl = Table("eq_sec_master", meta, Column("id", Integer, primary_key=True),
                      Column("ticker", String),
                      Column("weight_date", Date),
                      Column("desc_en", String),
                      Column("desc_jp", String),
                      Column("industry_en", String),
                      Column("industry_jp", String),
                      Column("new_index_series_code_en", String),
                      Column("new_index_series_code_jp", String),
                      Column("component_weight", Float))
    meta.create_all(engine)

if "eq_company_profile" not in engine.table_names():
    eq_company_profile_tbl = Table("eq_company_profile", meta, Column("id", Integer, primary_key=True),
                                   Column("ticker", String),
                                   Column("features", String),
                                   Column("consolidated_business", String),
                                   Column("headquarters", String),
                                   Column("closest_station", String),
                                   Column("phone", String),
                                   Column("industry", String),
                                   Column("company_name_en", String),
                                   Column("ceo", String),
                                   Column("founded_date", Date),
                                   Column("listed_market", String),
                                   Column("ipo_date", Date),
                                   Column("settlement_date", String),
                                   Column("number_of_shares_per_transaction", Integer),
                                   Column("number_of_employees_single", Integer),
                                   Column("number_of_employees_consolidated", Integer),
                                   Column("average_age", Float),
                                   Column("average_annual_salary", Integer),
                                   )
    meta.create_all(engine)
