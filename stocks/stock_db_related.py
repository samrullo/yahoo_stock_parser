from sqlalchemy import create_engine
from sqlalchemy import Table, MetaData, Column, String, Integer, Date
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
