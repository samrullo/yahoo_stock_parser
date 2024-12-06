from header import *
from sqlalchemy import delete

table_name = "eq_returns"
eqret_table = Table(table_name, meta, autoload_with=engine)
delete_query = delete(eqret_table)

logging.info(f"will execute and commit this statement : {delete_query}")

# query eq_returns before deleting
eqretdf = pd.read_sql(f"select * from {table_name}", engine)
logging.info(f"{table_name} had {len(eqretdf):,} records before removing all records")
with engine.connect() as conn:
    conn.execute(delete_query)
    conn.commit()

# let's query records
df = pd.read_sql(f"select * from {table_name}", engine)
logging.info(f"{table_name} has {len(df):,} records after removing all records")
