from header import *

droplet_mysql_engine = create_engine(Config.droplet_mysql_uri)

eq_company_profile_df = pd.read_sql("select * from eq_company_profile", engine)
eq_prices_df = pd.read_sql("select * from eq_prices", engine)
eq_sec_master_df = pd.read_sql("select * from eq_sec_master", engine)

for df, tbl in zip([eq_company_profile_df, eq_prices_df, eq_sec_master_df], ["eq_company_profile", "eq_prices", "eq_sec_master"]):
    df.to_sql(tbl, droplet_mysql_engine)
    logging.info(f"saved {tbl} {len(df)} to droplet_mysql")
