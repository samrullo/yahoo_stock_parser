from header import *

eq_px_df = pd.read_sql("SELECT * FROM `eq_prices`", engine)
logging.info(f"eq prices : {len(eq_px_df)}")

logging.info(f"min adate : {eq_px_df['adate'].min()}")
logging.info(f"max adate : {eq_px_df['adate'].max()}")
logging.info(f"number of days : {eq_px_df['adate'].nunique()}")
logging.info(f"distinct number of equituis : {eq_px_df['ticker'].nunique()}")