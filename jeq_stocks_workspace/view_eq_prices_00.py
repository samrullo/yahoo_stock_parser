from header import *

eq_px_df = pd.read_sql("SELECT * FROM `eq_prices`", engine)
eq_px_df["adate"] = pd.to_datetime(eq_px_df["adate"])
eq_px_df = eq_px_df.sort_values(["adate", "ticker"])
logging.info(f"eq_prices table records : {len(eq_px_df):,}")

logging.info(f"min adate : {eq_px_df['adate'].min()}")
logging.info(f"max adate : {eq_px_df['adate'].max()}")
logging.info(f"number of days : {eq_px_df['adate'].nunique():,}")
logging.info(f"distinct number of equities : {eq_px_df['ticker'].nunique()}")

# let's compute means and standard deviations based on raw px_close and plot results
grpdf = eq_px_df.groupby("ticker")["px_close"].agg(['mean', 'std'])