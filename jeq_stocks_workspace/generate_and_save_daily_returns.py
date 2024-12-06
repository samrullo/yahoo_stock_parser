from header import *
from tqdm import tqdm
from sqlalchemy import Table, Date, Float, String

eq_px_df = pd.read_sql("SELECT * FROM `eq_prices`", engine)
eq_px_df["adate"] = pd.to_datetime(eq_px_df["adate"])
eq_px_df = eq_px_df.sort_values(["adate", "ticker"])
logging.info(f"eq prices : {len(eq_px_df)}")

logging.info(f"min adate : {eq_px_df['adate'].min()}")
logging.info(f"max adate : {eq_px_df['adate'].max()}")
logging.info(f"number of days : {eq_px_df['adate'].nunique()}")
logging.info(f"distinct number of equities : {eq_px_df['ticker'].nunique()}")

# let's compute means and standard deviations based on raw px_close and plot results
grpdf = eq_px_df.groupby("ticker")["px_close"].agg(['mean', 'std'])

# let's calculate equity daily returns using pct_change method
eqret_table_name = "eq_returns"
eqret_table = Table(eqret_table_name, meta, autoload_with=engine)

pxdf = eq_px_df

crdf_list = []
with tqdm(total=len(grpdf)) as pbar:
    for ticker in grpdf.index:
        subpxdf = pxdf[pxdf["ticker"] == ticker]
        subpxdf.index = subpxdf["adate"]
        crdf = subpxdf[["px_close_after_adj"]].pct_change().iloc[1:]
        crdf["adate"] = crdf.index
        crdf["previous_date"] = subpxdf.iloc[:-1]["adate"]
        crdf["ticker"] = ticker

        # since I applied pct_change on px_close_after_adj, it represents daily returns
        crdf = crdf.rename(columns={"px_close_after_adj": "daily_ret"})

        # I will trust that iloc will respect the order of rows in the dataframe
        crdf["price"] = subpxdf.iloc[1:]["px_close_after_adj"].values
        crdf["previous_price"] = subpxdf.iloc[:-1]["px_close_after_adj"].values
        crdf_list.append(crdf)
        pbar.set_description(f"{ticker} calculated daily returns from {crdf.index.min()} to {crdf.index.max()}")
        pbar.update(1)
        crdf.to_sql(eqret_table_name, engine, index=False, if_exists="append",
                    dtype={"adate": Date, "previous_date": Date, "ticker": String, "price": Float,
                           "previous_price": Float, "daily_ret": Float})
crdf = pd.concat(crdf_list)
crgrpdf = crdf.groupby(["ticker"])["daily_ret"].agg(['mean', 'std', 'count'])
print(f"min px returns : {crgrpdf['mean'].min()}")
print(f"max px returns : {crgrpdf['mean'].max()}")
print(f"min px returns std: {crgrpdf['std'].min()}")
print(f"max px returns std: {crgrpdf['std'].max()}")
