from header import *
from tqdm import tqdm
from sampytools.list_utils import generate_comma_separated_text
from utils.analytics import ewma_volatility

# I want to compute mean return and volatilities of TOPIX Core30 stocks
eqsmdf = pd.read_sql("SELECT * FROM eq_sec_master WHERE new_index_series_code_en='TOPIX Core30'", engine)
logging.info(f"Fetched eq sec_master : {len(eqsmdf)}")

crdf = pd.read_sql(
    f"SELECT * FROM eq_returns WHERE ticker in ({generate_comma_separated_text(eqsmdf.ticker.unique())})", engine)
logging.info(f"Fetched {len(crdf):,} daily returns")
crdf["adate"] = pd.to_datetime(crdf["adate"])

# we will use daily returns since 2023 to compute mean returns and volatilities
subcrdf = crdf[crdf["adate"] >= "2023"][["adate", "ticker", "daily_ret"]]

tickers = list(subcrdf.ticker.unique())
eqmvdf = pd.DataFrame({"ticker": tickers, "eret": 0.0, "vol": 0.0})

with tqdm(total=len(tickers)) as pbar:
    for idx, row in eqmvdf.iterrows():
        ticker = row["ticker"]
        df = subcrdf[subcrdf["ticker"] == ticker]
        eret = df["daily_ret"].mean()
        vols = ewma_volatility(df["daily_ret"], decay_factor=0.94)
        eqvol = vols.iloc[-1]
        eqmvdf.loc[idx, "eret"] = eret
        eqmvdf.loc[idx, "vol"] = eqvol
        pbar.set_description(f"{ticker} eret : {eret:.2}, vol : {eqvol:.2}")
        pbar.update(1)
