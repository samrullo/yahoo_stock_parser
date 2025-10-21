from header import *
import matplotlib.pyplot as plt
import matplotlib
from sampytools.configdict import ConfigDict
from sampytools.list_utils import generate_comma_separated_text
from sampytools.datetime_utils import to_yyyymmdd
from utils.analytics import ewma_volatility

matplotlib.use("Qt5Agg")

plt.ion()

tickers = ["9551.T", "9531.T"]
pxdf = pd.read_sql(
    f"SELECT * FROM `eq_prices` WHERE ticker in ({generate_comma_separated_text(tickers)})",
    engine,
)
eqretdf = pd.read_sql(
    f"SELECT * FROM eq_returns WHERE ticker in ({generate_comma_separated_text(tickers)})",
    engine,
)
eqretdf["adate"] = pd.to_datetime(eqretdf["adate"])

d = ConfigDict({})
d["one"]["ticker"] = tickers[0]
d["two"]["ticker"] = tickers[1]

for label in ["one", "two"]:
    ticker = d[label]["ticker"]
    crdf = eqretdf[eqretdf["ticker"] == ticker].copy()
    crdf.index = crdf["adate"]
    subcrdf = crdf.loc["2024"]
    subcrdf["vol"] = ewma_volatility(subcrdf["daily_ret"], decay_factor=0.94)
    d[label]["crdf"] = crdf
    d[label]["subcrdf"] = subcrdf

df = pd.DataFrame(
    {"one_vol": d["one"]["subcrdf"]["vol"], "two_vol": d["two"]["subcrdf"]["vol"]}
)
df.index = d["one"]["subcrdf"].index
axes = df.plot(
    subplots=True,
    figsize=(10, 6),
    sharey=True,
    title=[d["one"]["ticker"], d["two"]["ticker"]],
)
for ax in axes:
    ax.set_xlabel("Date")
    ax.set_ylabel("Volatility")
    ax.grid(True)
