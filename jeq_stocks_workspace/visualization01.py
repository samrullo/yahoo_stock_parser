import pandas as pd

from header import *
import matplotlib.pyplot as plt
import matplotlib

matplotlib.use("Qt5Agg")

plt.ion()

ticker = "7203.T"
pxdf = pd.read_sql(f"SELECT * FROM `eq_prices` WHERE ticker like '{ticker}'", engine)
eqretdf = pd.read_sql(f"SELECT * FROM eq_returns WHERE ticker like '{ticker}'", engine)

logging.info(f"{ticker} has {len(pxdf):,} prices and {len(eqretdf):,} returns")

eqretdf.index = eqretdf["adate"]
eqretdf.index = pd.to_datetime(eqretdf.index)
eqretdf[["daily_ret"]].plot(kind="line")

subretdf = eqretdf.loc["2024"]


def ewma_volatility(returns, decay_factor=0.94):
    """
    Calculate EWMA volatility from a series of returns.

    Parameters:
    - returns: A pandas Series of returns (daily, monthly, etc.)
    - decay_factor: Lambda, the smoothing parameter (default = 0.94).

    Returns:
    - ewma_vol: A pandas Series of EWMA volatility.
    """
    # Initialize EWMA variance with the first squared return
    ewma_variance = [returns.iloc[0] ** 2]

    # Compute EWMA variance recursively
    for ret in returns.iloc[1:]:
        ewma_variance.append(
            decay_factor * ewma_variance[-1] + (1 - decay_factor) * ret**2
        )

    # Convert variance to volatility
    ewma_vol = pd.Series(np.sqrt(ewma_variance), index=returns.index)
    return ewma_vol


def ewma_vols(returns, decay_factor=0.94):
    # Calculate EWMA Variance using Pandas `.ewm()`
    ewma_variance = returns.ewm(span=(2 / (1 - decay_factor) - 1)).var()

    # Calculate EWMA Volatility (square root of variance)
    ewma_volatility = np.sqrt(ewma_variance)
    return ewma_volatility


returns = subretdf["daily_ret"]
vols = ewma_volatility(returns, decay_factor=0.94)
vols2 = ewma_vols(returns, decay_factor=0.94)
df = pd.DataFrame({"returns": returns, "vol1": vols, "vol2": vols2})
df.plot()
