import pandas as pd
import numpy as np


def ewma_volatility(returns: pd.Series, decay_factor: float = 0.94):
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


def ewma_vectorized_vols(returns, decay_factor=0.94):
    # Calculate EWMA Variance using Pandas `.ewm()`
    ewma_variance = returns.ewm(span=(2 / (1 - decay_factor) - 1)).var()

    # Calculate EWMA Volatility (square root of variance)
    ewma_volatility = np.sqrt(ewma_variance)
    return ewma_volatility
