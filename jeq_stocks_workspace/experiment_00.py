from config import Config
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import seaborn as sns
import matplotlib.pyplot as plt

import logging

logging.basicConfig(format="%(asctime)-15s %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

engine = create_engine(Config.db_uri)

query = "SELECT * FROM `eq_prices`"
eq_px_df = pd.read_sql(query, engine, parse_dates=["adate"])
logging.info(f"eq_prices has total of {len(eq_px_df)} records")

eq_sm_df = pd.read_sql("SELECT * FROM `eq_sec_master`", engine)
logging.info(f"eq_sec_master has total of {len(eq_sm_df)} records")

# eq_prices columns
# ['id', 'adate', 'ticker', 'px_open', 'px_high', 'px_low', 'px_close','volume', 'px_close_after_adj']

# let's add year and month columns to eq_px_df
eq_px_df["year"] = eq_px_df["adate"].map(lambda adate: adate.year)
eq_px_df["month"] = eq_px_df["adate"].map(lambda adate: adate.month)

# let's merge eq_px_df and eq_sm_df
eq_px_mrg_df = pd.merge(left=eq_px_df, right=eq_sm_df, on="ticker", how="left")
logging.info(f"eq_px_df : {len(eq_px_df)}, eq_px_mrg_df : {len(eq_px_mrg_df)}")

# equity with the largest price
max_px_close = eq_px_mrg_df["px_close"].max()
logging.info(
    f"equity with max px_close :\n{eq_px_mrg_df.loc[eq_px_mrg_df['px_close'] == max_px_close].to_string()}"
)

# equity with the smallest price
min_px_close = eq_px_mrg_df["px_close"].min()
logging.info(
    f"equity with smallest px_close :\n{eq_px_mrg_df.loc[eq_px_mrg_df['px_close'] == min_px_close].to_string()}"
)

# box plot of equity prices px_close by year. box plot will show minimum and maximum values, its body is between 25% quartile and 75% quartile
sns.set_style("darkgrid")
g = sns.catplot(
    x="year", y="px_close", data=eq_px_mrg_df, hue="industry_en", kind="box"
)
g.fig.suptitle(f"Equity close price box plot by year")
g.fig.set_size_inches(18, 7)
plt.show()

# box plot volume by year
g = sns.catplot(x="year", y="volume", data=eq_px_mrg_df, kind="box")
g.fig.suptitle("Volume by year box plot")
g.fig.set_size_inches(18, 7)
plt.show()

grp_df = eq_sm_df.groupby("industry_en").count()
grp_df.sort_values("desc_en", ascending=False, inplace=True)

g = sns.catplot(x=grp_df.index, y="desc_en", data=grp_df, kind="bar")
g.fig.suptitle("Count by industry")
ax = g.fig.axes[0]
ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
plt.show()
