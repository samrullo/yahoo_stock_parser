from header import *

query = """SELECT eq_sm.desc_en, eq_sm.component_weight, eq_ret.* 
        FROM `eq_returns` eq_ret, `eq_sec_master` eq_sm
        WHERE eq_ret.ticker=eq_sm.ticker
        AND eq_sm.new_index_series_code_en='TOPIX Core30'
        """

eq_ret_df = pd.read_sql(query, engine)
logging.info(f"eq_ret_df loaded {len(eq_ret_df)}")

pvt_df = pd.pivot_table(
    eq_ret_df,
    index=["ticker", "desc_en", "component_weight"],
    values=["daily_ret"],
    aggfunc=[np.mean, np.std],
)
logging.info(f"pivoted df {len(pvt_df)}")

eq_df = pd.DataFrame(
    index=pvt_df.index.get_level_values("ticker").tolist(),
    data={
        "desc": pvt_df.index.get_level_values("desc_en").tolist(),
        "daily_ret_mean": pvt_df["mean"]["daily_ret"].tolist(),
        "daily_ret_std": pvt_df["std"]["daily_ret"].tolist(),
        "weight": pvt_df.index.get_level_values("component_weight").tolist(),
    },
)
eq_df["daily_ret_mean_bps"] = eq_df["daily_ret_mean"] * 10000
eq_df["daily_ret_std_bps"] = eq_df["daily_ret_std"] * 10000
logging.info(f"loaded eq_df {len(eq_df)}")

sns.set_style("darkgrid")
g = sns.relplot(
    x="daily_ret_std_bps",
    y="daily_ret_mean_bps",
    data=eq_df,
    kind="scatter",
    size="weight",
)
g.fig.suptitle("Risk/Return of Topix 30 equities")
g.fig.set_size_inches(15, 15)
g.set(xlabel="Daily Risk (bps)", ylabel="Daily Mean Return (bps)")
ax = g.axes[0, 0]
for i, row in eq_df.iterrows():
    x_coord = row["daily_ret_std_bps"]
    y_coord = row["daily_ret_mean_bps"]
    point_label = i + " : " + row["desc"]
    ax.text(
        x_coord + 0.05, y_coord, point_label, horizontalalignment="left", fontsize=9
    )
plt.show()
