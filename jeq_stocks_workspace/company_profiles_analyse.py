from header import *

profile_df = pd.read_sql("SELECT * FROM `eq_company_profile`", engine)
logging.info(f"total of {len(profile_df)} company profiles")

sns.set_style('darkgrid')
# sns.set(font="IPAexGothic")
g = sns.catplot(x='industry', y='average_annual_salary', data=profile_df, kind='box')
g.fig.suptitle("Average annual salary by industry", y=1.03)
g.fig.set_size_inches(18, 7)
plt.xticks(rotation=80)
plt.show()

g = sns.catplot(x='industry', y='number_of_employees_consolidated', data=profile_df, kind='box')
g.fig.suptitle("Number of employees consolidated by industry", y=1.04)
g.fig.set_size_inches(18, 7)
plt.xticks(rotation=80)
plt.show()

g = sns.catplot(x='industry', data=profile_df, kind='count')
g.fig.suptitle("Company count by industry")
g.fig.set_size_inches(18, 7)
plt.xticks(rotation=80)
plt.show()
