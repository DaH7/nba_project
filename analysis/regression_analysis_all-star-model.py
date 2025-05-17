import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from statsmodels.stats.outliers_influence import variance_inflation_factor

from config import DB_CONFIG
import statsmodels.api as sm

engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

QUERIES ={
'MAIN_DATA' :
    """
     SELECT * from staging.LogR_allstar_data
    """,

    'TEST_DATA':
    """
    SELECT * from staging.LogR_allstar_data
    where season != 2025
    """



}

query = QUERIES.get('MAIN_DATA', None)
df = pd.read_sql(query, engine)
# print(df.columns)

#find which variables are most impactful in producing all stars


X = df[['PTS percentile group','TRB percentile group','AST percentile group','STL percentile group','BLK percentile group','TOV percentile group'
    ,'won MVP','won DPOY','won MIP']].copy()
y = df['this_season_ALLSTAR'].astype(int)

# convert bool to int for initial bool columns
bool_cols = X.select_dtypes(include='bool').columns
X[bool_cols] = X[bool_cols].astype(int) #true and false to 1 and 0

#creates dummy variables (bool) for each cat (object). the dummy variable indicate if this cat is true or false
cat_cols = X.select_dtypes(include='object').columns
X = pd.get_dummies(X, columns=cat_cols, drop_first=True)

# Convert bool dummies to int
bool_cols = X.select_dtypes(include='bool').columns
X[bool_cols] = X[bool_cols].astype(int)


#check if any columns have Nan in the rows
# print("NaNs per column:\n", X.isna().sum())

#drops any na rows
X = X.dropna()
#realiggns X with y
y = y.loc[X.index]

X = sm.add_constant(X)


model = sm.Logit(y, X)
results = model.fit()
print(results.summary())

#checking VIF ( Variance Inflation Factor)
vif_df = pd.DataFrame()
vif_df['variable'] = X.columns
vif_df['VIF'] = [variance_inflation_factor(X.values,i) for i in range(X.shape[1])]
print(vif_df)

# if __name__ == '__main__':
