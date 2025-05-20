import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from statsmodels.stats.outliers_influence import variance_inflation_factor
from config import DB_CONFIG
from sklearn.model_selection import train_test_split
import statsmodels.api as sm
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score

engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

QUERIES ={
'MAIN_DATA' :
    """
     SELECT * from staging.logr_allstar_data
    """,

    'TEST_DATA':
    """
    SELECT * from staging.logr_allstar_data
    where season != 2025
    """,
    'PRED_DATA':
        """
        SELECT * from staging.logr_allstar_data
        where season == 2025
        """,


}

def regression_var_test(query_input):
    query = QUERIES.get(query_input, None)
    df = pd.read_sql(query, engine)
    # print(df.columns)

    #find which variables are most impactful in producing all stars
    X = df[['pre_win_precentage','PTS percentile group','TRB percentile group','AST percentile group','STL percentile group','BLK percentile group','TOV percentile group'
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

query = QUERIES.get("TEST_DATA", None)
df = pd.read_sql(query, engine)
X = df[['pre_win_precentage','PTS percentile group','TRB percentile group','AST percentile group','STL percentile group','BLK percentile group','TOV percentile group'
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
#drops any na rows
X = X.dropna()
#realiggns X with y
y = y.loc[X.index]
X = sm.add_constant(X)

#spliting the train and test sets
X_train, X_test, y_train, y_test = train_test_split(X,y,test_size = 0.2, random_state = 42)
all_star_model = LogisticRegression(penalty = 'l2', solver='liblinear',class_weight='balanced')
all_star_model.fit(X_train, y_train)
y_pred = all_star_model.predict(X_test)
print(confusion_matrix(y_test, y_pred))
print(classification_report(y_test, y_pred))
print("ROC AUC Score:", roc_auc_score(y_test, all_star_model.predict_proba(X_test)[:, 1]))
