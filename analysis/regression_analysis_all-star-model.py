import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from statsmodels.stats.outliers_influence import variance_inflation_factor
from config import DB_CONFIG
from sklearn.model_selection import train_test_split
import statsmodels.api as sm
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score,f1_score,precision_score, recall_score
from sklearn.model_selection import GridSearchCV
from sklearn.calibration import calibration_curve,CalibratedClassifierCV


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
    X = df[['won ALLSTAR','pre_win_precentage','PTS percentile group','TRB percentile group','AST percentile group','STL percentile group','BLK percentile group','TOV percentile group'
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

# regression_var_test('MAIN_DATA')

#LR model training
query = QUERIES.get("TEST_DATA", None)
df = pd.read_sql(query, engine)
# print(df.columns)
X = df[['won ALLSTAR','pre_win_precentage','PTS percentile group','TRB percentile group','AST percentile group','STL percentile group','BLK percentile group','TOV percentile group'
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
# print(confusion_matrix(y_test, y_pred))
# print(classification_report(y_test, y_pred))
# print("ROC AUC Score:", roc_auc_score(y_test, all_star_model.predict_proba(X_test)[:, 1]))

#C - regularization  strength
param_grid = {'C': [0.001, 0.01, 0.1, 1, 10, 100]}
grid = GridSearchCV(LogisticRegression(penalty='l2', solver='liblinear', class_weight='balanced'),
                    param_grid,
                    scoring='f1',
                    cv=5)
grid.fit(X_train, y_train)
# print("Best C:", grid.best_params_['C'])

#Threshold Tuning
# get predicted probabilities (positive class)
y_proba = all_star_model.predict_proba(X_test)[:, 1]
thresholds = [0.5, 0.45, 0.4, 0.35, 0.3, 0.25, 0.2, 0.1]
# f1_scores = []
# precisions = []
# recalls = []
# false_negatives = []
# false_positives = []

for t in thresholds:
    y_pred = (y_proba >= t).astype(int)
    f1 = f1_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    cm = confusion_matrix(y_test, y_pred)
    # print(f"Threshold: {t:.2f}, Recall: {recall:.3f}, Precision: {precision:.3f}, F1: {f1:.3f}, FN: {cm[1, 0]}, FP: {cm[0, 1]}")

### this graphs all the metrics to compare if dropping the threshold is worth the trade off of worse metrics
# for t in thresholds:
#     y_pred = (y_proba >= t).astype(int)
#     f1_scores.append(f1_score(y_test, y_pred))
#     precisions.append(precision_score(y_test, y_pred))
#     recalls.append(recall_score(y_test, y_pred))
#     cm = confusion_matrix(y_test, y_pred)
#     false_negatives.append(cm[1, 0])
#     false_positives.append(cm[0, 1])
#
# fig, ax1 = plt.subplots(figsize=(12, 8))
#
# ax1.plot(thresholds, f1_scores, label='F1 Score', marker='o')
# ax1.plot(thresholds, precisions, label='Precision', marker='o')
# ax1.plot(thresholds, recalls, label='Recall', marker='o')
# ax1.plot(thresholds, false_negatives, label='False Negatives', marker='o')
#
# ax1.set_xlabel('Threshold')
# ax1.set_ylabel('Score / Count')
# ax1.invert_xaxis()
# ax1.legend(loc='upper left')
# ax1.grid(True)
#
# ax2 = ax1.twinx()  # Create a second y-axis
# ax2.plot(thresholds, false_positives, label='False Positives', color='red', marker='x')
# ax2.set_ylabel('False Positives Count', color='red')
# ax2.tick_params(axis='y', labelcolor='red')
# ax2.legend(loc='upper right')
#
# plt.title('Model Performance Metrics vs Decision Threshold')
# plt.show()

#calibration
# Isotonic Regression
calibrated_model = CalibratedClassifierCV(all_star_model, method='isotonic', cv='prefit')
calibrated_model.fit(X_train, y_train)

# get predicted probabilities (positive class)
y_proba = calibrated_model.predict_proba(X_test)[:, 1]

# compute calibration curve
prob_true, prob_pred = calibration_curve(y_test,y_proba, n_bins = 10)

plt.plot(prob_pred, prob_true, marker='o', label='Calibration curve')
plt.plot([0, 1], [0, 1], linestyle='--', label='Perfectly calibrated')
plt.xlabel('Mean predicted probability')
plt.ylabel('Fraction of positives')
plt.title('Calibration Curve')
plt.legend()
plt.show()

# Apply custom threshold (0.35) to get class predictions
threshold = 0.35
y_pred_custom = (y_proba >= threshold).astype(int)

#checking confusion matrix and classifcation report for calibrated model
print(confusion_matrix(y_test, y_pred_custom))
print(classification_report(y_test, y_pred_custom))
print("ROC AUC Score:", roc_auc_score(y_test, y_proba))


#threshold turning for calibrated model
# y_proba = calibrated_model.predict_proba(X_test)[:, 1]
# thresholds = [0.5, 0.45, 0.4, 0.35, 0.3, 0.25, 0.2, 0.1,0.05]
# for t in thresholds:
#     y_pred = (y_proba >= t).astype(int)
#     f1 = f1_score(y_test, y_pred)
#     precision = precision_score(y_test, y_pred)
#     recall = recall_score(y_test, y_pred)
#     cm = confusion_matrix(y_test, y_pred)
#     print(f"Threshold: {t:.2f}, Recall: {recall:.3f}, Precision: {precision:.3f}, F1: {f1:.3f}, FN: {cm[1, 0]}, FP: {cm[0, 1]}")