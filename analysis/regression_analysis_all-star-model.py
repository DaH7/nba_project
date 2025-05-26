import pandas as pd
import matplotlib.pyplot as plt
from holoviews.plotting.bokeh.styles import font_size
from sqlalchemy import create_engine
from statsmodels.stats.outliers_influence import variance_inflation_factor
from config import DB_CONFIG
from sklearn.model_selection import train_test_split
import statsmodels.api as sm
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score,f1_score,precision_score, recall_score
from sklearn.model_selection import GridSearchCV
from sklearn.calibration import calibration_curve,CalibratedClassifierCV
from sklearn.model_selection import StratifiedKFold
import seaborn as sns
import numpy as np



engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

QUERIES ={
'MAIN_DATA' :
    """
     SELECT * from staging.new_logr_allstar_data
    """,

    'TRAINING_DATA':
    """
    SELECT * from staging.new_logr_allstar_data
    where season < 2023 
    and season > 1950
    """,

    'PRED_DATA':
        """
        SELECT * from staging.new_logr_allstar_data
        where season >= 2023
        """,


}

def regression_var_test(query_input):
    query = QUERIES.get(query_input, None)
    df = pd.read_sql(query, engine)
    # print(df.columns)

    # #find which variables are most impactful in producing all stars
    # X = df[['GS','Age','won ALLSTAR','pre_win_precentage','PTS percentile group','TRB percentile group','AST percentile group','STL percentile group','BLK percentile group','TOV percentile group'
    #     ,'won MVP','won DPOY','won MIP']].copy() #model 1

    X = df[["eFG% percentile","GS percentile","Pos", "PTS percentile", "AST percentile", "TRB percentile", "STL percentile",
            "BLK percentile", "TOV percentile", "won MVP", "won DPOY", "pre_win_precentage",
            "won ALLSTAR"]].copy() #model 2

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
    results = model.fit(maxiter=100)
    print(results.summary())

    #checking VIF ( Variance Inflation Factor)
    vif_df = pd.DataFrame()
    vif_df['variable'] = X.columns
    vif_df['VIF'] = [variance_inflation_factor(X.values,i) for i in range(X.shape[1])]
    print(vif_df)
regression_var_test('MAIN_DATA')


def all_star_model_analysis(query):
    #LR model training
    query = QUERIES.get(query, None)
    df = pd.read_sql(query, engine)
    # print(df.columns)
    # X = df[['GS percentile group','Age','won ALLSTAR','pre_win_precentage','PTS percentile group','TRB percentile group','AST percentile group','STL percentile group','BLK percentile group','TOV percentile group'
    #     ,'won MVP','won DPOY','won MIP']].copy() #model 1

    X = df[["eFG% percentile","GS percentile", "Pos", "3P%", "FT%", "PTS percentile", "AST percentile", "TRB percentile", "STL percentile",
            "BLK percentile", "TOV percentile", "won MVP", "won DPOY", "pre_win_precentage",
            "won ALLSTAR"]].copy()  # model 2

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


    #C - regularization  strength
    param_grid = {'C': [0.001, 0.01, 0.1, 1, 10, 100]}
    grid = GridSearchCV(LogisticRegression(penalty='l2', solver='liblinear', class_weight='balanced'),
                        param_grid,
                        scoring='f1',
                        cv=5)
    grid.fit(X_train, y_train)
    print("Best C:", grid.best_params_['C'])

    #Threshold Tuning
    # get predicted probabilities (positive class)
    y_proba = all_star_model.predict_proba(X_test)[:, 1]
    thresholds = [0.5, 0.45, 0.4, 0.35, 0.3, 0.25, 0.2, 0.1]

    for t in thresholds:
        y_pred = (y_proba >= t).astype(int)
        f1 = f1_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred)
        recall = recall_score(y_test, y_pred)
        cm = confusion_matrix(y_test, y_pred)
        print(f"Baseline Threshold: {t:.2f}, Recall: {recall:.3f}, Precision: {precision:.3f}, F1: {f1:.3f}, FN: {cm[1, 0]}, FP: {cm[0, 1]}")

    #update to best C
    all_star_model = LogisticRegression(penalty='l2', solver='liblinear', class_weight='balanced',C = 10).fit(X_train, y_train)
    calibrated_model = CalibratedClassifierCV(all_star_model, method='sigmoid', cv='prefit')
    calibrated_model.fit(X_train, y_train)

    # get predicted probabilities (positive class)
    y_proba = calibrated_model.predict_proba(X_test)[:, 1]

    # compute calibration curve
    prob_true, prob_pred = calibration_curve(y_test, y_proba, n_bins=10)

    # Apply custom threshold (0.35) to get class predictions
    threshold = 0.3
    y_pred_custom = (y_proba >= threshold).astype(int)

    # checking confusion matrix and classifcation report for calibrated model
    print(confusion_matrix(y_test, y_pred_custom))
    print(classification_report(y_test, y_pred_custom))
    print("ROC AUC Score:", roc_auc_score(y_test, y_proba))

    # threshold turning for calibrated model
    y_proba = calibrated_model.predict_proba(X_test)[:, 1]
    thresholds = [0.5, 0.45, 0.4, 0.35, 0.3, 0.25, 0.2, 0.1,0.05]
    for t in thresholds:
        y_pred = (y_proba >= t).astype(int)
        f1 = f1_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred)
        recall = recall_score(y_test, y_pred)
        cm = confusion_matrix(y_test, y_pred)
        print(f"Calibrated Threshold: {t:.2f}, Recall: {recall:.3f}, Precision: {precision:.3f}, F1: {f1:.3f}, FN: {cm[1, 0]}, FP: {cm[0, 1]}")

    plt.plot(prob_pred, prob_true, marker='o', label='Calibration curve')
    plt.plot([0, 1], [0, 1], linestyle='--', label='Perfectly calibrated')
    plt.xlabel('Mean predicted probability')
    plt.ylabel('Fraction of positives')
    plt.title('Calibration Curve')
    plt.legend()
    plt.show()

    # Cross Validation
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    threshold = 0.3

    f1_scores, precisions, recalls, aucs = [], [], [], []

    for train_idx, val_idx in skf.split(X, y):
        X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

        all_star_model = LogisticRegression(penalty='l2', solver='liblinear', class_weight='balanced')
        all_star_model.fit(X_train, y_train)

        calibrated_model = CalibratedClassifierCV(all_star_model, method='sigmoid', cv='prefit')
        calibrated_model.fit(X_train, y_train)

        y_proba = calibrated_model.predict_proba(X_val)[:, 1]
        y_pred = (y_proba >= threshold).astype(int)

        f1_scores.append(f1_score(y_val, y_pred))
        precisions.append(precision_score(y_val, y_pred))
        recalls.append(recall_score(y_val, y_pred))
        aucs.append(roc_auc_score(y_val, y_proba))

    print(f"Selected Calibrated Threshold: {threshold}")
    print(f"Avg F1: {np.mean(f1_scores):.3f} ± {np.std(f1_scores):.3f}")
    print(f"Avg Precision: {np.mean(precisions):.3f} ± {np.std(precisions):.3f}")
    print(f"Avg Recall: {np.mean(recalls):.3f} ± {np.std(recalls):.3f}")
    print(f"Avg ROC AUC: {np.mean(aucs):.3f} ± {np.std(aucs):.3f}")

    # error analysis
    false_positives = X_val[(y_pred == 1) & (y_val == 0)].copy()
    false_positives["actual"] = y_val[(y_pred == 1) & (y_val == 0)]

    false_negatives = X_val[(y_pred == 0) & (y_val == 1)].copy()
    false_negatives["actual"] = y_val[(y_pred == 0) & (y_val == 1)]
    # print("False Positives:\n", false_positives)
    # print("False Negatives:\n", false_negatives)

    #getting original dataset with all columns
    query1 = QUERIES.get("TRAINING_DATA", None)
    df1 = pd.read_sql(query1, engine)
    print(df1.columns)

    # after getting predictions (y_pred, y_proba)
    y_proba = calibrated_model.predict_proba(X_test)[:, 1]
    y_pred = (y_proba >= threshold).astype(int)
    results_df = X_test.copy()
    results_df['y_true'] = y_test
    results_df['y_pred'] = y_pred
    results_df['y_proba'] = y_proba


    # merging back some column names to make it easier to read
    results_df = pd.merge(results_df, df1[['Age','Player','season']],
                          left_index=True, right_index=True, how='left')
    results_df.to_csv("results_df", index=False)
# all_star_model_analysis("TRAINING_DATA")

def all_star_model(query,C = 10, threshold = 0.3):
    """
        Trains and calibrates a logistic regression model to predict NBA All-Star selections.
        query = TEST_DATA
        threshold is set to 0.35
        C is set to 100 (minimal regularization)
        returns = calibrated model that was found from all_star_model_analysis with test_data
    """
    #read testing data
    query = QUERIES.get(query, None)
    df = pd.read_sql(query, engine)

    # X = df[['GS percentile group','Age','won ALLSTAR','pre_win_precentage','PTS percentile group','TRB percentile group','AST percentile group','STL percentile group','BLK percentile group','TOV percentile group'
    #     ,'won MVP','won DPOY','won MIP']].copy() #model 1

    X = df[["eFG% percentile","GS percentile", "Pos", "3P%", "FT%", "PTS percentile", "AST percentile", "TRB percentile", "STL percentile",
            "BLK percentile", "TOV percentile", "won MVP", "won DPOY", "pre_win_precentage",
            "won ALLSTAR"]].copy()  # model 2

    y = df['this_season_ALLSTAR'].astype(int)

    # convert bool to int for initial bool columns
    bool_cols = X.select_dtypes(include='bool').columns
    X[bool_cols] = X[bool_cols].astype(int)  # true and false to 1 and 0

    # hot encodes cat vars (0 and 1)
    cat_cols = X.select_dtypes(include='object').columns
    X = pd.get_dummies(X, columns=cat_cols, drop_first=True)

    # Convert bool to int
    bool_cols = X.select_dtypes(include='bool').columns
    X[bool_cols] = X[bool_cols].astype(int)

    # drops any na rows
    X = X.dropna()

    # realigns X with y
    y = y.loc[X.index]
    X = sm.add_constant(X)

    # Model training
    model = LogisticRegression(penalty='l2', solver='liblinear', class_weight='balanced', C=C)
    model.fit(X, y)

    # Calibration
    calibrated_model = CalibratedClassifierCV(model, method='isotonic', cv='prefit')
    calibrated_model.fit(X, y)

    return calibrated_model, X, y, threshold


def prediction_data(query):
    """
    preparing data for prediction
    """
    query = QUERIES.get(query, None)
    df = pd.read_sql(query, engine)
    # X = df[['GS percentile group','Age','won ALLSTAR','pre_win_precentage','PTS percentile group','TRB percentile group','AST percentile group','STL percentile group','BLK percentile group','TOV percentile group'
    #     ,'won MVP','won DPOY','won MIP']].copy() #model 1

    X = df[["eFG% percentile","GS percentile", "Pos", "3P%", "FT%", "PTS percentile", "AST percentile", "TRB percentile", "STL percentile",
            "BLK percentile", "TOV percentile", "won MVP", "won DPOY", "pre_win_precentage",
            "won ALLSTAR"]].copy()  # model 2

    y = df['this_season_ALLSTAR'].astype(int)

    # convert bool to int for initial bool columns
    bool_cols = X.select_dtypes(include='bool').columns
    X[bool_cols] = X[bool_cols].astype(int)  # true and false to 1 and 0

    # hot encodes cat vars (0 and 1)
    cat_cols = X.select_dtypes(include='object').columns
    X = pd.get_dummies(X, columns=cat_cols, drop_first=True)

    # Convert bool to int
    bool_cols = X.select_dtypes(include='bool').columns
    X[bool_cols] = X[bool_cols].astype(int)

    # drops any na rows
    # X = X.dropna()  #for groups
    X = X.fillna(0)  # for numeric

    # realigns X with y
    y = y.loc[X.index]
    X = sm.add_constant(X)

    # Align columns of X_test to X_train to avoid mismatch (important!)
    X_test = X.reindex(columns=X.columns, fill_value=0)


    calibrated_model, X_train, y_train, threshold = all_star_model("TRAINING_DATA")
    # Predict probabilities for positive class
    y_proba_test = calibrated_model.predict_proba(X_test)[:, 1]

    # Apply your threshold to get predicted classes
    y_pred_test = (y_proba_test >= threshold).astype(int)

    results_df = df.copy()  # keep original test data
    results_df['y_proba'] = y_proba_test
    results_df['y_pred'] = y_pred_test


    results_df.to_csv('prediction_results',index=False)
    results_df.to_sql('prediction_results', con=engine, if_exists='replace', index=False)
    print('Results saved to csv and database')
# prediction_data("PRED_DATA")