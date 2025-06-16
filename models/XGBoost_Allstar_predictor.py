import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from statsmodels.stats.outliers_influence import variance_inflation_factor
from config import DB_CONFIG
from sklearn.model_selection import train_test_split
import statsmodels.api as sm
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score,f1_score,precision_score, recall_score,accuracy_score
from sklearn.model_selection import GridSearchCV
from sklearn.calibration import calibration_curve,CalibratedClassifierCV
from sklearn.model_selection import StratifiedKFold
import numpy as np
from xgboost import XGBClassifier



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

    "LOG_R_DATA":
        """
        SELECT * FROM raw_logr_allstar_data
        """,

        "TRAINING_DATA_2":
        """
        SELECT * FROM raw_logr_allstar_data
            where season < 2023 
         and season > 1950
        """,

        'PRED_DATA_2':
        """
        SELECT * from raw_logr_allstar_data
        where season >= 2023
        """,
}
def all_star_model(query):
    query = QUERIES.get(query, None)
    df = pd.read_sql(query, engine)

    X = df[["Age", "GS percentile", "Pos", "PTS percentile", "AST percentile", "TRB percentile",
            "BLK percentile", "TOV percentile", "PER percentile", "pre_win_precentage", "num_DPOY_selections_before",
            "num_ALLSTAR_selections_before", 'num_MIP_selections_before']]

    y = df['this_season_ALLSTAR'].astype(int)

    # Convert bool columns to int if any
    bool_cols = X.select_dtypes(include='bool').columns
    X[bool_cols] = X[bool_cols].astype(int)

    # convert categorical variables to dummies
    cat_cols = X.select_dtypes(include='object').columns
    X = pd.get_dummies(X, columns=cat_cols, drop_first=True)

    # convert any bool dummies to int just in case
    bool_cols = X.select_dtypes(include='bool').columns
    X[bool_cols] = X[bool_cols].astype(int)

    # Drop rows with NA values
    X = X.dropna()

    # Realign y with X
    y = y.loc[X.index]

    #setting up test and train data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y)

    # calculate imbalance ratio
    POS = sum(y_train == 1)
    NEG = sum(y_train == 0)
    scale_pos_weight = NEG / POS

    model_xgb = XGBClassifier(
        eval_metric='logloss',
        scale_pos_weight=scale_pos_weight,
        random_state=42,
    )
    model_xgb.fit(X_train, y_train)

    y_probs_xgb = model_xgb.predict_proba(X_test)[:, 1]
    y_preds_xgb = model_xgb.predict(X_test)
    print("Base XGBoost Performance:")
    print(classification_report(y_test, y_preds_xgb))
    print(confusion_matrix(y_test, y_preds_xgb))
    print("ROC AUC:", roc_auc_score(y_test, y_probs_xgb))

    # --- Part 2: XGBoost Grid Search ---
    param_grid = {
        'n_estimators': [50, 100, 200],
        'max_depth': [3, 5, 7],
        'learning_rate': [0.01, 0.1, 0.3],
        'scale_pos_weight': [scale_pos_weight],  # fixed to address imbalance
        'reg_alpha': [0, 0.1, 1],
        'reg_lambda': [1, 5, 10]

    }

    grid = GridSearchCV(
        XGBClassifier(eval_metric='logloss', random_state=42),
        param_grid,
        scoring='f1',
        cv=3,
        verbose=1
    )
    grid.fit(X_train, y_train)

    best_model = grid.best_estimator_
    print("Best parameters:", grid.best_params_)
    #M1 parameters: {'learning_rate': 0.1, 'max_depth': 7, 'n_estimators': 200, 'scale_pos_weight': 11.081944444444444}
    #M2 parameters: {'learning_rate': 0.3, 'max_depth': 8, 'n_estimators': 300, 'scale_pos_weight': 11.081944444444444}
    #M3 parameters: {'learning_rate': 0.3, 'max_depth': 7, 'n_estimators': 100, 'reg_alpha': 0.1, 'reg_lambda': 10, 'scale_pos_weight': 11.081944444444444}
    final_model = XGBClassifier(
        learning_rate=0.3,
        max_depth=7,
        n_estimators=100,
        scale_pos_weight=11.081944444444444,
        eval_metric='logloss',
        random_state= 42,
        reg_lambda = 10,
        reg_alpha = 0.1
    )
    final_model.fit(X_train, y_train)

    # Predict class labels and probabilities
    y_probs = final_model.predict_proba(X_test)[:, 1]  # Probabilities for class 1
    y_preds = final_model.predict(X_test)  # Default threshold of 0.5

    print("Accuracy:", accuracy_score(y_test, y_preds))
    print("Precision:", precision_score(y_test, y_preds))
    print("Recall:", recall_score(y_test, y_preds))
    print("F1 Score:", f1_score(y_test, y_preds))
    print("ROC AUC Score:", roc_auc_score(y_test, y_probs))
    print("Confusion Matrix:\n", confusion_matrix(y_test, y_preds))
    print("Classification Report:\n", classification_report(y_test, y_preds))

    thresholds = [0.5, 0.45, 0.4, 0.35, 0.3, 0.25]
    for t in thresholds:
        y_pred_t = (y_probs >= t).astype(int)
        precision = precision_score(y_test, y_pred_t)
        recall = recall_score(y_test, y_pred_t)
        f1 = f1_score(y_test, y_pred_t)
        cm = confusion_matrix(y_test, y_pred_t)
        print(
            f"Threshold: {t:.2f}, Recall: {recall:.3f}, Precision: {precision:.3f}, F1: {f1:.3f}, FN: {cm[1, 0]}, FP: {cm[0, 1]}")


all_star_model("TRAINING_DATA_2")