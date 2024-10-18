import numpy as np
from catboost import CatBoostRegressor, Pool
from supporting_functions import model_performance, splitting_input_and_target_variables


def return_df_with_tool_batchq(train_df, test_df):
    df_train_batchq = train_df[(train_df["queue_name"] != "shortq") & (train_df["queue_name"] != "mediumq")]
    df_test_batchq = test_df[(test_df["queue_name"] != "shortq") & (test_df["queue_name"] != "mediumq")]

    return df_train_batchq, df_test_batchq


def model_training_CatBoost(pool_train, pool_test):
    cbr = CatBoostRegressor(iterations=100, learning_rate=0.05)
    cbr.fit(pool_train)

    predictions = cbr.predict(pool_test)

    return predictions


def catboost_implementation(X_train, y_train, X_test, y_test):
    # log transforming the target variable
    y_train = np.log(y_train.astype(float) + 1)

    print("Creating pool object for CatBoost algorithm....")
    obj_features = list(X_train.loc[:, X_train.dtypes == "object"].columns.values)
    pool_train = Pool(X_train, y_train, cat_features=obj_features)
    pool_test = Pool(X_test, cat_features=obj_features)

    print("model training for batchq jobs........")

    pred = model_training_CatBoost(pool_train, pool_test)
    print(type(pred), len(pred), pred)

    predictions = np.asarray(pred)
    print(type(predictions), predictions)

    # reverse transform to original scale of the target variable
    pred_reverse_log = np.exp(predictions.astype(float))

    r2, rmse, mae = model_performance(y_test, pred_reverse_log)

    return r2, rmse, mae


def return_result_metrics_for_batchq_catboost(train_df, test_df):
    df_train_batchq, df_test_batchq = return_df_with_tool_batchq(train_df, test_df)
    print("data shape for batchq.....")
    print(f"train_df shape : {df_train_batchq.shape}")
    print(f"test_df shape : {df_test_batchq.shape}")
    X_train, y_train, X_test, y_test = splitting_input_and_target_variables(df_train_batchq, df_test_batchq)
    r2, rmse, mae = catboost_implementation(X_train, y_train, X_test, y_test)
    return r2, rmse, mae
