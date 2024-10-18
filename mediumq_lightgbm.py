import lightgbm
import numpy as np
from supporting_functions import model_performance, perform_binary_encoding, splitting_input_and_target_variables


def return_df_with_tool_mediumq(train_df, test_df):
    df_train_mediumq = train_df[train_df["queue_name"] == "mediumq"]
    df_test_mediumq = test_df[test_df["queue_name"] == "mediumq"]
    return df_train_mediumq, df_test_mediumq


def model_training_LightGBM(X_train, y_train, X_test):
    model = lightgbm.LGBMRegressor(n_estimators=50, boosting_type="dart")

    model.fit(X_train, y_train)

    # Making predictions on the test set
    predictions = model.predict(X_test)
    return predictions


def lightgbm_implementation_mediumq(X_train, y_train, X_test, y_test):
    print("Data encoding........")
    X_train, X_test = perform_binary_encoding(X_train, X_test)
    print("model training for mediumq jobs........")

    # log transforming the target variable, adding 1 second extra
    y_train = np.log(y_train.astype(float) + 1)

    predictions = model_training_LightGBM(X_train, y_train, X_test)

    predictions = np.asarray(predictions)

    # reverse transform to original scale of the target variable
    pred_reverse_log = np.exp(predictions.astype(float))

    r2, rmse, mae = model_performance(y_test, pred_reverse_log)

    return r2, rmse, mae


def return_result_metrics_for_mediumq_lightgbm(train_df, test_df):
    df_train_mediumq, df_test_mediumq = return_df_with_tool_mediumq(train_df, test_df)
    print("data shape for mediumq.....")
    print(f"train_df shape : {df_train_mediumq.shape}")
    print(f"test_df shape : {df_test_mediumq.shape}")
    X_train, y_train, X_test, y_test = splitting_input_and_target_variables(df_train_mediumq, df_test_mediumq)
    r2, rmse, mae = lightgbm_implementation_mediumq(X_train, y_train, X_test, y_test)
    return r2, rmse, mae
