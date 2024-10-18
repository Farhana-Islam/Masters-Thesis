import lightgbm
import numpy as np
from supporting_functions import model_performance, perform_binary_encoding, splitting_input_and_target_variables


def return_df_with_tool_shortq(train_df, test_df):
    df_train_shortq = train_df[train_df["queue_name"] == "shortq"]
    df_test_shortq = test_df[test_df["queue_name"] == "shortq"]
    return df_train_shortq, df_test_shortq


def model_training_LightGBM(X_train, y_train, X_test):
    model = lightgbm.LGBMRegressor(n_estimators=50, boosting_type="dart")

    model.fit(X_train, y_train)

    # Making predictions on the test set
    predictions = model.predict(X_test)
    return predictions


def lightgbm_implementation_shortq(X_train, y_train, X_test, y_test):
    print("Data encoding........")
    X_train, X_test = perform_binary_encoding(X_train, X_test)
    print("model training for mediumq jobs........")

    predictions = model_training_LightGBM(X_train, y_train, X_test)

    predictions = np.asarray(predictions)

    r2, rmse, mae = model_performance(y_test, predictions)

    return r2, rmse, mae


def return_result_metrics_for_shortq_lightgbm(train_df, test_df):
    df_train_shortq, df_test_shortq = return_df_with_tool_shortq(train_df, test_df)
    print("data shape for mediumq.....")
    print(f"train_df shape : {df_train_shortq.shape}")
    print(f"test_df shape : {df_test_shortq.shape}")
    X_train, y_train, X_test, y_test = splitting_input_and_target_variables(df_train_shortq, df_test_shortq)
    r2, rmse, mae = lightgbm_implementation_shortq(X_train, y_train, X_test, y_test)
    return r2, rmse, mae
