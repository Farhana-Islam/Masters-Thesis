from sklearn.tree import DecisionTreeRegressor
import numpy as np
from supporting_functions import model_performance, perform_binary_encoding, splitting_input_and_target_variables


def model_training_Unified_DT(X_train, y_train, X_test):
    # Creating an XGBoost regressor
    model = DecisionTreeRegressor(max_depth=10, min_samples_leaf=50)

    model.fit(X_train, y_train)

    # Making predictions on the test set
    predictions = model.predict(X_test)

    return predictions


def encoding_training_and_prediction(X_train, y_train, X_test, y_test):
    print("Data encoding........")
    X_train, X_test = perform_binary_encoding(X_train, X_test)
    print("model training........")

    # log transforming the target variable
    y_train = np.log(y_train.astype(float) + 1)
    predictions = model_training_Unified_DT(X_train, y_train, X_test)

    # reverse transform to original scale of the target variable
    pred_reverse_log = np.exp(predictions.astype(float))

    r2, rmse, mae = model_performance(y_test, pred_reverse_log)

    return r2, rmse, mae


def return_result_metrics_for_unified_model(train_df, test_df):
    X_train, y_train, X_test, y_test = splitting_input_and_target_variables(train_df, test_df)
    r2, rmse, mae = encoding_training_and_prediction(X_train, y_train, X_test, y_test)
    return r2, rmse, mae
