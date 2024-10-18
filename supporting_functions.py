from merge_and_clean_data import merge_and_clean_data
import math
import category_encoders as ce
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error


def train_test_split():
    merged_df = merge_and_clean_data()
    # merged_df['start_time_gmt'] = merged_df['start_time_gmt'].dt.date()
    # latest_date = max(merged_df.loc[:,'start_time_gmt'], key=lambda d: datetime.strptime(str(d), '%m/%d/%Y'))
    latest_date = max(merged_df["start_time_gmt"].dt.date)
    print(f"latest date : {latest_date}")
    train_df = merged_df[(merged_df["finish_time_gmt"].dt.date < latest_date)]
    test_df = merged_df[(merged_df["start_time_gmt"].dt.date == latest_date)]
    print(f"train_df shape : {train_df.shape}")
    print(f"test_df shape : {test_df.shape}")

    return train_df, test_df


def perform_binary_encoding(df1, df2):
    obj_features = list(df1.loc[:, df1.dtypes == "object"].columns.values)
    encoder = ce.BinaryEncoder(cols=obj_features, return_df=True)
    train_encoded = encoder.fit_transform(df1)
    test_encoded = encoder.transform(df2)

    return train_encoded, test_encoded


def model_performance(y_test, predictions):
    rmse = math.sqrt(mean_squared_error(y_test, predictions))
    r2 = r2_score(y_test, predictions)
    mae = mean_absolute_error(y_test, predictions)
    # print('................For Test data..............')

    return r2, rmse, mae


def change_data_type(df):
    df["user_name"] = df["user_name"].astype(object)
    df["user_group"] = df["user_group"].astype(object)
    df["queue_name"] = df["queue_name"].astype(object)
    df["mem_req"] = df["mem_req"].astype(float)
    df["num_slots"] = df["num_slots"].astype(int)
    df["tool_extracted"] = df["tool_extracted"].astype(object)
    df["cluster_name"] = df["cluster_name"].astype(object)
    df["project_name"] = df["project_name"].astype(object)
    df["user_skills"] = df["user_skills"].astype(object)
    df["ifrs_type"] = df["ifrs_type"].astype(object)
    df["STDEV_runtime"] = df["STDEV_runtime"].astype(float)
    df["technology"] = df["technology"].astype(object)
    df["pf_level_6"] = df["pf_level_6"].astype(object)
    df["project_life_cycle"] = df["project_life_cycle"].astype(object)
    df["project_mag_code"] = df["project_mag_code"].astype(object)
    df["milestone_code_mapping"] = df["milestone_code_mapping"].astype(object)
    df["moving_average"] = df["moving_average"].astype(float)
    df["target_runtime"] = df["target_runtime"].astype(float)
    return df


def splitting_input_and_target_variables(train_df, test_df):
    used_features = [
        "user_name",
        "user_group",
        "queue_name",
        "mem_req",
        "num_slots",
        "tool_extracted",
        "cluster_name",
        "project_name",
        "user_skills",
        "ifrs_type",
        "STDEV_runtime",
        "technology",
        "pf_level_6",
        "project_life_cycle",
        "project_mag_code",
        "milestone_code_mapping",
        "moving_average",
        "target_runtime",
    ]

    train_df = train_df[used_features]
    test_df = test_df[used_features]

    train_df = change_data_type(train_df)
    test_df = change_data_type(test_df)

    X_train = train_df.drop(columns=["target_runtime"])
    y_train = train_df["target_runtime"].astype(float)

    X_test = test_df.drop(columns=["target_runtime"])
    y_test = test_df["target_runtime"].astype(float)

    return X_train, y_train, X_test, y_test
