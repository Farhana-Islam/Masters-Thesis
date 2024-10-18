from preprocess_IAM import pa_join_IAM
from preprocess_sci import pa_join_sci
from preprocess_TR import pa_join_tr
import pandas as pd


#  Get data
print("Fetching Data...")
pa_join_tr = pa_join_tr()

pa_join_sci = pa_join_sci()

pa_join_IAM = pa_join_IAM()

print("Merging and cleaning Data...")


def clean_df(df):
    # df.loc["mem_req"].fillna(0, inplace = True)
    df["STDEV_runtime"] = df["STDEV_runtime"].fillna(0)
    df["user_skills"] = df["user_skills"].fillna("Not_mentioned")
    df["user_name"] = df["user_name"].fillna("Not_mentioned")
    df["user_group"] = df["user_group"].fillna("Not_mentioned")
    df["queue_name"] = df["queue_name"].fillna("Not_mentioned")
    df["mem_req"] = df["mem_req"].fillna(0)
    df["num_slots"] = df["num_slots"].fillna(1)
    df["ifrs_type"] = df["ifrs_type"].fillna("Not_mentioned")
    df["technology"] = df["technology"].fillna("Not_mentioned")
    df["pf_level_6"] = df["pf_level_6"].fillna("Not_mentioned")
    df["project_life_cycle"] = df["project_life_cycle"].fillna("Not_mentioned")
    df["project_mag_code"] = df["project_mag_code"].fillna("Not_mentioned")
    df["milestone_code_mapping"] = df["milestone_code_mapping"].fillna("Not_mentioned")
    # df["job_class"] = df["job_class"].fillna('Not_mentioned')
    return df


def merge_and_clean_data():
    merged_df = pd.merge(pa_join_sci, pa_join_IAM, on="job_description", how="left")
    merged_df = pd.merge(merged_df, pa_join_tr, on="job_description", how="left")
    merged_df.drop(
        [
            "row_rank",
            "project_name_y",
            "user_name_y",
            "row_sha2_y",
            "project_name",
            "user_name",
            "job_cmd_y",
            "row_sha2",
            "milestone_rank",
        ],
        axis=1,
        inplace=True,
    )
    merged_df.rename(
        columns={
            "project_name_x": "project_name",
            "user_name_x": "user_name",
            "job_cmd_x": "job_cmd",
            "row_sha2_x": "row_sha2",
        },
        inplace=True,
    )
    merged_df = merged_df[merged_df["target_runtime"] <= 2419200]
    clean_df(merged_df)
    return merged_df
