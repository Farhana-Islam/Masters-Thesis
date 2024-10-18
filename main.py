from tool_dictionary import tool_extract_train_and_test
from model_training_and_testing import return_result_metrics_for_unified_model
from batchq_catboost import return_result_metrics_for_batchq_catboost
from mediumq_lightgbm import return_result_metrics_for_mediumq_lightgbm
from shortq_lightgbm import return_result_metrics_for_shortq_lightgbm

train_df, test_df = tool_extract_train_and_test()

print("...........Unified model results.............")
r2, rmse, mae = return_result_metrics_for_unified_model(train_df, test_df)
print("R Square : {:.4f}".format(r2))
print("RMSE : {:.4f}".format(rmse))
print(f"MAE: {mae}")

print("...........Queue Wise model results.............")
print(".........Batchq results..........")
r2, rmse, mae = return_result_metrics_for_batchq_catboost(train_df, test_df)
print("R Square : {:.4f}".format(r2))
print("RMSE : {:.4f}".format(rmse))
print(f"MAE: {mae}")


print(".........Mediumq results..........")
r2, rmse, mae = return_result_metrics_for_mediumq_lightgbm(train_df, test_df)
print("R Square : {:.4f}".format(r2))
print("RMSE : {:.4f}".format(rmse))
print(f"MAE: {mae}")

print(".........Shortq results..........")
r2, rmse, mae = return_result_metrics_for_shortq_lightgbm(train_df, test_df)
print("R Square : {:.4f}".format(r2))
print("RMSE : {:.4f}".format(rmse))
print(f"MAE: {mae}")
