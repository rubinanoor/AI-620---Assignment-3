import os
import json
import pandas as pd
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.data_context import FileDataContext

df = pd.read_csv("data-a2.csv", low_memory=False)

print(f"Dataset shape: {df.shape}")
print(f"\nColumn dtypes:\n{df.dtypes}")
print(f"\nMissing values:\n{df.isnull().sum()}")
print(f"\nSample rows:\n{df.head(3)}")

# setting up GE project directory
GE_ROOT_DIR  = "ge_project_pakwheels"
SUITE_NAME   = "pakwheels_car_suite"

GE_ROOT_DIR = "ge_project_pakwheels"

if not os.path.exists(GE_ROOT_DIR):
    context = FileDataContext.create(project_root_dir=GE_ROOT_DIR)
else:
    context = gx.get_context(context_root_dir=GE_ROOT_DIR)


context.add_or_update_datasource(**{
    "name": "pakwheels_datasource",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "PandasExecutionEngine"},
    "data_connectors": {
        "runtime_connector": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["batch_id"],
        }
    },
})


try:
    context.delete_expectation_suite(SUITE_NAME)
except Exception:
    pass

suite = context.add_expectation_suite(expectation_suite_name=SUITE_NAME)

validator = context.get_validator(
    batch_request=RuntimeBatchRequest(
        datasource_name="pakwheels_datasource",
        data_connector_name="runtime_connector",
        data_asset_name="pakwheels_data",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"batch_id": "pakwheels_run"},
    ),
    expectation_suite_name=SUITE_NAME,
)

# all expected columns must be present
for col in ["addref","city","assembly","body","make","model","year",
            "engine","transmission","fuel","color","registered","mileage","price"]:
    validator.expect_column_to_exist(col)

# completeness 
for col in ["addref","city","make","model","transmission","mileage"]:
    validator.expect_column_values_to_not_be_null(col)

for col in ["price","year","engine"]:
    validator.expect_column_values_to_not_be_null(col, mostly=0.90)

# uniqueness 
validator.expect_column_values_to_be_unique("addref")


validator.expect_column_values_to_be_between(
    column="year",
    min_value=1980,
    max_value=2024,
    mostly=0.98,
)

validator.expect_column_values_to_be_between(
    column="engine",
    min_value=660,
    max_value=8000,
    mostly=0.97,
)

validator.expect_column_values_to_be_between(
    column="mileage",
    min_value=0,
    max_value=500_000,
    mostly=0.99,
)

validator.expect_column_values_to_be_between(
    column="price",
    min_value=200_000,
    max_value=165_000_000,
    mostly=0.98,
)

validator.expect_column_values_to_be_in_set(
    column="transmission",
    value_set=["Manual", "Automatic"],
)

validator.expect_column_values_to_be_in_set(
    column="fuel",
    value_set=["Petrol", "Diesel", "Hybrid", "Electric"],
    mostly=0.99,   
)

validator.expect_column_values_to_be_in_set(
    column="assembly",
    value_set=["Local", "Imported"],
    mostly=0.95,  
)

# distributional checks 
validator.expect_column_proportion_of_unique_values_to_be_between(
    column="mileage",
    min_value=0.01,
    max_value=1.0,
)

validator.expect_column_median_to_be_between(
    column="price",
    min_value=1_000_000,
    max_value=5_000_000,
)

validator.expect_column_values_to_be_between(
    column="year",
    min_value=2005,
    max_value=2024,
    mostly=0.70,
    result_format="COMPLETE",
)

# 1. Pull the suite out of the validator
final_suite = validator.get_expectation_suite(discard_failed_expectations=False)

# 2. Add/Update it in the context explicitly
context.add_or_update_expectation_suite(expectation_suite=final_suite)

# 3. Force the physical save to the .json file
context.save_expectation_suite(expectation_suite=final_suite, expectation_suite_name=SUITE_NAME)

print(f"✅ SUCCESSFULLY SAVED {len(final_suite.expectations)} RULES TO DISK.")

# 4. Find where it actually went
import glob
actual_path = glob.glob(f"**/{SUITE_NAME}.json", recursive=True)
if actual_path:
    print(f"📍 FOUND IT! Your rules are actually in: {os.path.abspath(actual_path[0])}")
    with open(actual_path[0], 'r') as f:
        content = json.load(f)
        print(f"📊 Rules inside the file: {len(content.get('expectations', []))}")


# validator.save_expectation_suite(discard_failed_expectations=False)
# print(f"\n✅ Expectation suite '{SUITE_NAME}' saved with all rules.")
print(f"✅ SUCCESSFULLY SAVED {len(final_suite.expectations)} RULES TO DISK.")

run_batch_request = {
    "datasource_name": "pakwheels_datasource",
    "data_connector_name": "runtime_connector",
    "data_asset_name": "pakwheels_data",
    "runtime_parameters": {"batch_data": df},
    "batch_identifiers": {"batch_id": "pakwheels_run"},
}

checkpoint = SimpleCheckpoint(name="pakwheels_checkpoint", data_context=context)

result = checkpoint.run(
    validations=[{"batch_request": run_batch_request, "expectation_suite_name": SUITE_NAME}],
    action_list=[{"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}}]
)
print(f"\n{'='*65}")
print(f"  PAKWHEELS DATASET — VALIDATION SUMMARY")
print(f"{'='*65}")

for key, run in result.run_results.items():
    vr      = run["validation_result"]
    total   = vr.statistics["evaluated_expectations"]
    passed  = vr.statistics["successful_expectations"]
    failed  = vr.statistics["unsuccessful_expectations"]
    pct     = vr.statistics["success_percent"]
    print(f"  Expectations evaluated : {total}")
    print(f"  Passed                 : {passed}")
    print(f"  Failed                 : {failed}")
    print(f"  Success rate           : {pct:.1f}%\n")
    if failed > 0:
        print("  ❌ Failed expectations:")
        for er in vr.results:
            if not er.success:
                etype = er.expectation_config.expectation_type
                col   = er.expectation_config.kwargs.get("column","—")
                obs   = er.result.get("observed_value","N/A")
                print(f"     • [{col}] {etype}")
                print(f"       Observed: {obs}")

print(f"\n📂 HTML report available at your project path:")
report_path = os.path.abspath(os.path.join(GE_ROOT_DIR, "gx/uncommitted/data_docs/local_site/index.html"))
if not os.path.exists(report_path): 
    report_path = os.path.abspath(os.path.join(GE_ROOT_DIR, "uncommitted/data_docs/local_site/index.html"))

print(f"\n📂 Report: {report_path}")

# Force building the Data Docs manually
print("🏗️ Building Data Docs...")
context.build_data_docs()

# Find the specific index file location from the context itself
data_docs_site_info = context.get_docs_sites_urls()[0]
report_url = data_docs_site_info['site_url']

print(f"\n✅ SUCCESS! Your report is actually here:")
print(f"{report_url}")

# This will open it automatically on your Mac
import webbrowser
webbrowser.open(report_url)