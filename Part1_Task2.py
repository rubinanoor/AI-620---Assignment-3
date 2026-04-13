
import json
import os
import pandas as pd
import great_expectations as gx
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import FileDataContext

CLEAN_CSV     = "clean_synthetic_dataset.csv"
CORRUPTED_CSV = "corrupted_synthetic_dataset.csv"
GE_ROOT_DIR   = "ge_project_synthetic"   


context = FileDataContext.create(project_root_dir=GE_ROOT_DIR)
context.build_data_docs()
print(context.get_docs_sites_urls())


# to prevent errors when we re run the script
try:
    context.add_datasource(
        "synthetic_datasource",
        class_name="Datasource",
        execution_engine={"class_name": "PandasExecutionEngine"},
        data_connectors={
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["batch_id"],
            },
        },
    )
except Exception:
    # Datasource might already exist in this session
    pass

# defining the expectation suite
SUITE_NAME = "synthetic_car_suite"



# delete any previous suite 
try:
    suite = ExpectationSuite(expectation_suite_name=SUITE_NAME)
    context.add_expectation_suite(expectation_suite=suite)
except Exception:
    # If it exists, just get the existing one
    suite = context.get_expectation_suite(SUITE_NAME)


# defining expectations against the clean dataset
clean_df = pd.read_csv(CLEAN_CSV)

batch_request = RuntimeBatchRequest(
    datasource_name="synthetic_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="clean_cars",  # This is a name you choose for the batch
    runtime_parameters={"batch_data": clean_df},
    batch_identifiers={"batch_id": "initial_validation"},
)

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=SUITE_NAME,
)
# column existence 
for col in ["addref","city","year","engine","mileage","body",
            "make","fuel","transmission","price"]:
    validator.expect_column_to_exist(col)

# completeness for no null values
for col in ["addref","engine","mileage","transmission","price","year"]:
    validator.expect_column_values_to_not_be_null(col)

# uniqueness 
validator.expect_column_values_to_be_unique("addref")

validator.expect_column_values_to_be_between(
    column="year",
    min_value=1970,
    max_value=2024,   
    mostly=0.98,      
)

validator.expect_column_values_to_be_between(
    column="engine",
    min_value=600,
    max_value=6500,
    mostly=0.98,
)

validator.expect_column_values_to_be_between(
    column="mileage",
    min_value=0,
    max_value=500_000,
    mostly=0.99,
)

validator.expect_column_values_to_be_between(
    column="price",
    min_value=100_000,
    max_value=100_000_000,
    mostly=0.98,
)

validator.expect_column_values_to_be_in_set(
    column="transmission",
    value_set=["Manual", "Automatic"],
)

validator.expect_column_values_to_be_in_set(
    column="fuel",
    value_set=["Petrol", "Diesel", "Hybrid", "CNG"],
)

# distributional check 
validator.expect_column_proportion_of_unique_values_to_be_between(
    column="transmission",
    min_value=0.01,
    max_value=0.5,
)
final_suite = validator.get_expectation_suite(discard_failed_expectations=False)

# 2. Force-save it to the JSON file
context.save_expectation_suite(final_suite, SUITE_NAME)

print(f" SUCCESS: Saved {len(final_suite.expectations)} synthetic rules to disk.")

def print_summary(result, label: str):
    print(f"\n{'='*60}")
    print(f"  VALIDATION SUMMARY — {label}")
    print(f"{'='*60}")
    # GX 0.x stores results in run_results
    stats = result.run_results
    for key, run in stats.items():
        vr = run["validation_result"]
        total    = vr.statistics["evaluated_expectations"]
        passed   = vr.statistics["successful_expectations"]
        failed   = vr.statistics["unsuccessful_expectations"]
        pct      = vr.statistics["success_percent"]
        print(f"  Total expectations : {total}")
        print(f"  Passed             : {passed}")
        print(f"  Failed             : {failed}")
        print(f"  Success rate       : {pct:.1f}%")
        if failed > 0:
            print(f"\n   Failed expectations:")
            for er in vr.results:
                if not er.success:
                    exp_type = er.expectation_config.expectation_type
                    col      = er.expectation_config.kwargs.get("column","—")
                    print(f"     • {exp_type}  [column: {col}]")


# validating both datasets and building reports
def validate_dataset(df: pd.DataFrame, batch_id: str, asset_name: str):
    # 1. Define the batch request
    batch_request = {
        "datasource_name": "synthetic_datasource",
        "data_connector_name": "default_runtime_data_connector_name",
        "data_asset_name": asset_name,
        "runtime_parameters": {"batch_data": df},
        "batch_identifiers": {"batch_id": batch_id},
    }

    # 2. Initialize the Checkpoint object
    checkpoint = SimpleCheckpoint(
        name=f"checkpoint_{batch_id}",
        data_context=context,
    )

    # 3. Pass the validations and actions directly to the run method
    results = checkpoint.run(
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": SUITE_NAME,
            }
        ],
        action_list=[
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction"},
            }
        ],
    )
    
    return results

 
# 2. NOW call the validations and the summary
print("\n🔍 Validating CLEAN dataset …")
clean_result = validate_dataset(clean_df, "clean", "clean_cars")
print(f"   Overall pass: {clean_result.success}")

print("\n🔍 Validating CORRUPTED dataset …")
corrupted_df = pd.read_csv(CORRUPTED_CSV)
corrupted_result = validate_dataset(corrupted_df, "corrupted", "corrupted_cars")
print(f"   Overall pass: {corrupted_result.success}")

# 3. Trigger the prints
print_summary(clean_result, "CLEAN dataset")
print_summary(corrupted_result, "CORRUPTED dataset")

# Capture the rules from the validator
final_suite = validator.get_expectation_suite(discard_failed_expectations=False)
suite_dict = final_suite.to_json_dict()

# Define the path to your SYNTHETIC project JSON
target_path = "/Users/BABARHUSSAIN/Desktop/Study Material/MS AI/Data Engineering/Assignment 3/ge_project_synthetic/gx/expectations/synthetic_car_suite.json"

# Manually write the file
with open(target_path, "w") as f:
    json.dump(suite_dict, f, indent=2)

print(f" FORCE SAVE COMPLETE: {len(final_suite.expectations)} synthetic rules written to disk.")