import json
import os
import numpy as np
import pandas as pd
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.checkpoint import SimpleCheckpoint

# helper cleaning function
def clean_cars_dataframe(df: pd.DataFrame, dataset_label: str) -> pd.DataFrame:
 
    df = df.copy()
    initial_rows = len(df)
    print(f"\n{'─'*55}")
    print(f"  Cleaning: {dataset_label}  |  Initial rows: {initial_rows:,}")
    print(f"{'─'*55}")

   
    threshold = 0.50
    null_frac  = df.isnull().mean()
    high_null  = null_frac[null_frac > threshold].index.tolist()
    if high_null:
        df.drop(columns=high_null, inplace=True)
        print(f"[1] Dropped columns >50% null: {high_null}")
    else:
        print("[1] No columns exceed 50% null threshold.")

    
    # dropping rows where critical columns are null
    critical_cols = [c for c in ["addref","price","year","mileage"] if c in df.columns]
    before = len(df)
    df.dropna(subset=critical_cols, inplace=True)
    print(f"[2] Dropped {before - len(df):,} rows with null in {critical_cols}")

    # fixing out of range engine values
    if "engine" in df.columns:
        df["engine"] = pd.to_numeric(df["engine"], errors="coerce")
        df.loc[(df["engine"] < 600) | (df["engine"] > 8000), "engine"] = np.nan
        median_val = df["engine"].median()
        df["engine"] = df["engine"].fillna(median_val)
        df["engine"] = df["engine"].fillna(1300) 
        print(f"[3] Cleaned Engine: No nulls remain.")

    if "fuel" in df.columns:

        df["fuel"] = df["fuel"].replace("Unknown", df["fuel"].mode()[0])

    # fixxing out of range mileage
    if "mileage" in df.columns:
        df["mileage"] = pd.to_numeric(df["mileage"], errors="coerce")
        df.loc[(df["mileage"] < 0) | (df["mileage"] > 500000), "mileage"] = np.nan
        df["mileage"] = df["mileage"].fillna(df["mileage"].median()).fillna(0)


    # fixing negative price 
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df.loc[df["price"] <= 0, "price"] = np.nan
        df["price"] = df["price"].fillna(df["price"].median()).fillna(100000)

    # fixing future year 
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce")
        df.loc[df["year"] > 2024, "year"] = np.nan
        fallback_year = 2020
        if not df["year"].mode().empty:
            fallback_year = df["year"].mode()[0]
        df["year"] = df["year"].fillna(fallback_year).astype("Int64")


    # standardise categorical casing
    cat_cols = [c for c in ["transmission","fuel","body","color"] if c in df.columns]
    for col in cat_cols:
        df[col] = df[col].astype(str).str.strip().str.title()
        # Re-set "nan" strings back to NaN (str conversion artefact)
        df[col] = df[col].replace("Nan", np.nan)
    print(f"[7] Standardised casing for: {cat_cols}")

    # removing exact duplicate rows 
    if "addref" in df.columns:
        before = len(df)
        df.drop_duplicates(subset=["addref"], keep="first", inplace=True)
        print(f"[8] Removed {before - len(df):,} duplicate addref rows")

    # filling remaining minor nulls 
    fill_map = {}
    for col in ["fuel","body","color","city","make","model"]:
        if col in df.columns and df[col].isnull().any():
            fill_map[col] = "Unknown"
    if fill_map:
        df.fillna(fill_map, inplace=True)
        print(f"[9] Filled remaining nulls with 'Unknown' in: {list(fill_map.keys())}")

    # reset index
    df.reset_index(drop=True, inplace=True)
    final_rows = len(df)
    print(f"\n✅ Cleaned. Rows: {initial_rows:,} → {final_rows:,} "
          f"(removed {initial_rows - final_rows:,})")
    return df


# cleaning the PakWheels dataset
raw_pakwheels = pd.read_csv("data-a2.csv", low_memory=False)
clean_pakwheels = clean_cars_dataframe(raw_pakwheels, "PakWheels")
clean_pakwheels.to_csv("cleaned_pakwheels.csv", index=False)
print("\n💾 Saved: cleaned_pakwheels.csv")

# cleaning the corrupted synthetic dataset 
raw_corrupted = pd.read_csv("corrupted_synthetic_dataset.csv", low_memory=False)
clean_corrupted = clean_cars_dataframe(raw_corrupted, "Corrupted Synthetic")
clean_corrupted.to_csv("cleaned_corrupted_synthetic.csv", index=False)
print("💾 Saved: cleaned_corrupted_synthetic.csv")


def revalidate(df, suite_name, datasource_name, ge_root_dir, batch_id, asset_name):
    # 1. Use a fresh context
    context = gx.get_context(context_root_dir=ge_root_dir)
    
    # 2. FORCE LOAD the suite from the file system to be 100% sure
    # This bypasses the GX cache which might be stuck on the '0 rules' version
    suite = context.get_expectation_suite(suite_name)
    
    # Check if it's empty and try to reload if it is
    if len(suite.expectations) == 0:
        print(f"⚠️ Warning: Context reports 0 rules for {suite_name}. Attempting deep reload...")
        # This re-syncs the context with the actual files on your hard drive
        context = gx.get_context(context_root_dir=ge_root_dir)
        suite = context.get_expectation_suite(suite_name)

    print(f"✅ Context verified: {len(suite.expectations)} expectations found for {suite_name}")


    context.add_or_update_datasource(
        name=datasource_name,
        class_name="Datasource",
        execution_engine={"class_name": "PandasExecutionEngine"},
        data_connectors={
            "runtime_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["batch_id"],
            }
        },
    )



    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name=datasource_name,
            data_connector_name="runtime_connector",
            data_asset_name=asset_name,
            runtime_parameters={"batch_data": df},
            batch_identifiers={"batch_id": batch_id},
        ),
        expectation_suite=suite,
    )

    result = validator.validate()


    context.build_data_docs()



 
  

    print(f"\n{'='*55}")
    print(f"  RE-VALIDATION — {batch_id.upper()}")
    print(f"{'='*55}")
    
    s = result.statistics
    total = s.get("evaluated_expectations", 0)
    passed = s.get("successful_expectations", 0)
    failed = s.get("unsuccessful_expectations", 0)
    # Handle the NoneType/Zero case for the percentage
    pct = s.get("success_percent", 0.0) if s.get("success_percent") is not None else 0.0

    print(f"  Total Expectations : {total}")
    print(f"  Passed             : {passed}")
    print(f"  Failed             : {failed}")
    print(f"  Success Rate       : {pct:.1f}%")
    
    if failed > 0:
        print("\n  ❌ Remaining Issues:")
        for res in result.results:
            if not res.success:
                col = res.expectation_config.kwargs.get("column","—")
                etype = res.expectation_config.expectation_type
                print(f"     • [{col}] {etype}")
    
    return result


# Run re-validation using the GE suites created in Tasks 2 and 3
# NOTE: These suite directories must exist (run Tasks 2 & 3 first).

print("\n\n🔁 Re-validating cleaned PakWheels dataset …")
print("\n\n🔁 Re-validating cleaned PakWheels dataset …")
revalidate(
    df=clean_pakwheels,
    suite_name="pakwheels_car_suite",
    datasource_name="pakwheels_datasource_v2",
    # Point exactly to where the gx folder is
    ge_root_dir="ge_project_pakwheels/gx", 
    batch_id="cleaned_pakwheels",
    asset_name="cleaned_pakwheels",
)

print("\n🔁 Re-validating cleaned corrupted synthetic dataset …")
revalidate(
    df=clean_corrupted,
    suite_name="synthetic_car_suite",
    datasource_name="synthetic_datasource_v2",
    # Point exactly to where the gx folder is
    ge_root_dir="ge_project_synthetic/gx", 
    batch_id="cleaned_corrupted",
    asset_name="cleaned_corrupted",
)
