# analysis/explore.py
import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 120)

INPUT_PARQUET = "data/jobs_curated.parquet"
CLEAN_CSV = "data/jobs_curated_clean.csv"

def main():
    print("🔁 Reading parquet:", INPUT_PARQUET)
    try:
        df = pd.read_parquet(INPUT_PARQUET)
    except Exception as e:
        print("❗ Failed to read parquet. Do you have pyarrow installed? Error:", e)
        raise

    print("\n📐 Shape (rows, cols):", df.shape)
    print("\n🔎 Sample rows:")
    print(df.head(5).to_string(index=False))

    # Basic data quality checks
    print("\n🧪 Missing values per column:")
    print(df.isnull().sum())

    # Duplicate check
    if "job_id" in df.columns:
        dup_count = df.duplicated(subset=["job_id"]).sum()
        print(f"\n🔁 Duplicate job_id count: {dup_count}")
    else:
        print("\n⚠️ job_id column not present — skipping duplicate check")

    # Salary sanity
    if {"salary_min", "salary_max"}.issubset(df.columns):
        bad_salary = (df["salary_min"] > df["salary_max"]).sum()
        print(f"\n💸 Records where salary_min > salary_max: {bad_salary}")
        print("\n💰 Salary stats:")
        print(df[["salary_min", "salary_max"]].describe())
    else:
        print("\n⚠️ salary_min / salary_max columns not present — skipping salary checks")

    # Top companies & categories
    if "company" in df.columns:
        print("\n🏢 Top companies by count:")
        print(df["company"].value_counts().head(10).to_string())
    if "category" in df.columns:
        print("\n📚 Category counts:")
        print(df["category"].value_counts().head(20).to_string())

    # Parse created_date
    if "created_date" in df.columns:
        df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")
        print("\n📅 Date range after parsing created_date:")
        print(df["created_date"].min(), "->", df["created_date"].max())

    # Optional: basic enrichment example (salary band)
    def salary_band(row):
        sm = row.get("salary_min") if pd.notnull(row.get("salary_min")) else row.get("salary_max")
        if pd.isnull(sm):
            return None
        if sm < 50000:
            return "Low"
        if sm < 120000:
            return "Medium"
        return "High"
    df["salary_band"] = df.apply(salary_band, axis=1)

    # Save cleaned CSV for Streamlit (easy to load)
    df.to_csv(CLEAN_CSV, index=False)
    print(f"\n✅ Cleaned CSV written to: {CLEAN_CSV}")

if __name__ == "__main__":
    main()
