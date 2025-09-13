import os
os.environ["HADOOP_HOME"] = "C:/hadoop"   # path where you create bin/winutils.exe
os.environ["hadoop.home.dir"] = "C:/hadoop"
os.environ["ARROW_PRE_0_15_IPC_FORMAT"] = "1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

spark = (
    SparkSession.builder
    .appName("JobETL")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    .getOrCreate()
)

def main():
    # 1️⃣ Start Spark session
    spark = (
        SparkSession.builder
        .appName("JobETL")
        .getOrCreate()
    )

    # 2️⃣ Load raw JSON (set multiline to true for Adzuna format)
    raw_df = (
        spark.read
        .option("multiline", "true")
        .json("data/jobs_raw.json")
    )

    print("📂 Raw schema:")
    raw_df.printSchema()

    # 3️⃣ Flatten the JSON structure (Adzuna wraps jobs in 'results')
    jobs_df = raw_df.select(explode(col("results")).alias("job"))

    # 4️⃣ Select useful curated columns
    curated_df = jobs_df.select(
        col("job.id").alias("job_id"),
        col("job.title").alias("title"),
        col("job.company.display_name").alias("company"),
        col("job.location.display_name").alias("location"),
        col("job.salary_min").alias("salary_min"),
        col("job.salary_max").alias("salary_max"),
        col("job.created").alias("created_date"),
        col("job.category.label").alias("category"),
        col("job.redirect_url").alias("url")
    )

    print("📊 Curated schema:")
    curated_df.printSchema()
    curated_df.show(5, truncate=False)

    # 5️⃣ Save curated data as Parquet (overwrite mode for testing)
    curated_df.write.mode("overwrite").parquet("data/jobs_curated.parquet")

    print("✅ ETL complete! Curated data saved to data/jobs_curated.parquet")
# 6️⃣ Read back curated Parquet (to verify)
    check_df = spark.read.parquet("data/jobs_curated.parquet")
    check_df.show(5, truncate=False)

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
