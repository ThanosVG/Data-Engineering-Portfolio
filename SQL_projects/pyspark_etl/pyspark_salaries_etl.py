from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, when, lit
import os
import shutil

# ────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────
INPUT_CSV        = "salaries.csv"
OUTPUT_FOLDER    = "processed_salaries_parquet"
BONUS_MAX_PCT    = 0.30

# Clean previous run
if os.path.exists(OUTPUT_FOLDER):
    print(f"Removing previous output: {OUTPUT_FOLDER}")
    shutil.rmtree(OUTPUT_FOLDER)

# ────────────────────────────────────────────────
# START SPARK
# ────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Salaries ETL with Random Bonus") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ────────────────────────────────────────────────
# 1. EXTRACT - Read CSV (NO inferSchema)
# ────────────────────────────────────────────────
print("Reading CSV...")
df = spark.read.csv(INPUT_CSV, header=True, inferSchema=False)

print("Original schema (all string):")
df.printSchema()

# ────────────────────────────────────────────────
# Cast important columns manually
# ────────────────────────────────────────────────
from pyspark.sql.types import IntegerType, DoubleType, StringType

df = df.withColumn("work_year", col("work_year").cast(IntegerType())) \
       .withColumn("salary_in_usd", col("salary_in_usd").cast(DoubleType())) \
       .withColumn("remote_ratio", col("remote_ratio").cast(IntegerType())) \
       .withColumn("salary", col("salary").cast(DoubleType()))   # if you need the original salary

print("Schema after casting:")
df.printSchema()

# ────────────────────────────────────────────────
# 2. TRANSFORM - Add random bonus
# ────────────────────────────────────────────────
print("Adding random bonus (0–30% of salary_in_usd)...")

df_transformed = df \
    .withColumn("bonus_pct", rand() * lit(BONUS_MAX_PCT)) \
    .withColumn("bonus", col("salary_in_usd") * col("bonus_pct")) \
    .withColumn("total_comp", col("salary_in_usd") + col("bonus")) \
    .drop("bonus_pct")

print("Sample after transformation:")
df_transformed.select(
    "job_title",
    "salary_in_usd",
    "bonus",
    "total_comp"
).show(10, truncate=False)

# ────────────────────────────────────────────────
# 3. LOAD - Write Parquet
# ────────────────────────────────────────────────
print(f"Writing Parquet to: {OUTPUT_FOLDER}")
df_transformed.write.mode("overwrite").parquet(OUTPUT_FOLDER)

print("ETL finished successfully!")
spark.stop()