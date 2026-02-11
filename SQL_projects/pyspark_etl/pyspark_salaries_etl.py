from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, rand, lit

# ────────────────────────────────────────────────
# CONFIG – CHANGE THESE FOR YOUR AWS SETUP
# ────────────────────────────────────────────────
S3_BUCKET = "training-datalake-thanosvg"  # ← YOUR BUCKET NAME
INPUT_PATH = f"s3a://{S3_BUCKET}/salaries.csv"
OUTPUT_PATH = f"s3a://{S3_BUCKET}/processed_salaries_parquet"

BONUS_MAX_PCT = 0.30

# ────────────────────────────────────────────────
# START SPARK – with configs for S3 access
# ────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Salaries ETL on EMR") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")  # Cleaner output

print("Spark session started successfully.")
print(f"Spark version: {spark.version}")

# ────────────────────────────────────────────────
# 1. EXTRACT - Read CSV from S3
# ────────────────────────────────────────────────
print("Reading CSV from S3...")
df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)

print("Original schema:")
df.printSchema()
print(f"Original row count: {df.count()}")
df.show(5, truncate=False)

# ────────────────────────────────────────────────
# 2. TRANSFORM - Add random bonus and total_comp
# ────────────────────────────────────────────────
print("Adding random bonus (0–30% of salary_in_usd)...")

df_transformed = df.withColumn("bonus_pct", rand() * lit(BONUS_MAX_PCT)) \
                   .withColumn("bonus", col("salary_in_usd") * col("bonus_pct")) \
                   .withColumn("total_comp", col("salary_in_usd") + col("bonus")) \
                   .drop("bonus_pct")

print("Transformed schema:")
df_transformed.printSchema()

print("Sample after transformation:")
df_transformed.select(
    "job_title", "salary_in_usd", "bonus", "total_comp"
).show(10, truncate=False)

# ────────────────────────────────────────────────
# 3. GROUP BY AGGREGATIONS (your requested ones)
# ────────────────────────────────────────────────
print("\nRunning group-by aggregations...")

# Avg salary
print("\nAvg salary by job_title:")
df_transformed.groupBy("job_title").agg(avg("salary_in_usd").alias("avg_salary")).show(5, truncate=False)

print("\nAvg salary by company_location:")
df_transformed.groupBy("company_location").agg(avg("salary_in_usd").alias("avg_salary")).show(5, truncate=False)

print("\nAvg salary by company_size:")
df_transformed.groupBy("company_size").agg(avg("salary_in_usd").alias("avg_salary")).show(5, truncate=False)

print("\nAvg salary by remote_ratio:")
df_transformed.groupBy("remote_ratio").agg(avg("salary_in_usd").alias("avg_salary")).show(5, truncate=False)

print("\nAvg salary by employment_type:")
df_transformed.groupBy("employment_type").agg(avg("salary_in_usd").alias("avg_salary")).show(5, truncate=False)

print("\nAvg salary by experience_level:")
df_transformed.groupBy("experience_level").agg(avg("salary_in_usd").alias("avg_salary")).show(5, truncate=False)

print("\nAvg salary by work_year:")
df_transformed.groupBy("work_year").agg(avg("salary_in_usd").alias("avg_salary")).show(5, truncate=False)

print("\nAvg salary by salary_currency:")
df_transformed.groupBy("salary_currency").agg(avg("salary_in_usd").alias("avg_salary")).show(5, truncate=False)

print("\nAvg salary by employee_residence:")
df_transformed.groupBy("employee_residence").agg(avg("salary_in_usd").alias("avg_salary")).show(5, truncate=False)

# Avg bonus
print("\nAvg bonus by job_title:")
df_transformed.groupBy("job_title").agg(avg("bonus").alias("avg_bonus")).show(5, truncate=False)

print("\nAvg bonus by experience_level:")
df_transformed.groupBy("experience_level").agg(avg("bonus").alias("avg_bonus")).show(5, truncate=False)

print("\nAvg bonus by work_year:")
df_transformed.groupBy("work_year").agg(avg("bonus").alias("avg_bonus")).show(5, truncate=False)

print("\nAvg bonus by salary_currency:")
df_transformed.groupBy("salary_currency").agg(avg("bonus").alias("avg_bonus")).show(5, truncate=False)

print("\nAvg bonus by employee_residence:")
df_transformed.groupBy("employee_residence").agg(avg("bonus").alias("avg_bonus")).show(5, truncate=False)

# Avg total_comp
print("\nAvg total_comp by job_title:")
df_transformed.groupBy("job_title").agg(avg("total_comp").alias("avg_total_comp")).show(5, truncate=False)

print("\nAvg total_comp by experience_level:")
df_transformed.groupBy("experience_level").agg(avg("total_comp").alias("avg_total_comp")).show(5, truncate=False)

print("\nAvg total_comp by work_year:")
df_transformed.groupBy("work_year").agg(avg("total_comp").alias("avg_total_comp")).show(5, truncate=False)

print("\nAvg total_comp by salary_currency:")
df_transformed.groupBy("salary_currency").agg(avg("total_comp").alias("avg_total_comp")).show(5, truncate=False)

print("\nAvg total_comp by employee_residence:")
df_transformed.groupBy("employee_residence").agg(avg("total_comp").alias("avg_total_comp")).show(5, truncate=False)

# ────────────────────────────────────────────────
# 4. LOAD - Write Parquet with partition
# ────────────────────────────────────────────────
print(f"Writing Parquet to S3: {OUTPUT_FOLDER}")
df_transformed.write.mode("overwrite").partitionBy("experience_level").parquet(OUTPUT_FOLDER)

print("ETL finished successfully!")
spark.stop()