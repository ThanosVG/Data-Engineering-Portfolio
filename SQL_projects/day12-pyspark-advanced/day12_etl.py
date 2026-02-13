from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, count, isnull, row_number
from pyspark.sql.window import Window
import os
import shutil

# CONFIG
SALARIES_CSV = "salaries.csv"
DEPARTMENTS_CSV = "departments.csv"
OUTPUT_FOLDER = "day12_processed_parquet"

# CLEAN PREVIOUS RUN
if os.path.exists(OUTPUT_FOLDER):
    print(f"Removing previous output: {OUTPUT_FOLDER}")
    shutil.rmtree(OUTPUT_FOLDER)

# START SPARK
spark = SparkSession.builder.appName("Day 12 - Advanced ETL").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# READ INPUTS
df_salaries = spark.read.csv(SALARIES_CSV, header=True, inferSchema=True)
df_depts = spark.read.csv(DEPARTMENTS_CSV, header=True, inferSchema=True)

print("Salaries count:", df_salaries.count())
print("Departments count:", df_depts.count())

# JOIN - Enrich salaries with department category (left join on job_title contains department)
df_joined = df_salaries.join(df_depts, df_salaries.job_title.contains(df_depts.department), "left_outer")

# TRANSFORM 1: Add random bonus (fixed: native when instead of UDF)
df = df_joined.withColumn("bonus", when(col("job_category") == "Tech", col("salary_in_usd") * 0.25).otherwise(col("salary_in_usd") * 0.10)) \
              .withColumn("total_comp", col("salary_in_usd") + col("bonus"))

# TRANSFORM 2: Window function - running avg total_comp per job_title ordered by work_year
window_spec = Window.partitionBy("job_title").orderBy("work_year").rowsBetween(Window.unboundedPreceding, 0)
df = df.withColumn("running_avg_total_comp", avg("total_comp").over(window_spec))

# TRANSFORM 3: Data quality flags
df = df.withColumn("null_salary", when(isnull("salary_in_usd"), 1).otherwise(0)) \
       .withColumn("outlier_salary", when(col("salary_in_usd") > 500000, 1).otherwise(0))  # example threshold

# SHOW RESULTS
print("Sample with bonus, window & quality flags:")
df.select("job_title", "work_year", "salary_in_usd", "job_category", "bonus", "total_comp", "running_avg_total_comp", "null_salary", "outlier_salary").show(10)

# LOAD - Partitioned Parquet
print(f"Writing to: {OUTPUT_FOLDER}")
df.write.mode("overwrite").partitionBy("experience_level").parquet(OUTPUT_FOLDER)

print("Done - Parquet written with partitions")
spark.stop()