from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, lit, avg, count, isnull, row_number, lag, sum , when
from pyspark.sql.window import Window
import os
import shutil

# CONFIG
SALARIES_CSV = "salaries.csv"
OUTPUT_FOLDER = "day13_processed_parquet"

# CLEAN PREVIOUS RUN
if os.path.exists(OUTPUT_FOLDER):
    print(f"Removing previous output: {OUTPUT_FOLDER}")
    shutil.rmtree(OUTPUT_FOLDER)

# START SPARK
spark = SparkSession.builder.appName("Day 13 - Advanced ETL").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# READ INPUT
df = spark.read.csv(SALARIES_CSV, header=True, inferSchema=True)

print("Salaries count:", df.count())
print("Original schema:")
df.printSchema()
df.show(5, truncate=False)

# TRANSFORM 1: Add random bonus (based on experience_level)
df = df.withColumn("bonus", when(col("experience_level") == "SE", col("salary_in_usd") * 0.25)
                            .when(col("experience_level") == "EX", col("salary_in_usd") * 0.20)
                            .otherwise(col("salary_in_usd") * 0.10)) \
       .withColumn("total_comp", col("salary_in_usd") + col("bonus"))

# TRANSFORM 2: Window functions - running total comp + lag for previous + rank per job_title ordered by work_year
window_spec = Window.partitionBy("job_title").orderBy("work_year")

df = df.withColumn("running_total_comp", sum("total_comp").over(window_spec.rowsBetween(Window.unboundedPreceding, 0))) \
       .withColumn("prev_total_comp", lag("total_comp", 1, 0).over(window_spec)) \
       .withColumn("rank_per_job", row_number().over(window_spec))

# TRANSFORM 3: Data quality flags
df = df.withColumn("null_salary", when(isnull("salary_in_usd"), 1).otherwise(0)) \
       .withColumn("outlier_salary", when(col("salary_in_usd") > 500000, 1).otherwise(0))  # example threshold

# SHOW RESULTS
print("Sample with bonus, window & quality flags:")
df.select(
    "job_title", "work_year", "salary_in_usd", "bonus", "total_comp",
    "running_total_comp", "prev_total_comp", "rank_per_job", "null_salary", "outlier_salary"
).show(10, truncate=False)

# QUALITY SUMMARY
print("Quality summary:")
df.groupBy().agg(
    count("*").alias("total_rows"),
    count(when(col("null_salary") == 1, 1)).alias("null_salaries"),
    count(when(col("outlier_salary") == 1, 1)).alias("outliers")
).show()

# CACHE for reuse (demo performance)
df.cache()
print("Data cached for faster subsequent operations.")

# LOAD - Partitioned Parquet
print(f"Writing to: {OUTPUT_FOLDER}")
df.write.mode("overwrite").partitionBy("experience_level").parquet(OUTPUT_FOLDER)

print("Day 13 ETL complete!")
spark.stop()