from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, rand, lit

# CONFIG
INPUT_CSV = "salaries.csv"
OUTPUT_FOLDER = "processed_salaries_parquet"
BONUS_MAX_PCT = 0.30

# START SPARK
spark = SparkSession.builder.appName("Salaries ETL").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. EXTRACT - Read CSV
print("Reading CSV...")
df = spark.read.csv(INPUT_CSV, header=True, inferSchema=True)

print("Original schema:")
df.printSchema()
print(f"Original row count: {df.count()}")
df.show(5)

# 2. TRANSFORM - Add random bonus and total_comp
print("Adding random bonus (0–30% of salary_in_usd)...")
df_transformed = df.withColumn("bonus_pct", rand() * lit(BONUS_MAX_PCT)) \
                   .withColumn("bonus", col("salary_in_usd") * col("bonus_pct")) \
                   .withColumn("total_comp", col("salary_in_usd") + col("bonus")) \
                   .drop("bonus_pct")

print("Transformed schema:")
df_transformed.printSchema()
df_transformed.show(5)

# 3. GROUP BY (your requests - showing 5 rows each)
print("Running group by aggregations...")
# Avg salary
df_transformed.groupBy("job_title").agg(avg("salary_in_usd").alias("avg_salary")).show(5)
df_transformed.groupBy("company_location").agg(avg("salary_in_usd").alias("avg_salary")).show(5)
df_transformed.groupBy("company_size").agg(avg("salary_in_usd").alias("avg_salary")).show(5)
df_transformed.groupBy("remote_ratio").agg(avg("salary_in_usd").alias("avg_salary")).show(5)
df_transformed.groupBy("employment_type").agg(avg("salary_in_usd").alias("avg_salary")).show(5)
df_transformed.groupBy("experience_level").agg(avg("salary_in_usd").alias("avg_salary")).show(5)
df_transformed.groupBy("work_year").agg(avg("salary_in_usd").alias("avg_salary")).show(5)
df_transformed.groupBy("salary_currency").agg(avg("salary_in_usd").alias("avg_salary")).show(5)
df_transformed.groupBy("employee_residence").agg(avg("salary_in_usd").alias("avg_salary")).show(5)

# Avg bonus
df_transformed.groupBy("job_title").agg(avg("bonus").alias("avg_bonus")).show(5)
df_transformed.groupBy("experience_level").agg(avg("bonus").alias("avg_bonus")).show(5)
df_transformed.groupBy("work_year").agg(avg("bonus").alias("avg_bonus")).show(5)
df_transformed.groupBy("salary_currency").agg(avg("bonus").alias("avg_bonus")).show(5)
df_transformed.groupBy("employee_residence").agg(avg("bonus").alias("avg_bonus")).show(5)

# Avg total_comp
df_transformed.groupBy("job_title").agg(avg("total_comp").alias("avg_total_comp")).show(5)
df_transformed.groupBy("experience_level").agg(avg("total_comp").alias("avg_total_comp")).show(5)
df_transformed.groupBy("work_year").agg(avg("total_comp").alias("avg_total_comp")).show(5)
df_transformed.groupBy("salary_currency").agg(avg("total_comp").alias("avg_total_comp")).show(5)
df_transformed.groupBy("employee_residence").agg(avg("total_comp").alias("avg_total_comp")).show(5)

# 4. LOAD - Write Parquet with partition
print(f"Writing Parquet to: {OUTPUT_FOLDER}")
df_transformed.write.mode("overwrite").partitionBy('experience_level').parquet(OUTPUT_FOLDER)

print("ETL finished successfully!")
spark.stop()