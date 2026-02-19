from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window, count, current_timestamp, rand, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("Combined Kafka PySpark Pipeline").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "salaries_stream_spark") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
schema = StructType([
    StructField("job_title", StringType(), True),
    StructField("salary_in_usd", DoubleType(), True),
    StructField("experience_level", StringType(), True)
])
df_parsed = df.selectExpr("CAST(value AS STRING) as value") \
              .select(from_json(col("value"), schema).alias("data")) \
              .select("data.*") \
              .withColumn("timestamp", current_timestamp())

# ETL Transformation (add bonus, total_comp)
df_transformed = df_parsed.withColumn("bonus", rand() * lit(0.30) * col("salary_in_usd")) \
                          .withColumn("total_comp", col("salary_in_usd") + col("bonus"))

# Aggregations (e.g., avg by experience_level over 1-min window)
df_agg = df_transformed.withWatermark("timestamp", "10 seconds") \
                       .groupBy(window("timestamp", "1 minute"), "experience_level") \
                       .agg(avg("total_comp").alias("avg_total_comp"), count("*").alias("count"))

# Output to console
console_query = df_agg.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Output to Parquet (append mode)
parquet_query = df_agg.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "output_parquet/") \
    .option("checkpointLocation", "checkpoint/") \
    .start()

console_query.awaitTermination()