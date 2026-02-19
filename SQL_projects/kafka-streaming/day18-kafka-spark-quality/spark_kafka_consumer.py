from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window, count, current_timestamp, when

spark = SparkSession.builder.appName("Kafka Spark Streaming Quality").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "salaries_stream") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON + add timestamp
df_parsed = df.selectExpr("CAST(value AS STRING) as value") \
              .select(from_json(col("value"), "struct<job_title:string,salary_in_usd:double>").alias("data")) \
              .select("data.*") \
              .withColumn("timestamp", current_timestamp())

# Quality checks (flag invalid salaries, e.g., null or <0)
df_quality = df_parsed.withColumn("valid_salary", when(col("salary_in_usd") > 0, 1).otherwise(0))

# Process (filter high salaries, running count/avg over 1-min window)
df_processed = df_quality.filter(col("valid_salary") == 1) \
                         .filter(col("salary_in_usd") > 100000) \
                         .withWatermark("timestamp", "10 seconds") \
                         .groupBy(window("timestamp", "1 minute")) \
                         .agg(count("*").alias("high_salary_count"), avg("salary_in_usd").alias("avg_high_salary"))

# Output to console
console_query = df_processed.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Output to file (Parquet in append mode)
file_query = df_processed.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "output_parquet/") \
    .option("checkpointLocation", "checkpoint/") \
    .start()

console_query.awaitTermination()