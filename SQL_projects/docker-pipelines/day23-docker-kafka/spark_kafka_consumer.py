from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window, count, current_timestamp

spark = SparkSession.builder.appName("Kafka Spark Streaming").getOrCreate()
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
              .withColumn("timestamp", current_timestamp())   # <-- Fixed: add timestamp

# Process stream
df_processed = df_parsed.filter(col("salary_in_usd") > 100000) \
                        .withWatermark("timestamp", "10 seconds") \
                        .groupBy(window("timestamp", "1 minute")) \
                        .agg(count("*").alias("high_salary_count"), avg("salary_in_usd").alias("avg_high_salary"))

# Output to console
query = df_processed.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()