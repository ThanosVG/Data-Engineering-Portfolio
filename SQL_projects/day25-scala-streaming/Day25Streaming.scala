import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Day25Streaming {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Scala Kafka Streaming").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // Read from Kafka
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "salaries_stream")
      .option("startingOffsets", "earliest")
      .load()

    // Parse JSON
    val schema = StructType(Seq(
      StructField("job_title", StringType, true),
      StructField("salary_in_usd", DoubleType, true)
    ))
    val dfParsed = df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).alias("data"))
      .select("data.*")
      .withColumn("timestamp", current_timestamp())  // Add timestamp

    // Transform (add bonus)
    val dfTransformed = dfParsed.withColumn("bonus", rand() * lit(0.30) * col("salary_in_usd"))
                                .withColumn("total_comp", col("salary_in_usd") + col("bonus"))

    // Aggregations (avg total_comp over 1-min window)
    val dfAgg = dfTransformed.withWatermark("timestamp", "10 seconds")
                             .groupBy(window(col("timestamp"), "1 minute"))
                             .agg(avg("total_comp").alias("avg_total_comp"), count("*").alias("count"))

    // Output to console
    val query = dfAgg.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}