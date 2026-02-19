import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day14Etl {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Scala Spark ETL").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val inputCsv = "salaries.csv"
    val outputFolder = "day14_processed_parquet"
    val bonusMaxPct = 0.30

	// READ
	val df = spark.read.option("header", "true").option("inferSchema", "true").csv(inputCsv)

    print("Original count: " + df.count())
    df.printSchema()
    df.show(5, truncate = false)

	// TRANSFORM - Add random bonus
	val dfTransformed = df.withColumn("bonus", rand() * lit(bonusMaxPct) * col("salary_in_usd"))
						  .withColumn("total_comp", col("salary_in_usd") + col("bonus"))

    print("Transformed schema:")
    dfTransformed.printSchema()
    dfTransformed.show(5, truncate = false)

    // LOAD - Partitioned Parquet
    print(s"Writing to: $outputFolder")
    dfTransformed.write.mode("overwrite").partitionBy("experience_level").parquet(outputFolder)

    print("Day 14 ETL complete!")
    spark.stop()
  }
}