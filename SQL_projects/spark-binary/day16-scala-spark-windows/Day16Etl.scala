import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Day16Etl {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Scala Spark Windows").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val inputCsv = "salaries.csv"
    val outputFolder = "day16_processed_parquet"
    val bonusMaxPct = 0.30

    // READ
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(inputCsv)

    print("Original count: " + df.count())
    df.printSchema()
    df.show(5, truncate = false)

    // TRANSFORM 1: Add random bonus
    val dfTransformed = df.withColumn("bonus", rand() * lit(bonusMaxPct) * col("salary_in_usd"))
                          .withColumn("total_comp", col("salary_in_usd") + col("bonus"))

    // TRANSFORM 2: Window functions - running total comp + lag for previous + rank per job_title ordered by work_year
    val windowSpec = Window.partitionBy("job_title").orderBy("work_year")

    val dfWindows = dfTransformed.withColumn("running_total_comp", sum("total_comp").over(windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
                                 .withColumn("prev_total_comp", lag("total_comp", 1, 0).over(windowSpec))
                                 .withColumn("rank_per_job", row_number.over(windowSpec))
								 .withColumn("yoy_change", col("total_comp") - col("prev_total_comp"))

    print("Schema with windows:")
    dfWindows.printSchema()
    dfWindows.show(10, truncate = false)

    // LOAD - Partitioned Parquet
    print(s"Writing to: $outputFolder")
    dfWindows.write.mode("overwrite").partitionBy("experience_level").parquet(outputFolder)

    print("Day 16 ETL complete!")
    spark.stop()
  }
}