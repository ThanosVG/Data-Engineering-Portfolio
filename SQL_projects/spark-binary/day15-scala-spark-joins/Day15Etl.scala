import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day15Etl {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Scala Spark Joins").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val salariesCsv = "salaries.csv"
    val deptsCsv = "departments.csv"
    val outputFolder = "day15_processed_parquet"
    val bonusMaxPct = 0.30

    // READ
    val dfSalaries = spark.read.option("header", "true").option("inferSchema", "true").csv(salariesCsv)
    val dfDepts = spark.read.option("header", "true").option("inferSchema", "true").csv(deptsCsv)

    print("Salaries count: " + dfSalaries.count())
    dfSalaries.printSchema()
    dfSalaries.show(5, truncate = false)

    print("Depts count: " + dfDepts.count())
    dfDepts.show()

    // JOIN - Enrich salaries with category (left join on job_title)
    val dfJoined = dfSalaries.join(dfDepts, dfSalaries("job_title") === dfDepts("job_title"), "left_outer")

    // TRANSFORM - Add random bonus
    val dfTransformed = dfJoined.withColumn("bonus", rand() * lit(bonusMaxPct) * col("salary_in_usd"))
                                .withColumn("total_comp", col("salary_in_usd") + col("bonus"))

    print("Transformed schema:")
    dfTransformed.printSchema()
    dfTransformed.show(5, truncate = false)

    // LOAD - Partitioned Parquet
    print(s"Writing to: $outputFolder")
    dfTransformed.write.mode("overwrite").partitionBy("experience_level").parquet(outputFolder)

    print("Day 15 ETL complete!")
    spark.stop()
  }
}