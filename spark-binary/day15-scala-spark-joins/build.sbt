name := "Day15Etl"

version := "0.1"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided"
)

mainClass in assembly := Some("Day15Etl")